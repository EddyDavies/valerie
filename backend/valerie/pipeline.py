DEFAULT_CLIPTAGGER_API_URL = "https://api.inference.net/v1"
DEFAULT_CLIPTAGGER_MODEL = "inference-net/cliptagger-12b"
DEFAULT_FRAME_SAMPLE_INTERVAL_SECONDS = 2.0
DEFAULT_MAX_FRAMES = 150

"""Prefect flow that pulls video data from Apify, analyzes it with ClipTagger-12b,
and stores the output in MongoDB without persisting artifacts to S3."""


import base64
import json
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence
from uuid import uuid4

import httpx
from openai import OpenAI
from pymongo import MongoClient
from prefect import flow, get_run_logger, task


def _load_prompt_file(filename: str) -> str:
    """Load a prompt file from the prompts directory."""
    prompts_dir = Path(__file__).resolve().parent.parent / "prompts"
    prompt_path = prompts_dir / filename
    if not prompt_path.exists():
        raise FileNotFoundError(
            f"Prompt file not found: {prompt_path}. "
            f"Expected prompts directory at: {prompts_dir}"
        )
    return prompt_path.read_text(encoding="utf-8").strip()


@dataclass(slots=True)
class PipelineConfig:
    """Configuration required to execute the Apify to ClipTagger pipeline."""

    apify_actor_id: str
    apify_token: str
    apify_input: dict[str, Any]
    cliptagger_api_key: str
    mongo_uri: str | None = None
    mongo_database: str | None = None
    mongo_collection: str | None = None
    cliptagger_api_url: str = DEFAULT_CLIPTAGGER_API_URL
    cliptagger_model: str = DEFAULT_CLIPTAGGER_MODEL
    cliptagger_system_prompt: str = field(
        default_factory=lambda: _load_prompt_file("cliptagger_system.jinja")
    )
    cliptagger_user_prompt: str = field(
        default_factory=lambda: _load_prompt_file("cliptagger_user.jinja")
    )
    frame_sample_interval_seconds: float = DEFAULT_FRAME_SAMPLE_INTERVAL_SECONDS
    max_frames: int = DEFAULT_MAX_FRAMES
    run_local: bool = False


@dataclass(slots=True)
class VideoArtifact:
    """Lightweight container for video metadata extracted from Apify."""

    url: str
    payload: dict[str, Any]


@dataclass(slots=True)
class FrameSample:
    """Metadata for a sampled video frame stored on disk."""

    path: Path
    timestamp: float


@dataclass(slots=True)
class FrameAnalysis:
    """Structured ClipTagger output paired with its frame metadata."""

    timestamp: float
    data: dict[str, Any]
    raw_response: dict[str, Any]


class PipelineError(RuntimeError):
    """Raised when the pipeline cannot proceed with the supplied inputs."""


@task(name="Run Apify actor", retries=2, retry_delay_seconds=30)
def run_apify_actor(config: PipelineConfig) -> list[dict[str, Any]]:
    """Invoke the configured Apify actor and return its dataset items."""
    logger = get_run_logger()
    logger.info("Running Apify actor %s", config.apify_actor_id)

    run_url = (
        f"https://api.apify.com/v2/acts/{config.apify_actor_id}/run-sync-get-dataset-items"
    )
    params = {"token": config.apify_token}

    with httpx.Client(timeout=httpx.Timeout(120.0)) as client:
        response = client.post(run_url, params=params, json=config.apify_input)
        response.raise_for_status()
        items = response.json()

    if not isinstance(items, list) or not items:
        raise PipelineError(
            "Apify actor returned no items. Check actor input and permissions."
        )

    logger.info("Retrieved %s records from Apify actor run", len(items))
    return items


def _pick_url(value: Any) -> str | None:
    """Heuristic to extract a URL string from nested structures."""
    if isinstance(value, str) and value.startswith("http"):
        return value
    if isinstance(value, list):
        for entry in value:
            candidate = _pick_url(entry)
            if candidate:
                return candidate
    if isinstance(value, dict):
        for key in (
            "url",
            "urlNoWatermark",
            "urlWatermark",
            "downloadUrl",
            "videoUrl",
        ):
            candidate = _pick_url(value.get(key))
            if candidate:
                return candidate
    return None


@task(name="Select video payload")
def select_video_payload(items: Iterable[dict[str, Any]]) -> VideoArtifact:
    """Pick the first dataset item that contains a downloadable video URL."""
    logger = get_run_logger()
    for item in items:
        if not isinstance(item, dict):
            continue
        media_urls = item.get("mediaUrls")
        url: str | None = None
        if isinstance(media_urls, list):
            first_entry = media_urls[0] if media_urls else None
            if isinstance(first_entry, dict):
                url = _pick_url(first_entry.get("url")) or _pick_url(first_entry)
            else:
                url = _pick_url(first_entry)

        if url:
            logger.info("Selected video URL %s", url)
            return VideoArtifact(url=url, payload=item)

    raise PipelineError("Apify dataset did not include mediaUrls entries.")


@task(name="Download video", retries=2, retry_delay_seconds=15)
def download_video(artifact: VideoArtifact) -> Path:
    """Download a remote video to a temporary file and return its path."""
    logger = get_run_logger()
    logger.info("Downloading video from %s", artifact.url)

    suffix = Path(artifact.url).suffix or ".mp4"
    temp_dir = Path(tempfile.mkdtemp(prefix="apify-video-"))
    file_path = temp_dir / f"input_video{suffix}"

    with httpx.stream("GET", artifact.url, timeout=httpx.Timeout(60.0)) as response:
        response.raise_for_status()
        with file_path.open("wb") as file_descriptor:
            for chunk in response.iter_bytes():
                file_descriptor.write(chunk)

    logger.info("Video saved to %s (%s bytes)", file_path, file_path.stat().st_size)
    return file_path


@task(name="Extract video frames", retries=2, retry_delay_seconds=15)
def extract_video_frames(
    video_path: Path, sample_interval_seconds: float, max_frames: int
) -> list[FrameSample]:
    """Sample still frames from the video using ffmpeg."""
    logger = get_run_logger()
    if sample_interval_seconds <= 0:
        raise PipelineError("Frame sampling interval must be greater than zero.")
    if max_frames <= 0:
        raise PipelineError("Max frames must be greater than zero.")

    frames_dir = video_path.parent / "frames"
    frames_dir.mkdir(exist_ok=True)
    frame_output = frames_dir / "frame_%05d.jpg"
    frame_rate = 1.0 / sample_interval_seconds

    logger.info(
        "Extracting frames using ffmpeg (fps=%s, max_frames=%s)",
        f"{frame_rate:.4f}",
        max_frames,
    )

    command = [
        "ffmpeg",
        "-loglevel",
        "error",
        "-i",
        str(video_path),
        "-vf",
        f"fps={frame_rate:.6f}",
        "-q:v",
        "2",
    ]

    if max_frames:
        command.extend(["-frames:v", str(max_frames)])

    command.append(str(frame_output))

    process = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )

    if process.returncode != 0:
        logger.error(
            "ffmpeg failed with return code %s and stderr: %s",
            process.returncode,
            process.stderr,
        )
        raise PipelineError("Failed to extract frames with ffmpeg.")

    frame_paths = sorted(frames_dir.glob("frame_*.jpg"))
    if not frame_paths:
        raise PipelineError("Frame extraction produced no frames.")

    samples: list[FrameSample] = [
        FrameSample(path=path, timestamp=round(index * sample_interval_seconds, 3))
        for index, path in enumerate(frame_paths[:max_frames])
    ]

    logger.info("Extracted %s frames for ClipTagger analysis", len(samples))
    return samples


@task(name="Run ClipTagger analysis", retries=2, retry_delay_seconds=10)
def analyze_frames_with_cliptagger(
    frames: Sequence[FrameSample],
    api_key: str,
    api_url: str,
    model_name: str,
    system_prompt: str,
    user_prompt: str,
) -> list[FrameAnalysis]:
    """Send sampled frames to the ClipTagger-12b API using OpenAI SDK."""
    logger = get_run_logger()
    if not frames:
        raise PipelineError("No frames available for ClipTagger analysis.")

    client = OpenAI(
        base_url=api_url,
        api_key=api_key,
        timeout=60.0,
    )

    logger.info("ClipTagger API base_url: %s", api_url)
    logger.info("ClipTagger model: %s", model_name)

    analyses: list[FrameAnalysis] = []
    for index, frame in enumerate(frames, start=1):
        image_bytes = frame.path.read_bytes()
        encoded = base64.b64encode(image_bytes).decode("ascii")

        logger.info(
            "Submitting frame %s/%s to ClipTagger (timestamp=%ss, size=%s bytes)",
            index,
            len(frames),
            frame.timestamp,
            len(image_bytes),
        )

        try:
            response = client.chat.completions.create(
                model=model_name,
                messages=[
                    {
                        "role": "system", "content": system_prompt
                    },
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": user_prompt},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{encoded}",
                                },
                            },
                        ],
                    },
                ],
                temperature=0.1,
                max_tokens=2000,
                response_format={"type": "json_object"},
            )
        except Exception as api_exc:
            logger.error("ClipTagger API error: %s", api_exc)
            logger.error("API URL: %s", api_url)
            logger.error("Model: %s", model_name)
            raise PipelineError(f"ClipTagger API request failed: {api_exc}") from api_exc

        content_text = response.choices[0].message.content
        if not content_text:
            raise PipelineError(
                f"ClipTagger returned empty content for timestamp {frame.timestamp}s"
            )

        try:
            parsed_data = json.loads(content_text)
        except json.JSONDecodeError as exc:
            logger.error(
                "ClipTagger response for frame at %ss was not valid JSON: %s",
                frame.timestamp,
                exc,
            )
            raise PipelineError(
                f"ClipTagger returned invalid JSON for timestamp {frame.timestamp}s"
            ) from exc

        analyses.append(
            FrameAnalysis(
                timestamp=frame.timestamp,
                data=parsed_data,
                raw_response=response.model_dump(),
            )
        )

    logger.info("ClipTagger analysis complete for %s frames", len(analyses))
    return analyses


@task(name="Delete local video", persist_result=False)
def delete_local_video(video_path: Path) -> None:
    """Remove the locally stored video file and its temporary directory."""
    logger = get_run_logger()

    if not isinstance(video_path, Path):
        logger.warning("Expected Path in delete_local_video, got %s", type(video_path))
        return

    try:
        if video_path.exists():
            video_path.unlink()
            logger.info("Deleted local video file %s", video_path)
        else:
            logger.info("Local video file already removed: %s", video_path)
    except OSError as exc:
        logger.warning("Failed to delete local video file %s: %s", video_path, exc)

    temp_dir = video_path.parent
    if temp_dir.exists():
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.info("Removed temporary directory %s", temp_dir)
        except OSError as exc:
            logger.warning("Failed to remove temporary directory %s: %s", temp_dir, exc)


@task(name="Persist ClipTagger analysis to MongoDB")
def persist_analysis(
    mongo_uri: str,
    database: str,
    collection: str,
    analysis_payload: dict[str, Any],
    video_url: str,
    video_payload: dict[str, Any],
) -> str:
    """Persist the ClipTagger output and source metadata in MongoDB."""
    logger = get_run_logger()
    document = {
        "video_url": video_url,
        "video_payload": video_payload,
        "analysis": analysis_payload,
    }

    with MongoClient(mongo_uri, serverSelectionTimeoutMS=5000) as client:
        result = client[database][collection].insert_one(document)
        inserted_id = str(result.inserted_id)

    logger.info("Stored ClipTagger analysis under MongoDB document %s", inserted_id)
    return inserted_id


# --- Local persistence fallback (RUN_LOCAL), remove when Mongo is mandatory ---
@task(name="Persist ClipTagger analysis locally", persist_result=False)
def persist_analysis_locally(document: dict[str, Any]) -> str:
    """Persist the ClipTagger output to a local JSON file when MongoDB is disabled."""
    logger = get_run_logger()
    output_dir = Path(__file__).resolve().parent / "local_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    safe_stub = "".join(
        character if character.isalnum() else "-" for character in document["video_url"]
    )[:60].strip("-") or "video"

    file_path = output_dir / f"{safe_stub}-{uuid4().hex}.json"
    file_path.write_text(json.dumps(document, indent=2), encoding="utf-8")

    logger.info("Saved ClipTagger analysis locally to %s", file_path)
    return str(file_path)


@flow(name="Apify to ClipTagger pipeline")
def apify_to_cliptagger_flow(config: PipelineConfig) -> dict[str, Any]:
    """Prefect entry point that orchestrates the full pipeline."""
    logger = get_run_logger()
    logger.info("Starting Apify to ClipTagger pipeline")

    items = run_apify_actor.submit(config).result()
    artifact = select_video_payload.submit(items).result()
    video_path = download_video.submit(artifact).result()
    frame_samples_future = extract_video_frames.submit(
        video_path,
        config.frame_sample_interval_seconds,
        config.max_frames,
    )
    cliptagger_response_future = analyze_frames_with_cliptagger.submit(
        frame_samples_future,
        config.cliptagger_api_key,
        config.cliptagger_api_url,
        config.cliptagger_model,
        config.cliptagger_system_prompt,
        config.cliptagger_user_prompt,
    )

    frame_analyses = cliptagger_response_future.result()

    analysis_payload = {
        "model": config.cliptagger_model,
        "sample_interval_seconds": config.frame_sample_interval_seconds,
        "max_frames": config.max_frames,
        "frames": [
            {
                "frame_index": index,
                "timestamp": analysis.timestamp,
                "data": analysis.data,
                "raw_response": analysis.raw_response,
            }
            for index, analysis in enumerate(frame_analyses)
        ],
    }

    document = {
        "video_url": artifact.url,
        "video_payload": artifact.payload,
        "analysis": analysis_payload,
    }

    if config.run_local:
        document_id = persist_analysis_locally.submit(document).result()
    else:
        document_id = persist_analysis.submit(
            config.mongo_uri or "",
            config.mongo_database or "",
            config.mongo_collection or "",
            analysis_payload,
            artifact.url,
            artifact.payload,
        ).result()

    cleanup_future = delete_local_video.submit(
        video_path, wait_for=[cliptagger_response_future]
    )
    cleanup_future.result()

    logger.info(
        "Pipeline complete; stored output under MongoDB document %s for video %s",
        document_id,
        artifact.url,
    )
    return {
        "document_id": document_id,
        "video_url": artifact.url,
        "apify_actor_id": config.apify_actor_id,
    }

