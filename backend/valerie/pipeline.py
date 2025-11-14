"""Prefect flow that pulls video data from Apify, analyzes it with Gemini, and
stores the output in MongoDB without persisting artifacts to S3."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

import google.generativeai as genai
import httpx
from pymongo import MongoClient
from prefect import flow, get_run_logger, task


@dataclass(slots=True)
class PipelineConfig:
    """Configuration required to execute the Apify to Gemini pipeline."""

    apify_actor_id: str
    apify_token: str
    apify_input: dict[str, Any]
    gemini_api_key: str
    system_prompt: str
    mongo_uri: str | None
    mongo_database: str | None
    mongo_collection: str | None
    gemini_model: str = "gemini-2.5-pro"
    run_local: bool = False


@dataclass(slots=True)
class VideoArtifact:
    """Lightweight container for video metadata extracted from Apify."""

    url: str
    payload: dict[str, Any]


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


_TIMECODE_PATTERN = re.compile(
    r"^(?:(?P<hours>\d+):)?(?P<minutes>\d{1,2}):(?P<seconds>\d{2})(?:[.,](?P<millis>\d{1,3}))?$"
)


def _strip_code_fence(text: str) -> str:
    cleaned = text.strip()
    cleaned = re.sub(r"^```[a-zA-Z0-9]*\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _try_parse_json(text: str) -> dict[str, Any] | None:
    cleaned = _strip_code_fence(text)
    if not cleaned:
        return None

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            snippet = cleaned[start : end + 1]
            try:
                return json.loads(snippet)
            except json.JSONDecodeError:
                return None
    return None


def _collect_screenshot_suggestions(gemini_response: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(gemini_response, dict):
        return []

    direct = gemini_response.get("screenshots")
    if isinstance(direct, list):
        return [entry for entry in direct if isinstance(entry, dict)]

    candidates = gemini_response.get("candidates", [])
    if not isinstance(candidates, list):
        return []

    for candidate in candidates:
        content = candidate.get("content")
        if isinstance(content, dict):
            raw_parts = content.get("parts") or []
        elif isinstance(content, list):
            raw_parts = content
        else:
            raw_parts = []

        for part in raw_parts:
            if not isinstance(part, dict):
                continue
            text = part.get("text")
            if not isinstance(text, str):
                continue
            payload = _try_parse_json(text)
            if isinstance(payload, dict):
                suggestions = payload.get("screenshots")
                if isinstance(suggestions, list):
                    structured = [
                        suggestion
                        for suggestion in suggestions
                        if isinstance(suggestion, dict)
                    ]
                    if structured:
                        return structured
    return []


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _parse_timestamp_string(value: str) -> float | None:
    match = _TIMECODE_PATTERN.match(value.strip())
    if not match:
        return None
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes"))
    seconds = int(match.group("seconds"))
    millis_group = match.group("millis")
    millis = int(millis_group.ljust(3, "0")) if millis_group else 0
    return hours * 3600 + minutes * 60 + seconds + millis / 1000.0


def _seconds_from_screenshot_entry(entry: dict[str, Any]) -> float | None:
    if not isinstance(entry, dict):
        return None

    for key in ("timestamp_ms", "timecode_ms", "millisecond_total"):
        if key in entry:
            total_ms = _coerce_float(entry.get(key))
            if total_ms is not None:
                return max(total_ms, 0.0) / 1000.0

    second = None
    for key in ("second", "seconds"):
        if key in entry:
            second = _coerce_float(entry.get(key))
            if second is not None:
                break

    if second is None and "milliseconds" in entry:
        total_ms = _coerce_float(entry.get("milliseconds"))
        if total_ms is not None:
            return max(total_ms, 0.0) / 1000.0

    millisecond = None
    for key in ("millisecond", "milliseconds", "millisecondOffset", "ms"):
        if key in entry:
            millisecond = _coerce_float(entry.get(key))
            if millisecond is not None:
                break

    if second is not None:
        millis = millisecond if millisecond is not None else 0.0
        return max(second + millis / 1000.0, 0.0)

    for key in ("timestamp", "timecode"):
        raw = entry.get(key)
        if isinstance(raw, str):
            parsed = _parse_timestamp_string(raw)
            if parsed is not None:
                return max(parsed, 0.0)
    return None


@task(name="Extract screenshots", persist_result=False)
def extract_screenshots(video_path: Path, gemini_response: dict[str, Any]) -> dict[str, Any]:
    """Generate image files for each screenshot suggestion using ffmpeg."""
    logger = get_run_logger()
    suggestions = _collect_screenshot_suggestions(gemini_response)

    if not suggestions:
        logger.info("No screenshot suggestions detected; skipping ffmpeg extraction.")
        return {"suggestions": [], "images": []}

    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        logger.warning(
            "ffmpeg executable not found on PATH. Returning suggestions without extraction."
        )
        return {"suggestions": suggestions, "images": []}

    screenshot_root = video_path.parent / "screenshots"
    screenshot_root.mkdir(parents=True, exist_ok=True)

    artifacts: list[dict[str, Any]] = []
    for index, suggestion in enumerate(suggestions, start=1):
        timestamp_seconds = _seconds_from_screenshot_entry(suggestion)
        if timestamp_seconds is None:
            logger.warning(
                "Skipping screenshot suggestion %s due to missing timestamp data: %s",
                index,
                suggestion,
            )
            continue

        timestamp_seconds = max(timestamp_seconds, 0.0)
        filename = f"screenshot_{index:02d}_{timestamp_seconds:06.3f}.jpg"
        output_path = screenshot_root / filename

        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            f"{timestamp_seconds:.3f}",
            "-i",
            str(video_path),
            "-frames:v",
            "1",
            "-y",
            str(output_path),
        ]

        logger.info(
            "Extracting screenshot %s at %s seconds to %s",
            index,
            f"{timestamp_seconds:.3f}",
            output_path,
        )

        try:
            process = subprocess.run(command, capture_output=True, text=True, check=False)
        except OSError as exc:
            logger.warning("Failed to invoke ffmpeg for screenshot %s: %s", index, exc)
            continue

        if process.returncode != 0:
            stderr = process.stderr.strip()
            logger.warning(
                "ffmpeg exited with code %s for screenshot %s: %s",
                process.returncode,
                index,
                stderr or "no stderr output",
            )
            continue

        artifacts.append(
            {
                "path": str(output_path),
                "timestamp_seconds": round(timestamp_seconds, 3),
                "suggestion": suggestion,
            }
        )

    logger.info("Extracted %s screenshot(s) via ffmpeg", len(artifacts))
    return {"suggestions": suggestions, "images": artifacts}


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


@task(name="Generate Gemini response")
def call_gemini(
    video_path: Path, system_prompt: str, gemini_api_key: str, model_name: str
) -> dict[str, Any]:
    """Upload the video to Gemini and return the structured response."""
    logger = get_run_logger()
    logger.info("Calling Gemini model %s with uploaded video", model_name)

    genai.configure(api_key=gemini_api_key)
    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=system_prompt,
    )

    uploaded_file = genai.upload_file(path=str(video_path))
    logger.info("Uploaded file to Gemini resource %s", uploaded_file.name)

    max_attempts = 15
    poll_interval = 2.0
    for attempt in range(1, max_attempts + 1):
        file_status = genai.get_file(name=uploaded_file.name)
        state_value = getattr(file_status, "state", None)
        state = getattr(state_value, "name", state_value)
        if state == "ACTIVE":
            logger.info(
                "Gemini file %s became ACTIVE on attempt %s/%s",
                uploaded_file.name,
                attempt,
                max_attempts,
            )
            break
        if state == "FAILED":
            raise PipelineError(
                f"Gemini file {uploaded_file.name} processing failed with state=FAILED"
            )

        logger.info(
            "Gemini file %s not ready yet (state=%s); waiting %.1fs before retry %s/%s",
            uploaded_file.name,
            state or "UNKNOWN",
            poll_interval,
            attempt,
            max_attempts,
        )
        time.sleep(poll_interval)
    else:
        raise PipelineError(
            f"Gemini file {uploaded_file.name} did not reach ACTIVE state after "
            f"{max_attempts * poll_interval:.0f}s"
        )

    response = model.generate_content([uploaded_file])
    response_dict = response.to_dict()
    logger.info("Gemini response received with %s candidates", len(response.candidates))

    return response_dict


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
            has_additional_artifacts = any(temp_dir.iterdir())
        except OSError as exc:
            logger.warning("Could not inspect temporary directory %s: %s", temp_dir, exc)
            return

        if has_additional_artifacts:
            logger.info(
                "Preserving temporary directory %s because it still contains artifacts.",
                temp_dir,
            )
            return

        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.info("Removed temporary directory %s", temp_dir)
        except OSError as exc:
            logger.warning("Failed to remove temporary directory %s: %s", temp_dir, exc)


@task(name="Persist Gemini response to MongoDB")
def persist_response(
    mongo_uri: str,
    database: str,
    collection: str,
    gemini_payload: dict[str, Any],
    video_url: str,
    video_payload: dict[str, Any],
    screenshots: dict[str, Any] | None = None,
) -> str:
    """Persist the Gemini output and source metadata in MongoDB."""
    logger = get_run_logger()
    document = {
        "video_url": video_url,
        "video_payload": video_payload,
        "output": gemini_payload,
    }
    if screenshots is not None:
        document["screenshots"] = screenshots

    with MongoClient(mongo_uri, serverSelectionTimeoutMS=5000) as client:
        result = client[database][collection].insert_one(document)
        inserted_id = str(result.inserted_id)

    logger.info("Stored Gemini response under MongoDB document %s", inserted_id)
    return inserted_id


# --- Local persistence fallback (RUN_LOCAL), remove when Mongo is mandatory ---
@task(name="Persist Gemini response locally", persist_result=False)
def persist_response_locally(document: dict[str, Any]) -> str:
    """Persist the Gemini output to a local JSON file when MongoDB is disabled."""
    logger = get_run_logger()
    output_dir = Path(__file__).resolve().parent / "local_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    safe_stub = "".join(
        character if character.isalnum() else "-" for character in document["video_url"]
    )[:60].strip("-") or "video"

    file_path = output_dir / f"{safe_stub}-{uuid4().hex}.json"
    file_path.write_text(json.dumps(document, indent=2), encoding="utf-8")

    logger.info("Saved Gemini response locally to %s", file_path)
    return str(file_path)


@flow(name="Apify to Gemini pipeline")
def apify_to_gemini_flow(config: PipelineConfig) -> dict[str, Any]:
    """Prefect entry point that orchestrates the full pipeline."""
    logger = get_run_logger()
    logger.info("Starting Apify to Gemini pipeline")

    items = run_apify_actor.submit(config).result()
    artifact = select_video_payload.submit(items).result()
    video_path = download_video.submit(artifact).result()
    gemini_response_future = call_gemini.submit(
        video_path,
        config.system_prompt,
        config.gemini_api_key,
        config.gemini_model,
    )

    gemini_response = gemini_response_future.result()

    screenshots_future = extract_screenshots.submit(video_path, gemini_response)
    screenshots = screenshots_future.result()

    document = {
        "video_url": artifact.url,
        "video_payload": artifact.payload,
        "output": gemini_response,
    }
    if screenshots:
        document["screenshots"] = screenshots

    if config.run_local:
        document_id = persist_response_locally.submit(document).result()
    else:
        document_id = persist_response.submit(
            config.mongo_uri or "",
            config.mongo_database or "",
            config.mongo_collection or "",
            gemini_response,
            artifact.url,
            artifact.payload,
            screenshots,
        ).result()

    cleanup_future = delete_local_video.submit(
        video_path, wait_for=[gemini_response_future, screenshots_future]
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
        "screenshots": screenshots,
    }

