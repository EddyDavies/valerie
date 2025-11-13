"""Prefect flow that pulls video data from Apify, analyzes it with Gemini, and
stores the output in MongoDB without persisting artifacts to S3."""

from __future__ import annotations

import json
import shutil
import tempfile
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

    try:
        response = model.generate_content([uploaded_file])
        response_dict = response.to_dict()
        logger.info("Gemini response received with %s candidates", len(response.candidates))
    finally:
        genai.delete_file(uploaded_file.name)
        logger.info("Deleted temporary Gemini file %s", uploaded_file.name)

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
) -> str:
    """Persist the Gemini output and source metadata in MongoDB."""
    logger = get_run_logger()
    document = {
        "video_url": video_url,
        "video_payload": video_payload,
        "output": gemini_payload,
    }

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

    document = {
        "video_url": artifact.url,
        "video_payload": artifact.payload,
        "output": gemini_response,
    }

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
        ).result()

    cleanup_future = delete_local_video.submit(
        video_path, wait_for=[gemini_response_future]
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

