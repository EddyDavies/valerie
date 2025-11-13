"""Prefect flow that pulls video data from Apify, analyzes it with Gemini, and
stores the output alongside an S3 reference in MongoDB."""

from __future__ import annotations

import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import boto3
import google.generativeai as genai
import httpx
from pymongo import MongoClient
from prefect import flow, get_run_logger, task


@dataclass(slots=True)
class PipelineConfig:
    """Configuration required to execute the Apify to Gemini pipeline."""

    apify_dataset_id: str
    apify_token: str
    gemini_api_key: str
    system_prompt: str
    mongo_uri: str
    mongo_database: str
    mongo_collection: str
    s3_bucket: str
    gemini_model: str = "gemini-1.5-pro-latest"
    video_url_key: str = "videoUrl"
    s3_prefix: str | None = None
    aws_region: str | None = None


@dataclass(slots=True)
class VideoArtifact:
    """Lightweight container for video metadata extracted from Apify."""

    url: str
    payload: dict[str, Any]


class PipelineError(RuntimeError):
    """Raised when the pipeline cannot proceed with the supplied inputs."""


@task(name="Fetch Apify dataset items", retries=3, retry_delay_seconds=10)
def fetch_apify_items(config: PipelineConfig) -> list[dict[str, Any]]:
    """Retrieve dataset items for the given Apify dataset."""
    logger = get_run_logger()
    logger.info("Fetching Apify dataset %s", config.apify_dataset_id)

    dataset_url = f"https://api.apify.com/v2/datasets/{config.apify_dataset_id}/items"
    params = {"token": config.apify_token, "format": "json"}

    with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
        response = client.get(dataset_url, params=params)
        response.raise_for_status()
        items = response.json()

    if not isinstance(items, list) or not items:
        raise PipelineError(
            "Apify dataset returned no items. Check dataset ID and permissions."
        )

    logger.info("Retrieved %s records from Apify dataset", len(items))
    return items


@task(name="Select video payload")
def select_video_payload(
    items: Iterable[dict[str, Any]], video_url_key: str
) -> VideoArtifact:
    """Pick the first dataset item that contains a downloadable video URL."""
    logger = get_run_logger()
    for item in items:
        if not isinstance(item, dict):
            continue
        url = item.get(video_url_key) or item.get(video_url_key.lower())
        if url:
            logger.info("Selected video URL %s", url)
            return VideoArtifact(url=url, payload=item)

    raise PipelineError(
        f"No items with key '{video_url_key}' were found in the Apify dataset."
    )


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


@task(name="Upload video to S3", retries=2, retry_delay_seconds=15)
def upload_to_s3(
    video_path: Path, bucket: str, prefix: str | None, region: str | None
) -> str:
    """Upload the downloaded video to S3 and return the object URI."""
    logger = get_run_logger()
    logger.info("Uploading %s to S3 bucket %s", video_path, bucket)

    file_suffix = video_path.suffix or ".mp4"
    safe_prefix = (prefix or "").strip("/")
    object_key = f"{safe_prefix + '/' if safe_prefix else ''}{uuid.uuid4().hex}{file_suffix}"

    s3_client = boto3.client("s3", region_name=region) if region else boto3.client("s3")
    s3_client.upload_file(str(video_path), bucket, object_key)

    s3_uri = f"s3://{bucket}/{object_key}"
    logger.info("Uploaded video to %s", s3_uri)
    return s3_uri


@task(name="Persist Gemini response to MongoDB")
def persist_response(
    mongo_uri: str,
    database: str,
    collection: str,
    s3_uri: str,
    gemini_payload: dict[str, Any],
) -> str:
    """Persist only the Gemini output and S3 reference in MongoDB."""
    logger = get_run_logger()
    document = {
        "s3_uri": s3_uri,
        "output": gemini_payload,
    }

    with MongoClient(mongo_uri, serverSelectionTimeoutMS=5000) as client:
        result = client[database][collection].insert_one(document)
        inserted_id = str(result.inserted_id)

    logger.info("Stored Gemini response under MongoDB document %s", inserted_id)
    return inserted_id


@flow(name="Apify to Gemini pipeline")
def apify_to_gemini_flow(config: PipelineConfig) -> dict[str, Any]:
    """Prefect entry point that orchestrates the full pipeline."""
    logger = get_run_logger()
    logger.info("Starting Apify to Gemini pipeline")

    items = fetch_apify_items.submit(config).result()
    artifact = select_video_payload.submit(items, config.video_url_key).result()
    video_path = download_video.submit(artifact).result()
    gemini_response_future = call_gemini.submit(
        video_path,
        config.system_prompt,
        config.gemini_api_key,
        config.gemini_model,
    )

    s3_uri_future = upload_to_s3.submit(
        video_path,
        config.s3_bucket,
        config.s3_prefix,
        config.aws_region,
    )
    gemini_response = gemini_response_future.result()
    s3_uri = s3_uri_future.result()

    document_id = persist_response.submit(
        config.mongo_uri,
        config.mongo_database,
        config.mongo_collection,
        s3_uri,
        gemini_response,
    ).result()

    logger.info(
        "Pipeline complete; stored output under MongoDB document %s referencing %s",
        document_id,
        s3_uri,
    )
    return {
        "document_id": document_id,
        "video_path": str(video_path),
        "s3_uri": s3_uri,
    }

