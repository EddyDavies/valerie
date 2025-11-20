import argparse
from copy import deepcopy
from os import environ
from typing import Any
from dotenv import load_dotenv

from valerie.pipeline import (
    DEFAULT_CLIPTAGGER_API_URL,
    DEFAULT_CLIPTAGGER_MODEL,
    DEFAULT_FRAME_SAMPLE_INTERVAL_SECONDS,
    DEFAULT_MAX_FRAMES,
    PipelineConfig,
    apify_to_cliptagger_flow,
)
DEFAULT_POST_URL = "https://vm.tiktok.com/ZNdE8MYnM/"
DEFAULT_APIFY_ACTOR_ID = "clockworks~tiktok-video-scraper"
APIFY_INPUT_TEMPLATE: dict[str, Any] = {
    "postURLs": [],
    "resultsPerPage": 100,
    "scrapeRelatedVideos": False,
    "shouldDownloadCovers": False,
    "shouldDownloadSlideshowImages": False,
    "shouldDownloadSubtitles": True,
    "shouldDownloadVideos": True,
}
def build_apify_input(video_url: str | None) -> dict[str, Any]:
    """Assemble the Apify actor input using the configured template."""
    url = video_url or environ.get("APIFY_POST_URL") or DEFAULT_POST_URL
    if not url:
        raise RuntimeError(
            "No video URL provided. Use the --url flag or set APIFY_POST_URL."
        )

    apify_input = deepcopy(APIFY_INPUT_TEMPLATE)
    apify_input["postURLs"] = [url]
    return apify_input


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Apify → ClipTagger pipeline against a single video URL."
    )
    parser.add_argument(
        "--url",
        dest="url",
        help="Video URL to submit to the Apify actor (overrides defaults).",
    )
    return parser.parse_args()


def build_config(video_url: str | None) -> PipelineConfig:
    """Construct a pipeline configuration from environment variables."""
    run_local = environ.get("RUN_LOCAL", "").strip().lower() == "true"

    # --- Local persistence fallback (gated by RUN_LOCAL, remove when Mongo is mandatory) ---
    required_keys = [
        "APIFY_API_TOKEN",
        "CLIPTAGGER_API_KEY",
    ]
    if not run_local:
        required_keys.extend(
            [
                "MONGO_URI",
                "MONGO_DATABASE",
                "MONGO_COLLECTION",
            ]
        )

    missing = [key for key in required_keys if key not in environ]
    if missing:
        missing_keys = ", ".join(missing)
        raise RuntimeError(
            f"Missing required environment variables: {missing_keys}. "
            "Define them in your shell or .env file."
        )

    # cliptagger_api_url = environ.get(
    #     "CLIPTAGGER_API_URL", DEFAULT_CLIPTAGGER_API_URL
    # ).strip()
    # cliptagger_model = environ.get("CLIPTAGGER_MODEL", DEFAULT_CLIPTAGGER_MODEL).strip()

    sample_interval_value = environ.get("CLIPTAGGER_SAMPLE_INTERVAL_SECONDS")
    if sample_interval_value:
        try:
            sample_interval = float(sample_interval_value)
        except ValueError as exc:
            raise RuntimeError(
                "CLIPTAGGER_SAMPLE_INTERVAL_SECONDS must be a valid number."
            ) from exc
        if sample_interval <= 0:
            raise RuntimeError("CLIPTAGGER_SAMPLE_INTERVAL_SECONDS must be > 0.")
    else:
        sample_interval = DEFAULT_FRAME_SAMPLE_INTERVAL_SECONDS

    max_frames_value = environ.get("CLIPTAGGER_MAX_FRAMES")
    if max_frames_value:
        try:
            max_frames = int(max_frames_value)
        except ValueError as exc:
            raise RuntimeError("CLIPTAGGER_MAX_FRAMES must be a valid integer.") from exc
        if max_frames <= 0:
            raise RuntimeError("CLIPTAGGER_MAX_FRAMES must be > 0.")
    else:
        max_frames = DEFAULT_MAX_FRAMES

    return PipelineConfig(
        apify_actor_id=environ.get("APIFY_ACTOR_ID", DEFAULT_APIFY_ACTOR_ID),
        apify_token=environ["APIFY_API_TOKEN"],
        apify_input=build_apify_input(video_url),
        cliptagger_api_key=environ["CLIPTAGGER_API_KEY"],
        mongo_uri=None if run_local else environ["MONGO_URI"],
        mongo_database=None if run_local else environ["MONGO_DATABASE"],
        mongo_collection=None if run_local else environ["MONGO_COLLECTION"],
        # cliptagger_api_url=cliptagger_api_url,
        # cliptagger_model=cliptagger_model or DEFAULT_CLIPTAGGER_MODEL,
        frame_sample_interval_seconds=sample_interval,
        max_frames=max_frames,
        run_local=run_local,
    )


def main() -> None:
    """Entrypoint used by Prefect CLI and local execution."""
    args = parse_args()
    load_dotenv()
    config = build_config(args.url)
    result = apify_to_cliptagger_flow(config)
    print("Pipeline finished:", result)


if __name__ == "__main__":
    main()
