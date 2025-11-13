from os import environ
from pathlib import Path

from dotenv import load_dotenv

from valerie.pipeline import PipelineConfig, apify_to_gemini_flow


def build_config() -> PipelineConfig:
    """Construct a pipeline configuration from environment variables."""
    required_keys = [
        "APIFY_DATASET_ID",
        "APIFY_API_TOKEN",
        "GEMINI_API_KEY",
        "MONGO_URI",
        "MONGO_DATABASE",
        "MONGO_COLLECTION",
        "S3_BUCKET_NAME",
    ]

    missing = [key for key in required_keys if key not in environ]
    if missing:
        missing_keys = ", ".join(missing)
        raise RuntimeError(
            f"Missing required environment variables: {missing_keys}. "
            "Define them in your shell or .env file."
        )

    return PipelineConfig(
        apify_dataset_id=environ["APIFY_DATASET_ID"],
        apify_token=environ["APIFY_API_TOKEN"],
        gemini_api_key=environ["GEMINI_API_KEY"],
        system_prompt=environ.get(
            "GEMINI_SYSTEM_PROMPT",
            "You are a helpful assistant that extracts insights from video content.",
        ),
        mongo_uri=environ["MONGO_URI"],
        mongo_database=environ["MONGO_DATABASE"],
        mongo_collection=environ["MONGO_COLLECTION"],
        s3_bucket=environ["S3_BUCKET_NAME"],
        gemini_model=environ.get("GEMINI_MODEL", "gemini-1.5-pro-latest"),
        video_url_key=environ.get("APIFY_VIDEO_URL_KEY", "videoUrl"),
        s3_prefix=environ.get("S3_KEY_PREFIX"),
        aws_region=environ.get("AWS_REGION"),
    )


def main() -> None:
    """Entrypoint used by Prefect CLI and local execution."""
    load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env", override=False)
    config = build_config()
    result = apify_to_gemini_flow(config)
    print("Pipeline finished:", result)


if __name__ == "__main__":
    main()
