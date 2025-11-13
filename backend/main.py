import json
from os import environ
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from valerie.pipeline import PipelineConfig, apify_to_gemini_flow


def load_apify_input() -> dict[str, Any]:
    """Load Apify actor input from a file path or inline JSON."""
    if "APIFY_INPUT_PATH" in environ:
        input_path = Path(environ["APIFY_INPUT_PATH"]).expanduser().resolve()
        if not input_path.exists():
            raise RuntimeError(f"APIFY_INPUT_PATH does not exist: {input_path}")
        return json.loads(input_path.read_text())

    if "APIFY_INPUT_JSON" in environ:
        return json.loads(environ["APIFY_INPUT_JSON"])

    raise RuntimeError(
        "Provide either APIFY_INPUT_PATH pointing to a JSON file or "
        "APIFY_INPUT_JSON containing the serialized actor input."
    )


def build_config() -> PipelineConfig:
    """Construct a pipeline configuration from environment variables."""
    required_keys = [
        "APIFY_ACTOR_ID",
        "APIFY_API_TOKEN",
        "GEMINI_API_KEY",
        "MONGO_URI",
        "MONGO_DATABASE",
        "MONGO_COLLECTION",
    ]

    missing = [key for key in required_keys if key not in environ]
    if missing:
        missing_keys = ", ".join(missing)
        raise RuntimeError(
            f"Missing required environment variables: {missing_keys}. "
            "Define them in your shell or .env file."
        )

    return PipelineConfig(
        apify_actor_id=environ["APIFY_ACTOR_ID"],
        apify_token=environ["APIFY_API_TOKEN"],
        apify_input=load_apify_input(),
        gemini_api_key=environ["GEMINI_API_KEY"],
        system_prompt=environ.get(
            "GEMINI_SYSTEM_PROMPT",
            "You are a helpful assistant that extracts insights from video content.",
        ),
        mongo_uri=environ["MONGO_URI"],
        mongo_database=environ["MONGO_DATABASE"],
        mongo_collection=environ["MONGO_COLLECTION"],
        gemini_model=environ.get("GEMINI_MODEL", "gemini-1.5-pro-latest"),
        video_url_key=environ.get("APIFY_VIDEO_URL_KEY", "videoUrl"),
    )


def main() -> None:
    """Entrypoint used by Prefect CLI and local execution."""
    load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env", override=False)
    config = build_config()
    result = apify_to_gemini_flow(config)
    print("Pipeline finished:", result)


if __name__ == "__main__":
    main()
