import argparse
from copy import deepcopy
from os import environ
from pathlib import Path
from typing import Any
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from valerie.pipeline import PipelineConfig, apify_to_gemini_flow

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
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


def load_system_prompt(prompt_name: str) -> str:
    """Render a system prompt from the prompts directory using Jinja."""
    template_name = f"{prompt_name}.jinja"
    environment = Environment(loader=FileSystemLoader(PROMPTS_DIR))
    try:
        template = environment.get_template(template_name)
    except TemplateNotFound as exc:
        available_templates = sorted(
            template_path.name for template_path in PROMPTS_DIR.glob("*.jinja")
        )
        raise RuntimeError(
            f"Prompt template '{template_name}' not found in {PROMPTS_DIR}. "
            f"Available templates: {', '.join(available_templates) or 'none'}."
        ) from exc

    return template.render()


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
        description="Run the Apify → Gemini pipeline against a single video URL."
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
        "GEMINI_API_KEY",
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

    return PipelineConfig(
        apify_actor_id=environ.get("APIFY_ACTOR_ID", DEFAULT_APIFY_ACTOR_ID),
        apify_token=environ["APIFY_API_TOKEN"],
        apify_input=build_apify_input(video_url),
        gemini_api_key=environ["GEMINI_API_KEY"],
        system_prompt=load_system_prompt(
            environ.get("GEMINI_PROMPT_NAME", "describe_video_timeline")
        ),
        mongo_uri=None if run_local else environ["MONGO_URI"],
        mongo_database=None if run_local else environ["MONGO_DATABASE"],
        mongo_collection=None if run_local else environ["MONGO_COLLECTION"],
        gemini_model=environ.get("GEMINI_MODEL", "gemini-1.5-pro"),
        run_local=run_local,
    )


def main() -> None:
    """Entrypoint used by Prefect CLI and local execution."""
    args = parse_args()
    load_dotenv()
    config = build_config(args.url)
    result = apify_to_gemini_flow(config)
    print("Pipeline finished:", result)


if __name__ == "__main__":
    main()
