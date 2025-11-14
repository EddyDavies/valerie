## Apify to Gemini Pipeline

This project contains a Prefect flow that

- fetches the latest dataset items from Apify,
- downloads the referenced video file,
- sends the video to the Gemini API with an optional system prompt, and
- stores the Gemini output and source metadata in MongoDB.

### 1. Setup

```bash
uv sync
```

If `APIFY_ACTOR_ID` is omitted, the backend defaults to `clockworks~tiktok-video-scraper`.

Create a `.env` file in the project root with:

```
APIFY_API_TOKEN=your-apify-api-token
GEMINI_API_KEY=your-google-gemini-key
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net
MONGO_DATABASE=app
MONGO_COLLECTION=gemini_outputs
# Optional overrides
RUN_LOCAL=false
APIFY_ACTOR_ID=clockworks~tiktok-video-scraper
GEMINI_PROMPT_NAME=describe_video_timeline
GEMINI_MODEL=gemini-2.5-pro
```

System prompts live in `backend/prompts/` as Jinja templates. Set `GEMINI_PROMPT_NAME`
to the template filename (without `.jinja`). The default `describe_video_timeline`
prompt turns Gemini output into a second-by-second timeline with screenshot suggestions.

### 2. Run the flow locally

```bash
uv run python main.py --url "https://vm.tiktok.com/your-video/"
```

You can also trigger it via Prefect CLI:

```bash
uv run prefect flow run --name "Apify to Gemini pipeline"
```

The flow logs each step in Prefect and prints the MongoDB document ID and source video URL at the end.

### 3. Implementation notes

- `valerie.pipeline.PipelineConfig` holds all runtime options.
- The flow retries network steps (Apify and download) to tolerate transient issues.
- Gemini uploads are cleaned up after each run.
- Apify runs use the `run-sync-get-dataset-items` endpoint, so the flow waits for the actor to complete and uses the returned dataset items.
- Only the Gemini output, the original Apify payload, and the source video URL are saved in MongoDB. 
- When Gemini suggests screenshots, the pipeline uses `ffmpeg` to extract the corresponding frames and stores their metadata alongside the analysis results.
- The Apify actor input is generated dynamically; pass the target URL via `--url` or set `APIFY_POST_URL`.
- Set `RUN_LOCAL=true` to activate the temporary JSON fallback (`backend/valerie/local_outputs/`) when MongoDB is unavailable. Remove this flag once Mongo connectivity is guaranteed.

