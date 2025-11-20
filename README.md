## Apify to ClipTagger Pipeline

This project contains a Prefect flow that

- fetches the latest dataset items from Apify,
- downloads the referenced video file,
- samples keyframes and sends them to the ClipTagger-12b API, and
- stores the structured analysis and source metadata in MongoDB.

### 1. Setup

```bash
uv sync
```

If `APIFY_ACTOR_ID` is omitted, the backend defaults to `clockworks~tiktok-video-scraper`.

Create a `.env` file in the project root with:

```
APIFY_API_TOKEN=your-apify-api-token
CLIPTAGGER_API_KEY=your-cliptagger-token
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net
MONGO_DATABASE=app
MONGO_COLLECTION=cliptagger_outputs
# Optional overrides
RUN_LOCAL=false
APIFY_ACTOR_ID=clockworks~tiktok-video-scraper
CLIPTAGGER_API_URL=https://api.inference.net/v1
CLIPTAGGER_MODEL=inference-net/cliptagger-12b
CLIPTAGGER_SAMPLE_INTERVAL_SECONDS=2.0
CLIPTAGGER_MAX_FRAMES=150
```

The ClipTagger system and user prompts are baked into the pipeline to match the
required production settings from GrassData.

### 2. Run the flow locally

```bash
uv run python main.py --url "https://vm.tiktok.com/your-video/"
```

You can also trigger it via Prefect CLI:

```bash
uv run prefect flow run --name "Apify to ClipTagger pipeline"
```

The flow logs each step in Prefect and prints the MongoDB document ID and source video URL at the end.

### 3. Implementation notes

- `valerie.pipeline.PipelineConfig` holds all runtime options.
- The flow retries network steps (Apify and download) to tolerate transient issues.
- ClipTagger frame images are stored in a temporary directory and removed after analysis.
- Apify runs use the `run-sync-get-dataset-items` endpoint, so the flow waits for the actor to complete and uses the returned dataset items.
- Only the ClipTagger frame annotations, the original Apify payload, and the source video URL are saved in MongoDB.
- The pipeline samples frames with `ffmpeg` (configurable via environment variables) before passing them to ClipTagger.
- The Apify actor input is generated dynamically; pass the target URL via `--url` or set `APIFY_POST_URL`.
- Set `RUN_LOCAL=true` to activate the temporary JSON fallback (`backend/valerie/local_outputs/`) when MongoDB is unavailable. Remove this flag once Mongo connectivity is guaranteed.

