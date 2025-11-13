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

Create a `.env` file in the project root with:

```
APIFY_ACTOR_ID=clockworks~tiktok-video-scraper
APIFY_API_TOKEN=your-apify-api-token
GEMINI_API_KEY=your-google-gemini-key
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net
MONGO_DATABASE=app
MONGO_COLLECTION=gemini_outputs
# Either point to an input file or provide inline JSON (choose one)
APIFY_INPUT_PATH=./apify-input.json
# APIFY_INPUT_JSON={"postURLs":["https://vm.tiktok.com/ZNdE8MYnM/"],"resultsPerPage":100,...}
# Optional overrides
GEMINI_SYSTEM_PROMPT=You are a helpful assistant...
GEMINI_MODEL=gemini-1.5-pro-latest
APIFY_VIDEO_URL_KEY=videoUrl
```

The sample `APIFY_INPUT_JSON` above mirrors the TikTok scraper input:

```json
{
  "postURLs": ["https://vm.tiktok.com/ZNdE8MYnM/"],
  "resultsPerPage": 100,
  "scrapeRelatedVideos": false,
  "shouldDownloadCovers": false,
  "shouldDownloadSlideshowImages": false,
  "shouldDownloadSubtitles": true,
  "shouldDownloadVideos": true
}
```

### 2. Run the flow locally

```bash
uv run python main.py
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

