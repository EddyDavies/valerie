## Apify to Gemini Pipeline

This project contains a Prefect flow that

- fetches the latest dataset items from Apify,
- downloads the referenced video file,
- sends the video to the Gemini API with an optional system prompt, and
- uploads the video to S3 while storing the Gemini output and S3 reference in MongoDB.

### 1. Setup

```bash
uv sync
```

Create a `.env` file in the project root with:

```
APIFY_DATASET_ID=the-dataset-you-want-to-read
APIFY_API_TOKEN=your-apify-api-token
GEMINI_API_KEY=your-google-gemini-key
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net
MONGO_DATABASE=app
MONGO_COLLECTION=gemini_outputs
S3_BUCKET_NAME=your-s3-bucket
# Optional overrides
GEMINI_SYSTEM_PROMPT=You are a helpful assistant...
GEMINI_MODEL=gemini-1.5-pro-latest
APIFY_VIDEO_URL_KEY=videoUrl
S3_KEY_PREFIX=apify-ingest
AWS_REGION=us-east-1
```

AWS credentials should be available to the process through the usual environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`) or an attached IAM role.

### 2. Run the flow locally

```bash
uv run python main.py
```

You can also trigger it via Prefect CLI:

```bash
uv run prefect flow run --name "Apify to Gemini pipeline"
```

The flow logs each step in Prefect and prints the MongoDB document ID and S3 URI at the end.

### 3. Implementation notes

- `valerie.pipeline.PipelineConfig` holds all runtime options.
- The flow retries network steps (Apify and download) to tolerate transient issues.
- Gemini uploads are cleaned up after each run.
- Only the Gemini output and its S3 reference are saved in MongoDB.

