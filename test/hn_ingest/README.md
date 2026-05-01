# HN Ingest Service

Standalone ingestion service for HN Station. Fetches top stories and comments from Hacker News and synchronizes them to a PostgreSQL database.

## Features
- **Polls HN API**: Fetches top stories and recursively indexes comments.
- **AI Summarization**: Integrates with local Ollama (Llama 3) or Google Gemini for automatic summarization of high-quality stories.
- **Dual-Sync Support**: Can broadcast updates to multiple PostgreSQL backends (MultiStore).
- **Rate-Limited**: Intelligent rate-limiting for AI providers to stay within free tiers.

## Configuration

Copy `.env.example` to `.env` and configure:

```bash
DATABASE_URL=postgresql://user:pass@host:5432/dbname
SECONDARY_DATABASE_URL=postgresql://user:pass@other-host:5432/dbname
# OLLAMA_URL=http://localhost:11434
# GEMINI_API_KEY=your_key
# DISABLE_AI=false
```

## Running Locally

```bash
go run ./cmd/ingest/main.go --interval 5m
```

## Docker

```bash
docker build -t hn-ingest .
docker run --env-file .env hn-ingest
```
