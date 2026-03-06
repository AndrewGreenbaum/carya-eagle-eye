# Carya Eagle Eye

Deal-intelligence system for tracking lead investment signals from target VC funds, extracting structured funding data, and serving that data to a frontend dashboard.

## What I Built

- Backend ingestion + extraction pipeline for funding/deal signals.
- Structured persistence with migration support.
- Frontend dashboard for review and drill-down.
- Test suite around extraction/storage flows.

## Why This Exists

Raw funding content is inconsistent across sources. This project standardizes extraction and tracks lead-signal quality so downstream analysis is reliable.

## Architecture

- Ingestion/scraping scripts for source collection.
- Extraction layer for structured deal parsing.
- Storage layer (`src/archivist`) for normalized persistence.
- API + frontend (`frontend`) for visibility.

## Key Tradeoffs

1. Structured extraction with strict schemas:
Reduces bad data drift; tradeoff is extra handling for ambiguous articles.

2. Source-specific heuristics + shared normalization:
Improves accuracy on noisy inputs; tradeoff is ongoing heuristic maintenance.

3. Clear separation of backend/frontend workspaces:
Keeps deployment and debugging cleaner; tradeoff is more coordination across stacks.

## Run

### Backend
Prerequisites: Python 3.11+ (project requires `>=3.11`).
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Frontend
Prerequisites: Node.js 18+ and npm dependencies installed.
```bash
cd frontend
npm install
npm run dev
```

## Test

Prerequisites: Python 3.11+ and project dependencies installed.

```bash
python3 -m pytest tests -q
```

## Troubleshoot

- Empty dashboard: verify backend data ingest path and API base config.
- Extraction quality drops: review source-specific parsing and confidence thresholds.
- Migration issues: check `alembic` revision state before rerunning ingestion.

## Interview Talking Points

- How I chose schema strictness vs extraction flexibility.
- How I debugged lead-signal misclassification.
- Why I split pipeline modules instead of one extraction script.

## Related Docs

- `DECISIONS.md`
- `BUILD_LOG.md`
- `KNOWN_LIMITATIONS.md`
- `DEMO.md`
- `SECURITY.md`
