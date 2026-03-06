# Demo Script (Carya Eagle Eye)

## Goal
Demonstrate end-to-end signal extraction and explain how data quality is enforced.

## Demo Flow

1. Context (1 minute)
- Explain problem: unstructured funding announcements and lead-signal ambiguity.

2. Backend walkthrough (3 minutes)
- Show ingestion/extraction/storage boundaries in `src`.
- Explain one key extraction safeguard.

3. Frontend walkthrough (2 minutes)
```bash
cd frontend
npm run dev
```
- Show a tracked record and what fields are critical.

4. Test path (1 minute)
Prerequisites: Python 3.11+ and dependencies installed.
```bash
python3 -m pytest tests -q
```

5. Close (1 minute)
- Known limitations and next engineering step.

## Interview Talking Points

- Why extraction strictness matters for downstream trust.
- One bug I fixed that changed data quality materially.
- What I would optimize next for scale/performance.
