# Engineering Decisions (Carya Eagle Eye)

## 1) Normalize deal records at ingestion boundary
I standardized output shape early to prevent downstream cleanup churn.
Tradeoff: more parser constraints and mapping code.

## 2) Keep extraction and storage concerns separate
Extraction logic and persistence logic are in different layers for safer debugging.
Tradeoff: more cross-module interfaces.

## 3) Frontend and backend as distinct workspaces
Allows independent deploy/debug cycles.
Tradeoff: more env/config sync work.

## 4) Schema strictness over permissive writes
I prefer rejecting ambiguous data over silently storing low-confidence fields.
Tradeoff: some records require manual review.

## 5) Test-first on parsing regressions
I add targeted tests for failure patterns after each parsing bug.
Tradeoff: test suite grows quickly with source variance.
