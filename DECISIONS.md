# Engineering Decisions

## 1) Keep config in environment variables
I chose env-based config so deployments remain portable across local, CI, and cloud runtimes.
Tradeoff: setup is stricter and requires reliable secret management.

## 2) Prefer small, composable services over one large module
I optimized for easier debugging and safer refactors.
Tradeoff: more files and explicit wiring.

## 3) Add blocking secret scanning in CI
I treat secret leaks as release blockers.
Tradeoff: occasional false positives need explicit triage.

## 4) Keep docs operational, not aspirational
I removed speculative roadmap language and kept runbooks tied to real workflows.
Tradeoff: less marketing-style presentation, more technical specificity.

## 5) Bias for deterministic behavior in core flows
I favor explicit defaults, bounded retries, and clear failure paths.
Tradeoff: slightly more boilerplate in control logic.
