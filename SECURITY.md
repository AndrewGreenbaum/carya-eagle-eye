# Security Policy

## Reporting
Report potential vulnerabilities privately with affected path and impact.

## Secrets
- Never commit tokens, private keys, or local key file paths.
- Keep `.env` local and untracked.
- Use placeholder values in examples.

## Incident Steps
1. Revoke exposed credentials.
2. Rotate and redeploy.
3. Remove from current branch.
4. Rewrite history if committed.
5. Re-run secret scans.
