## Security audit complete — sensitive-data leak review

Worked through the full checklist (§1–7: output/logging calls, indirect leaks, file writes, config & fixtures, log configuration, `__repr__`/`__str__`, git history). Audit-first; fixes applied after review.

### Results
- **Findings: 25** sites — 🔴 2 actual-leak, 🟠 19 possible-leak, ⚪ 4 theoretical (grouped into 7 remediation items G1–G7).
- **Fixed: 21** (groups G1–G5) + **2** latent fields hardened.
- **Deferred: 4** — 3 theoretical PII-passthrough sites (G6) + 1 no-op (G7).
- **Clean:** no checked-in secrets (`.env` absent, `config.example.json` placeholders only, creds via env vars); no credentials in git history (`*.log` is gitignored); no `__repr__`/`__str__` leaks. **No history rewrite needed.**

### Fixed (commit `ae0f4ca`, branch `main`)
- **G1 (actual):** `resume_helper.py` now redacts secret-named keys (`api_key|secret|token|password|credential|authorization|auth`) before writing `resume_config_*.json`.
- **G2 (actual):** top-level handler prints the exception **type** only, not the raw exception (was `print(f"ERROR: {e}")`).
- **G3 (possible, 16 sites):** `logger.error(f"...: {e}")` → `{type(e).__name__}` across the codebase; `representation_error` metadata stores the type, not `str(e)`.
- **G4 (possible):** `botocore`/`boto3`/`urllib3` loggers pinned to `WARNING` in `setup_logging()` (the app's own DEBUG file logging is intentionally kept).
- **G5 (possible):** `rename_report` TSV writes a sanitized error category, not raw OS error text.
- **+2 hardening:** `ExtractionResult.error` / `MatchResult.error` store the exception type instead of `str(e)`.

**Traceability preserved** (owner requirement): every error message still identifies the specific record/file (`mms_id` / `filename` / `folder` / `bib id`); only the raw exception text was removed.

### Deferred (consciously)
- **G6** — output TSV/JSON may pass through PII *from the input data* (`resume_tsv`, folder-name report, MARC 907 export). Data-dependent, not a code defect; recommend documenting input/output handling and an optional column allow-list later.
- **G7** — `"AWS credentials loaded for <env>"` info log contains no secret value; left as-is.

### Patterns worth remembering
1. Never interpolate `{e}`/`str(e)` for network/cloud/FS exceptions — log `type(e).__name__` + a safe identifier.
2. Never serialize a whole user config to a file without a redaction allow/deny list.
3. Pin `boto3`/`botocore`/`urllib3` to WARNING when the root logger runs at DEBUG.
4. Toolkit-level redaction (almaapitk 0.4.5) protects the toolkit's own output, **not** strings this repo builds from exceptions — defense-in-depth still needed repo-side.

Verified: 25 tests pass; no raw-exception emissions remain.
