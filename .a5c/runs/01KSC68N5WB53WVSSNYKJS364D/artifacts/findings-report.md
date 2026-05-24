# Issue #2 — Sensitive-Data Leak Audit: Findings Report

**Scope covered:** checklist §1–7 (output/logging calls, indirect leaks, file writes, config & fixtures, log configuration, `__repr__`/`__str__`, git history). Audit-only — no code was changed.

**Headline:** No checked-in secrets and no committed-history leaks (clean). The real exposure is **runtime**: two actual-leak paths plus a systemic "log/print the raw exception" pattern.

## Counts (25 distinct sites)
- 🔴 **actual-leak: 2**
- 🟠 **possible-leak: 19**
- ⚪ **theoretical: 4**

Grouped into 7 remediation items below (G1–G7). Recommendation = my suggested disposition; you decide at the review gate.

---

### 🔴 G1 — Unfiltered user-config dump to file *(actual-leak)* — **recommend FIX**
- `utils/resume_helper.py:354` — `json.dump(config, f, indent=2)` writes the **entire user config** to `output/resume_config_*.json`. If `config.json` holds `alma.api_key`, AWS secret, or token fields, they land verbatim in a plaintext file.
- **Fix:** sanitize before dump — drop/redact keys matching `api_key|secret|token|password|credential|authorization`; keep only safe keys (paths, codes, IDs).

### 🔴 G2 — Raw exception printed to stdout *(actual-leak)* — **recommend FIX**
- `alma_digital_upload.py:662` — `print(f"\nERROR: {e}")` in the top-level handler. Bypasses logging entirely; a `requests`/`boto3`/`almaapitk` exception can embed a credential-bearing URL or auth detail.
- **Fix:** print exception **type** only (`type(e).__name__`) and/or route through the logger with a sanitized message; full detail to the file log only if scrubbed.

### 🟠 G3 — Systemic "log the raw exception" pattern *(possible-leak, ~16 sites)* — **recommend FIX**
`logger.error(f"...: {e}")` / `str(e)` across the codebase. API/boto3 exceptions are the credential-relevant ones; FS-error ones mostly leak paths (lower impact) but share the same fix.
- Credential-relevant: `alma_digital_upload.py:344`, `:499`, `:342` (stores `str(e)` in `result.metadata['representation_error']`), `utils/marc_extraction.py:172`
- Path/FS detail: `utils/folder_matching.py:100,132,266`; `utils/resume_helper.py:152,242,307,360`; `utils/marc_extraction.py:285`; `strategies/marc_907e_strategy.py:192,217`; `utils/folder_renaming.py:312,410`
- **Fix:** log `type(e).__name__` (plus the safe identifier already present — `mms_id`/`filename`), not the full exception. Drop `str(e)` from stored metadata (`:342`).

### 🟠 G4 — Log-config hardening for dependency loggers *(possible-leak)* — **recommend FIX (cheap)**
- `alma_digital_upload.py:399-404` — `boto3.resource()` gets explicit AWS keys; with the root logger at DEBUG, a stray `boto3`/`botocore`/`urllib3` DEBUG line could expose request-signing detail.
- `alma_digital_upload.py:60-66` — root + FileHandler at DEBUG, no repo-side scrubbing filter (relies entirely on almaapitk 0.4.5 redaction).
- **Fix:** in `setup_logging()`, pin `logging.getLogger('botocore'|'boto3'|'urllib3').setLevel(logging.WARNING)`; consider FileHandler at INFO (or add a repo-side `logging.Filter`).

### 🟠 G5 — Error string written into TSV report *(possible-leak)* — **recommend FIX (with G3)**
- `utils/folder_renaming.py:396` — `writer.writerow([..., result.error or ''])` writes raw OS error text (FS paths) into `rename_report_*.tsv`.
- **Fix:** write error **type/code** only; strip paths. Same disposition as G3.

### ⚪ G6 — Output files may pass through input PII *(theoretical)* — **recommend DEFER (document)**
- `utils/resume_helper.py:296` (input-TSV passthrough), `utils/folder_matching.py:260` (folder names), `utils/marc_extraction.py:271` (MARC 907 values). Leakage is data-dependent, not a code defect.
- **Disposition:** document that inputs/outputs may contain PII; optional column whitelist/anonymization later. Out of scope to redesign here.

### ⚪ G7 — "AWS credentials loaded" info log *(theoretical / actually safe)* — **no change**
- `alma_digital_upload.py:431` — message contains no values. Listed for completeness; no action.

---

## Patterns worth remembering
1. **Never interpolate `{e}`/`str(e)` into logs or prints** for exceptions from network/cloud libs — log `type(e).__name__` + a safe id. This is the dominant pattern here (G2, G3, G5).
2. **Never serialize a whole user config to an output file** without a redaction allow/deny list (G1).
3. **Pin third-party loggers (`boto3`, `botocore`, `urllib3`) to WARNING** when your root logger runs at DEBUG (G4).
4. Toolkit-level redaction (almaapitk 0.4.5) protects the toolkit's own output, **not** strings this repo builds from exceptions — defense-in-depth still needed repo-side.

## Out of scope (per issue)
- No git-history rewrite (history is clean anyway).
- No broad logging-architecture refactor — targeted fixes only.
