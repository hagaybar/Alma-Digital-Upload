# Issue #2 — Sensitive-Data Leak Fixes (applied)

Minimal, surgical edits. Error traceability preserved: every sanitized message
keeps its safe identifier (mms_id, filename, folder path, candidate.old_name,
etc.) and only the raw exception text/credentials were removed, replaced by the
exception type name.

## Applied fixes

| Group | File:Line | Before-shape | After-shape |
|-------|-----------|--------------|-------------|
| G1 | utils/resume_helper.py (json.dump site, ~L384) | `json.dump(config, f, indent=2)` | `json.dump(_redact_config(config), f, indent=2)` — added recursive `_redact_config()` helper + `import re`; redacts dict keys matching `api[_-]?key\|secret\|token\|password\|passwd\|credential\|authorization\|auth` (case-insensitive) to `"***REDACTED***"`, all other keys intact |
| G2 | alma_digital_upload.py (main except, ~L671) | `print(f"\nERROR: {e}")` | `print(f"\nERROR: {type(e).__name__} (see log for details)")` |
| G3 | alma_digital_upload.py (~L348) | `result.metadata['representation_error'] = str(e)` | `= type(e).__name__` |
| G3 | alma_digital_upload.py (~L350) | `logger.error(f"Error processing {mms_id}: {e}")` | `... {mms_id}: {type(e).__name__}` (mms_id kept) |
| G3 | alma_digital_upload.py (~L505) | `logger.error(f"Upload failed for {filename}: {e}")` | `... {filename}: {type(e).__name__}` (filename kept) |
| G3 | utils/marc_extraction.py (~L172) | `logger.error(f"Error processing MMS ID {mms_id}: {e}")` | `... {mms_id}: {type(e).__name__}` (mms_id kept) |
| G3 | utils/marc_extraction.py (~L285) | `logger.error(f"Failed to write TSV file: {e}")` | `... {type(e).__name__}` |
| G3 | utils/folder_matching.py (~L100) | `logger.error(f"Error reading TSV file: {e}")` | `... {type(e).__name__}` |
| G3 | utils/folder_matching.py (~L132) | `logger.error(f"Error listing folders: {e}")` | `... {type(e).__name__}` |
| G3 | utils/folder_matching.py (~L266) | `logger.error(f"Error writing report: {e}")` | `... {type(e).__name__}` |
| G3 | utils/resume_helper.py (~L152) | `logger.error(f"Error reading log info: {e}")` | `... {type(e).__name__}` |
| G3 | utils/resume_helper.py (~L242) | `logger.error(f"Error extracting processed IDs: {e}")` | `... {type(e).__name__}` |
| G3 | utils/resume_helper.py (~L307) | `logger.error(f"Error creating resume TSV: {e}")` | `... {type(e).__name__}` |
| G3 | utils/resume_helper.py (~L360) | `logger.error(f"Error creating resume config: {e}")` | `... {type(e).__name__}` |
| G3 | strategies/marc_907e_strategy.py (~L192) | `logger.error(f"Error matching {mms_id}: {e}")` | `... {mms_id}: {type(e).__name__}` (mms_id kept) |
| G3 | strategies/marc_907e_strategy.py (~L217) | `logger.error(f"Error discovering files in {folder_path}: {e}")` | `... {folder_path}: {type(e).__name__}` (folder_path FS path kept for traceability) |
| G3 | utils/folder_renaming.py (~L312) | `candidate.error = str(e)` + `logger.error(f"Error renaming {candidate.old_name}: {e}")` | `candidate.error = type(e).__name__` + `... {candidate.old_name}: {type(e).__name__}` (old_name kept) |
| G3 | utils/folder_renaming.py (~L410) | `logger.error(f"Failed to write report: {e}")` | `... {type(e).__name__}` |
| G4 | alma_digital_upload.py setup_logging() (after handlers, ~L80) | (none) | Added `logging.getLogger("botocore"/"boto3"/"urllib3").setLevel(logging.WARNING)`. App's own logger left at DEBUG. |
| G5 | utils/folder_renaming.py write_rename_report() (~L396) | `writerow([... result.status, result.error or ''])` | Write a sanitized `error_category = result.error.split(":")[0].strip()` (first token / exception type only); mms_id/old_name/new_name/status intact. |

## Deferred (no change), per instructions

| Group | Item | Reason |
|-------|------|--------|
| G6 | utils/resume_helper.py:296 (TSV passthrough) | Explicitly deferred — copies original TSV rows unchanged; not an exception/credential emission. |
| G6 | utils/folder_matching.py:260 (writes folder names) | Explicitly deferred — folder names are not credentials. |
| G6 | utils/marc_extraction.py:271 (MARC TSV write) | Explicitly deferred — writes mms_id/907$e/907$l_cleaned only; `.error` field is not written. |
| G7 | alma_digital_upload.py:431 `'AWS credentials loaded' info log` | Explicitly deferred — message contains no secret value, only the environment name. |

## Remaining `str(e)` / `{e}` occurrences after fixes (verified out of scope, not emissions)

- `utils/marc_extraction.py:177` — `error=str(e)` stored on `ExtractionResult.error`. Not in the G3 site list; falls under deferred G6 (the TSV writer does not write the `.error` field, so it never reaches output). Stored only on the dataclass.
- `strategies/marc_907e_strategy.py:197` — `error=str(e)` stored on `MatchResult.error`. Not in the G3 site list. Verified `MatchResult.error` is never written to TSV/log/print downstream in this codebase; consumed only via `.matched`/`.status`/`.metadata`. Field-storage, not an emission.
- `alma_digital_upload.py:110` — `raise ValueError(f"Invalid JSON in configuration file: {e}")` for `json.JSONDecodeError`. Not in the fix list. The raised exception is caught in `main()` and after the G2 fix only `type(e).__name__` is printed, so the raw text never reaches stdout. JSONDecodeError carries parse-position info, not credentials.

## Supplementary hardening (orchestrator, defense-in-depth)

Two of the "remaining `str(e)`" stored fields above were latent — never emitted today,
but would leak if a future code path logged them. Hardened to match the pattern:

| File:Line | Before | After |
|-----------|--------|-------|
| utils/marc_extraction.py:177 | `error=str(e)` on `ExtractionResult` | `error=type(e).__name__` |
| strategies/marc_907e_strategy.py:197 | `error=str(e)` on `MatchResult` | `error=type(e).__name__` |

`alma_digital_upload.py:110` (`raise ValueError(... {e})`) was intentionally left: it
is a re-raise caught in `main()` and sanitized by the G2 fix before any output, and a
`JSONDecodeError` message carries parse-position info, not credentials.

## Verification

- Spec grep over the targeted paths for raw-exception **emissions** (log/print/file): **no matches**.
- Only remaining `{e}` in the tree: `alma_digital_upload.py:110` (documented above — not an emission).
- All modified files pass `ast.parse` (syntax OK); full pytest run is the next gate.
