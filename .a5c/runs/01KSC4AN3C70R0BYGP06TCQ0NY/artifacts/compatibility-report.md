# almaapitk 0.3.1 → 0.4.5 — Compatibility Report (Issue #1)

**Verdict: ✅ ALIGNED — safe to commit.** All offline verification gates passed.

## Change
- `pyproject.toml`: `almaapitk = ">=0.3.1"` → `almaapitk = ">=0.4.5"`
- Locked & installed: **almaapitk 0.4.5** (via `poetry lock` + `poetry install`)
- Note: `poetry.lock` is **gitignored** in this repo, so the committed change is the `pyproject.toml` pin only.
- The jump crosses a full minor (0.3.x → 0.4.x), so the used API surface was verified, not assumed.

## Gate 1 — Test suite
`poetry run pytest -q` → **25 passed**, exit 0.

## Gate 2 — API surface (introspection of installed 0.4.5)
All 10 inventoried usages resolve with compatible signatures; **0 breaking changes**.

| Symbol | Present | Sig OK | Note |
|---|---|---|---|
| `AlmaAPIClient(environment)` | ✅ | ✅ | `environment` positional still accepted (defaults `'SANDBOX'`); other params keyword-only w/ defaults |
| `AlmaAPIClient.test_connection()` | ✅ | ✅ | `(self) -> bool` |
| `Admin(client)` | ✅ | ✅ | one positional client |
| `Admin.get_set_info(set_id)` | ✅ | ✅ | positional |
| `Admin.get_bib_set_members(set_id)` | ✅ | ✅ | positional |
| `BibliographicRecords(client)` | ✅ | ✅ | one positional client |
| `BibliographicRecords.get_representations(mms_id)` | ✅ | ✅ | `representation_id` optional |
| `BibliographicRecords.create_representation(mms_id, access_rights_value, access_rights_desc, lib_code, usage_type)` | ✅ | ✅ | all 5 keywords exist; `usage_type` now has a default — non-breaking |
| `BibliographicRecords.link_file_to_representation(mms_id, representation_id, file_path)` | ✅ | ✅ | all 3 keywords exist & supplied |
| `BibliographicRecords.get_marc_subfield(mms_id, field, subfield)` | ✅ | ✅ | all positional |

## Gate 3 — Credential redaction (offline, fake key)
The security fix is active for this consumer's logging path. With a **fake** Authorization header and no API calls:
- `redact_sensitive_data()` redacts nested `Authorization` (default patterns now include `authorization`).
- `TextFormatter(use_colors=False).format(record_with_headers)` renders: `(headers={'Authorization': '***REDACTED***'})`.
- `JSONFormatter` parity confirmed.
- Fake key never appeared in any formatter output.

This repo does not use almaapitk logging internals directly, so the fix applies transparently with no consumer code change.

## Scope note
Phase 1 (sandbox/main) only, per Issue #1. Live-sandbox spot-check and prod rollout (Phase 2) are out of scope here and tracked separately in the issue.
