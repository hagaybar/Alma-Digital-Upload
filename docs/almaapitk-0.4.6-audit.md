# almaapitk 0.4.5 → 0.4.6 Compatibility Audit

**Date:** 2026-07-08
**Scope:** Does bumping `almaapitk` from `>=0.4.5` to `>=0.4.6` (latest PyPI
release) break this repo — a **manually-run, mutating** job that creates Alma
digital representations and links S3 files to them (`alma_digital_upload.py`)?
Part of the consumer-rollout gate (meta-issue #158).

**Verdict: SAFE.**

## Usage surface

| Element | Site | Detail |
|---|---|---|
| Import | `alma_digital_upload.py:37` | `from almaapitk import AlmaAPIClient, Admin, BibliographicRecords` |
| Client | `:191` | `AlmaAPIClient(self.environment)` — single positional (`SANDBOX`/`PRODUCTION`), env-var key fallback |
| Domains | `:196–197` | `Admin(client)`, `BibliographicRecords(client)` |
| Read (GET) | `:291` | `bibs.get_representations(mms_id)` |
| Write (POST) | `:320` | `bibs.create_representation(mms_id, access_rights_value, access_rights_desc, lib_code, usage_type=…)` |
| Write | `:487` | `bibs.link_file_to_representation(mms_id, representation_id, file_path)` |

## 0.4.5 → 0.4.6 deltas that reach this repo

- **POST no longer auto-retried (#166)** — the one behavior change. `create_representation`
  is a POST; in 0.4.5 a transient 429/5xx was silently retried, in 0.4.6 it surfaces to the
  caller. This is the **correct posture for a non-idempotent create** (no duplicate
  representation on a lost-response retry). The repo already wraps the create in try/except
  (`representation_action="error"`); as a manually-run job the operator simply re-runs. **No
  code change.**
- **Logging defaults quieter (#142)** — INFO by default, no side log file, bodies not logged.
  Cosmetic here; the repo drives its own logging.
- **Added, unused:** `api_key=` ctor injection, `CredentialError`. No impact.
- **Fixed (#164/#162/#163/#167):** in domains/methods this repo does not call. No impact.

The three `BibliographicRecords` representation methods this repo depends on are unchanged
in signature and behavior across the bump.

## Validation (offline, 2026-07-08)

- `poetry lock` + `poetry install` → almaapitk **0.4.6** resolved and installed.
- Surface smoke: `get_representations` / `create_representation` /
  `link_file_to_representation` present with unchanged signatures; `AlmaAPIClient(environment)`
  unchanged.
- **`poetry run pytest` → 25 passed.**

## Not yet done (operator steps)

`main` → `prod` fast-forward + masedet `git pull` / `poetry install` are operator steps.
This is a manually-run repo, so the operator is the live backstop — run the upload workflow
against SANDBOX once before promoting.
