# almaapitk 0.4.6 → 0.7.0 compatibility audit

**Date:** 2026-11-11 · **Issue:** #8 · **Protocol:** `docs/almaapitk-bump-protocol.md`
**Method:** AlmaAPITK `CHANGELOG.md` sections 0.5.0, 0.5.1, 0.6.0 and 0.7.0, read against this
repo's usage surface, plus the toolkit source for signatures. The pin floor was `>=0.4.6`. The
lock is gitignored, and the local one had 0.5.1, so the audit covers everything from 0.4.6.

**Verdict: SAFE.** No code change.

## (a) Usage surface

| Symbol | Where |
|---|---|
| `AlmaAPIClient(environment)`, `Admin(client)`, `BibliographicRecords(client)` | `alma_digital_upload.py` l.191–197 |
| `Admin.get_set_info(set_id)` | l.212 |
| `Admin.get_bib_set_members(set_id)` | l.225 |
| `BibliographicRecords.get_representations(mms_id)` | l.291 |
| `BibliographicRecords.create_representation(...)` | l.320 |
| `BibliographicRecords.link_file_to_representation(...)` | l.487 |
| `BibliographicRecords.get_marc_subfield(mms_id, field, subfield)` | `strategies/marc_907e_strategy.py` l.114, `utils/marc_extraction.py` l.81–82 |

## (b) Upstream changes that touch this surface

- **0.5.0, #190.** `get_marc_subfield` gains `strict=False`. With the default, behaviour is as
  before: `[]` when the subfield is absent. Control-field tags (00X) are now rejected. This repo
  reads 907, a data field.
- **0.5.0.** The MARC editing fixes (#184–#186), `build_alma_bib_xml` content designation
  (#187) and `DEFAULT_LEADER` (#188) are not used here.
- **0.6.0, #196 and #208.** `AlmaTimeoutError` and `AlmaServerError` are `AlmaAPIError`s that
  replace raw `requests` `Timeout`/`RetryError`. `Retry-After` waits are capped at 120 s.
- **0.7.0, #205.** 401, 429 and 5xx outrank mapped codes. #139 changed the Configuration lists,
  which aren't used here. `iter_paged` now handles a lone dict record.
- No changelog entry since 0.4.6 touches `get_set_info`, `get_bib_set_members`,
  `get_representations`, `create_representation` or `link_file_to_representation`.

## (c) Classification

Every symbol in (a) is **unchanged**. `get_marc_subfield` is **compatible** (new optional flag,
default unchanged).

## (d) Repo-specific risks

- **Duplicate representations:** none added. POST is still not retried (since 0.4.6), and the
  existing-representation check runs before each create.
- **Set members:** `get_bib_set_members` is unchanged.
- **Error handling:** the repo catches generic exceptions around each record, and the new typed
  errors are `Exception`s like before.

## (e) Result

- **Pin:** `>=0.7.0`. Locally, `poetry update almaapitk` resolves 0.7.0 and
  `poetry check --lock` passes. The lock isn't committed (gitignored).
- **L2:** 25 passed.
- **L3:** not run in the TAU sandbox, because it uploads. The owner uploads one test file on
  SANDBOX (Sandbox bucket) before the next production use.
