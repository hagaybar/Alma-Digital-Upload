/**
 * @process alma-digital-upload/sensitive-data-leak-audit
 * @description Audit-first security review for Issue #2: find every code path that could
 *   write sensitive data (API keys, tokens, Authorization headers, credential-bearing URLs,
 *   PII) to stdout/stderr, log files, or output files. Produces a findings report FIRST
 *   (no code changes during audit, per the issue), gates on owner review, then applies only
 *   the approved fixes, verifies, commits, and posts the required summary comment to the issue.
 *   All work respects strict secret-handling: detect patterns and call sites, never print
 *   secret values or dump credential files.
 * @inputs { issue: number, projectRoot: string, maxAttempts: number }
 * @outputs { success: boolean, totalFindings: number, fixed: number, deferred: number, committed: boolean, commented: boolean }
 * @skill alma-api-expert
 */

import { defineTask } from '@a5c-ai/babysitter-sdk';

export async function process(inputs, ctx) {
  const {
    issue = 2,
    projectRoot = '.',
    maxAttempts = 3,
  } = inputs;

  // ==========================================================================
  // PHASE 1: AUDIT (read-only, parallel). NO code changes here — the issue
  // requires a findings report before any fix decisions.
  // ==========================================================================
  const [outputPaths, fileWrites, configLogHistory] = await ctx.parallel.all([
    () => ctx.task(auditOutputPathsTask, { projectRoot }),
    () => ctx.task(auditFileWritesTask, { projectRoot }),
    () => ctx.task(auditConfigLogHistoryTask, { projectRoot }),
  ]);

  // ==========================================================================
  // PHASE 2: CONSOLIDATE FINDINGS into a single reviewed report.
  // ==========================================================================
  const report = await ctx.task(consolidateFindingsTask, {
    outputPaths, fileWrites, configLogHistory, issue,
  });

  // ==========================================================================
  // PHASE 3: OWNER REVIEW of findings — decide what to fix vs defer.
  // ==========================================================================
  const decision = await ctx.breakpoint({
    question: `Security audit complete: ${report.totalFindings} finding(s) (`
      + `${report.severityCounts ? JSON.stringify(report.severityCounts) : 'see report'}). `
      + `Review the findings report and choose how to proceed.`,
    title: 'Review audit findings',
    expert: 'owner',
    tags: ['audit', 'review-gate'],
    options: [
      'Apply all proposed fixes',
      'Apply selected fixes (I will specify)',
      'Defer all (report only)',
      'Abort',
    ],
    context: {
      runId: ctx.runId,
      files: [{ path: `artifacts/findings-report.md`, format: 'markdown' }],
    },
  });

  if (!decision.approved || /abort/i.test(decision.response || '')) {
    return {
      success: false, totalFindings: report.totalFindings, fixed: 0,
      deferred: report.totalFindings, committed: false, commented: false, aborted: true,
      metadata: { processId: 'alma-digital-upload/sensitive-data-leak-audit', issue, timestamp: ctx.now() },
    };
  }

  const wantsFixes = report.totalFindings > 0 && !/defer all|report only/i.test(decision.response || '');

  // ==========================================================================
  // PHASE 4: APPLY APPROVED FIXES (conditional) + verify in a convergence loop.
  // ==========================================================================
  let fixResult = { filesModified: [], fixedCount: 0, deferredCount: report.totalFindings };
  let committed = false;

  if (wantsFixes) {
    let attempt = 0;
    let verified = false;
    let lastFeedback = null;
    let review = null;

    while (attempt < maxAttempts && !verified) {
      attempt++;
      fixResult = await ctx.task(applyFixesTask, {
        report, decision: decision.response, feedback: lastFeedback, attempt,
      });

      const [tests, reverify] = await ctx.parallel.all([
        () => ctx.task(runTestsTask, { attempt }),
        () => ctx.task(reverifyTask, { fixResult, report, attempt }),
      ]);

      review = await ctx.task(fixReviewTask, { fixResult, tests, reverify, attempt });
      if (review.verified === true) verified = true;
      else lastFeedback = review.remediation || review.summary || 'Fixes did not fully verify.';
    }

    if (!verified) {
      const esc = await ctx.breakpoint({
        question: `Fixes did not fully verify after ${maxAttempts} attempts. Latest: ${review ? review.summary : 'unknown'}. Proceed to commit review anyway, or revert?`,
        title: 'Fix verification did not converge',
        expert: 'owner', tags: ['fix', 'verification-failed'],
        options: ['Revert fixes', 'Proceed to commit review anyway'],
        context: { runId: ctx.runId, files: [{ path: `artifacts/fix-report.md`, format: 'markdown' }] },
      });
      if (!esc.approved || /revert/i.test(esc.response || '')) {
        await ctx.task(revertChangesTask, {});
        return {
          success: false, totalFindings: report.totalFindings, fixed: 0,
          deferred: report.totalFindings, committed: false, commented: false, reverted: true,
          metadata: { processId: 'alma-digital-upload/sensitive-data-leak-audit', issue, timestamp: ctx.now() },
        };
      }
    }

    // Owner approval before committing fixes to main.
    if ((fixResult.filesModified || []).length > 0) {
      const approve = await ctx.breakpoint({
        question: `Applied ${fixResult.fixedCount} fix(es) across ${(fixResult.filesModified || []).length} file(s); tests ${review && review.testsPass ? 'PASS' : 'see report'}. Review the diff and approve commit to main?`,
        title: 'Approve commit to main',
        expert: 'owner', tags: ['approval-gate', 'commit'],
        options: ['Approve commit to main', 'Skip commit (leave changes uncommitted)'],
        context: {
          runId: ctx.runId,
          files: [
            { path: `artifacts/fix-report.md`, format: 'markdown' },
            { path: `artifacts/fixes.diff`, format: 'code', language: 'diff' },
          ],
        },
      });
      if (approve.approved && !/skip/i.test(approve.response || '')) {
        const c = await ctx.task(commitTask, { fixResult, issue });
        committed = c.committed === true;
      }
    }
  }

  // ==========================================================================
  // PHASE 5: SUMMARY COMMENT on the issue (acceptance criterion). Outward-facing
  // → gated behind owner approval.
  // ==========================================================================
  let commented = false;
  const commentApprove = await ctx.breakpoint({
    question: `Post the required summary comment to issue #${issue}? (findings: ${report.totalFindings}, fixed: ${fixResult.fixedCount || 0}, deferred: ${(report.totalFindings - (fixResult.fixedCount || 0))})`,
    title: `Post summary comment to issue #${issue}`,
    expert: 'owner', tags: ['github', 'issue-comment'],
    options: ['Post the summary comment', 'Skip — I will comment myself'],
    context: { runId: ctx.runId, files: [{ path: `artifacts/issue-summary.md`, format: 'markdown' }] },
  });
  if (commentApprove.approved && !/skip/i.test(commentApprove.response || '')) {
    const r = await ctx.task(postIssueCommentTask, { issue });
    commented = r.commented === true;
  }

  return {
    success: true,
    totalFindings: report.totalFindings,
    fixed: fixResult.fixedCount || 0,
    deferred: report.totalFindings - (fixResult.fixedCount || 0),
    committed,
    commented,
    metadata: { processId: 'alma-digital-upload/sensitive-data-leak-audit', issue, timestamp: ctx.now() },
  };
}

// ============================================================================
// AUDIT TASKS (read-only)
// ============================================================================

export const auditOutputPathsTask = defineTask('audit-output-paths', (args, taskCtx) => ({
  kind: 'agent',
  title: 'Audit logging/print/output calls + __repr__/__str__',
  description: 'Issue #2 checklist sections 1, 2, 6 — terminal/log output call sites and string-representation leaks',
  agent: {
    name: 'general-purpose',
    prompt: {
      role: 'Application security auditor (data-leak focus)',
      task: 'Find every code path that emits to stdout/stderr/logs and judge whether it could carry a secret or PII.',
      context: { projectRoot: args.projectRoot, checklistSections: [1, 2, 6] },
      instructions: [
        'Scope: project Python only. EXCLUDE .a5c/, .venv/, site-packages.',
        'Find all output call sites: print, pprint, logging/logger.*, log.*, sys.stdout.write, sys.stderr.write, rich.print/console, traceback.print_exc/format_exc, warnings.warn, and any custom logger wrappers.',
        'For each, classify risk: DIRECT (f-string/%/.format on names like key/token/secret/password/auth/credential/api_key) or INDIRECT (logging a whole response/request/HTTP-exception/config dict/locals()/vars(self)/headers dict; logging str(e) from requests/urllib/httpx where a credential-bearing URL may appear).',
        'Also inspect every custom class __repr__/__str__ for credential or PII fields that would leak when logged.',
        'STRICT SECRET HANDLING: report file:line and the structural pattern only. NEVER print actual secret values, .env contents, or expand credentials.',
        'Do NOT modify any files. This is audit-only.',
      ],
      outputFormat: 'JSON: { findings: [{ file, line, category, what, whySensitive, severity, proposedFix }], scanned: { callSites: number }, notes: string }',
    },
    outputSchema: { type: 'object', required: ['findings'], properties: { findings: { type: 'array', items: { type: 'object' } }, notes: { type: 'string' } } },
  },
  io: { inputJsonPath: `tasks/${taskCtx.effectId}/input.json`, outputJsonPath: `tasks/${taskCtx.effectId}/output.json` },
  labels: ['audit', 'output-paths'],
}));

export const auditFileWritesTask = defineTask('audit-file-writes', (args, taskCtx) => ({
  kind: 'agent',
  title: 'Audit file writes and output artifacts',
  description: 'Issue #2 checklist section 3 — files written during operation that could echo secrets/PII',
  agent: {
    name: 'general-purpose',
    prompt: {
      role: 'Application security auditor (data-leak focus)',
      task: 'Find every file-write path and judge whether the written data could contain secrets or PII.',
      context: { projectRoot: args.projectRoot, checklistSections: [3] },
      instructions: [
        'Scope: project Python only. EXCLUDE .a5c/, .venv/, site-packages.',
        'Find file writes: open(...).write, Path(...).write_text/write_bytes, to_csv/to_json/to_excel, json.dump/dumps, yaml.dump, csv.writer, pickle.dump, logging FileHandler targets, and any report/export/dump/debug-capture writers.',
        'For each, identify WHAT is written and trace whether the source data could include credentials, Authorization headers, credential-bearing URLs, or PII (patron records, emails, IDs).',
        'Pay attention to output dirs/log files this tool creates during normal runs.',
        'STRICT SECRET HANDLING: report file:line + structure only; never print secret values.',
        'Do NOT modify any files. Audit-only.',
      ],
      outputFormat: 'JSON: { findings: [{ file, line, category, what, whySensitive, severity, proposedFix }], scanned: { writeSites: number }, notes: string }',
    },
    outputSchema: { type: 'object', required: ['findings'], properties: { findings: { type: 'array', items: { type: 'object' } }, notes: { type: 'string' } } },
  },
  io: { inputJsonPath: `tasks/${taskCtx.effectId}/input.json`, outputJsonPath: `tasks/${taskCtx.effectId}/output.json` },
  labels: ['audit', 'file-writes'],
}));

export const auditConfigLogHistoryTask = defineTask('audit-config-log-history', (args, taskCtx) => ({
  kind: 'agent',
  title: 'Audit config/fixtures, log configuration, and git history',
  description: 'Issue #2 checklist sections 4, 5, 7 — checked-in secrets, log config/level/filters, lightweight history scan',
  agent: {
    name: 'general-purpose',
    prompt: {
      role: 'Application security auditor (data-leak focus)',
      task: 'Audit configuration/fixtures for checked-in secrets, review logging configuration, and do a lightweight git-history credential scan.',
      context: { projectRoot: args.projectRoot, checklistSections: [4, 5, 7] },
      instructions: [
        'Section 4 — config & fixtures: check .env / .env.example / .env.local, config/, settings, *.yaml/*.toml/*.json, tests/fixtures, tests/data, docs/, README, examples/ for hardcoded or example credentials. STRICT: to decide if a value is a real secret, use shape/length heuristics and structural patterns — do NOT cat secret files or print values. For .env-style files, report only whether credential-shaped assignments exist (names + redacted), never the values.',
        'Section 5 — log configuration: find where logging is configured (level, handlers, formatters, filters), where logs go (file/stdout/both), and whether DEBUG could be active on auth-handling paths. Note that almaapitk 0.4.5 now redacts Authorization in its formatters; verify this repo does not re-expose it. Confirm any existing redaction/scrubbing actually covers the patterns this repo uses.',
        'Section 7 — git history (lightweight): run `git log --all -p -S api_key -- "*.py" | head -100` and the same for token/secret/password; note whether credentials ever appeared and were later removed (history rewrite is OUT OF SCOPE — just record). Report redacted/structural notes only.',
        'STRICT SECRET HANDLING applies throughout: never emit secret values to output.',
        'Do NOT modify any files. Audit-only.',
      ],
      outputFormat: 'JSON: { findings: [{ file, line, category, what, whySensitive, severity, proposedFix }], logConfig: { level, destinations, filters, redactionHolds: boolean }, historyNotes: [string], notes: string }',
    },
    outputSchema: { type: 'object', required: ['findings'], properties: { findings: { type: 'array', items: { type: 'object' } }, notes: { type: 'string' } } },
  },
  io: { inputJsonPath: `tasks/${taskCtx.effectId}/input.json`, outputJsonPath: `tasks/${taskCtx.effectId}/output.json` },
  labels: ['audit', 'config', 'log-config', 'history'],
}));

// ============================================================================
// CONSOLIDATION
// ============================================================================

export const consolidateFindingsTask = defineTask('consolidate-findings', (args, taskCtx) => ({
  kind: 'agent',
  title: 'Consolidate findings into a report',
  description: 'Merge the three audit outputs into a single deduplicated, severity-ranked findings report',
  agent: {
    name: 'general-purpose',
    prompt: {
      role: 'Security report editor',
      task: 'Merge and deduplicate all audit findings into one report with per-finding path, line, category, why-sensitive, severity, and a concrete proposed fix.',
      context: { outputPaths: args.outputPaths, fileWrites: args.fileWrites, configLogHistory: args.configLogHistory, issue: args.issue },
      instructions: [
        'Deduplicate findings that point at the same file:line.',
        'Assign severity per the issue: actual-leak > possible-leak > theoretical.',
        'For each finding give one proposed fix from: remove, redact, log-safe-fields-only, change-log-level, other.',
        'Write artifacts/findings-report.md: a header with counts by severity, then a table/list of findings, then a short "patterns worth remembering" section.',
        'If there are ZERO findings, still write the report stating the repo is clean and which checklist sections were covered.',
        'Return the structured totals and the full findings list.',
      ],
      outputFormat: 'JSON: { totalFindings: number, severityCounts: object, findings: [{ id, file, line, category, what, whySensitive, severity, proposedFix }], patterns: [string] }',
    },
    outputSchema: { type: 'object', required: ['totalFindings', 'findings'], properties: { totalFindings: { type: 'number' }, findings: { type: 'array', items: { type: 'object' } } } },
  },
  io: { inputJsonPath: `tasks/${taskCtx.effectId}/input.json`, outputJsonPath: `tasks/${taskCtx.effectId}/output.json` },
  labels: ['audit', 'report'],
}));

// ============================================================================
// FIX / VERIFY / COMMIT / COMMENT
// ============================================================================

export const applyFixesTask = defineTask('apply-fixes', (args, taskCtx) => ({
  kind: 'agent',
  title: `Apply approved fixes (attempt ${args.attempt})`,
  description: 'Apply ONLY the fixes the owner approved; leave deferred findings untouched',
  agent: {
    name: 'general-purpose',
    prompt: {
      role: 'Security remediation engineer',
      task: 'Apply only the owner-approved leak fixes, minimally and surgically.',
      context: { report: args.report, decision: args.decision, feedback: args.feedback, attempt: args.attempt },
      instructions: [
        'Apply ONLY the fixes the owner approved (see decision). If "apply selected", apply exactly those the owner named and leave the rest deferred.',
        'Prefer the proposed fix per finding: remove debug print, redact value (last-4 or ***), log safe fields only (e.g. len not body), or guard DEBUG.',
        'Make minimal changes; do not refactor logging broadly, add features, or touch deferred findings.',
        'STRICT SECRET HANDLING: never write secret values anywhere; redaction must not echo the secret.',
        'Write artifacts/fix-report.md listing each applied fix (file:line, before-shape, after-shape, finding id) and each deferred finding with reason.',
        'Return the list of files modified and counts.',
      ],
      outputFormat: 'JSON: { filesModified: [string], fixedCount: number, deferredCount: number, applied: [{ id, file, line, fix }], deferred: [{ id, reason }] }',
    },
    outputSchema: { type: 'object', required: ['filesModified', 'fixedCount'], properties: { filesModified: { type: 'array', items: { type: 'string' } }, fixedCount: { type: 'number' } } },
  },
  io: { inputJsonPath: `tasks/${taskCtx.effectId}/input.json`, outputJsonPath: `tasks/${taskCtx.effectId}/output.json` },
  labels: ['fix', 'remediation'],
}));

export const runTestsTask = defineTask('run-tests', (args, taskCtx) => ({
  kind: 'shell',
  title: `Run test suite (attempt ${args.attempt})`,
  description: 'Run pytest to confirm fixes did not break anything',
  shell: { command: 'poetry run pytest -q' },
  io: { inputJsonPath: `tasks/${taskCtx.effectId}/input.json`, outputJsonPath: `tasks/${taskCtx.effectId}/output.json` },
  labels: ['verification', 'tests'],
}));

export const reverifyTask = defineTask('reverify', (args, taskCtx) => ({
  kind: 'agent',
  title: `Re-verify fixed leak paths (attempt ${args.attempt})`,
  description: 'Confirm each applied fix actually removes the leak and introduces no new one',
  agent: {
    name: 'general-purpose',
    prompt: {
      role: 'Security remediation verifier',
      task: 'Re-inspect the modified files and confirm each approved finding is resolved and no new leak was introduced.',
      context: { fixResult: args.fixResult, report: args.report, attempt: args.attempt },
      instructions: [
        'For each applied fix, re-read the file:line and confirm the secret/PII is no longer emitted (removed, redacted, or replaced by safe fields).',
        'Confirm the edits did not introduce a new output/print of sensitive data.',
        'STRICT SECRET HANDLING: report structure only, never values.',
        'Return per-fix resolved booleans and an overall pass.',
      ],
      outputFormat: 'JSON: { allResolved: boolean, results: [{ id, resolved: boolean, note: string }], newIssues: [string] }',
    },
    outputSchema: { type: 'object', required: ['allResolved'], properties: { allResolved: { type: 'boolean' }, results: { type: 'array', items: { type: 'object' } } } },
  },
  io: { inputJsonPath: `tasks/${taskCtx.effectId}/input.json`, outputJsonPath: `tasks/${taskCtx.effectId}/output.json` },
  labels: ['verification', 'reverify'],
}));

export const fixReviewTask = defineTask('fix-review', (args, taskCtx) => ({
  kind: 'agent',
  title: `Fix verification verdict (attempt ${args.attempt})`,
  description: 'Aggregate tests + re-verification into a verified verdict',
  agent: {
    name: 'general-purpose',
    prompt: {
      role: 'Release-readiness reviewer',
      task: 'Decide whether the applied fixes are verified and safe to commit.',
      context: { fixResult: args.fixResult, tests: args.tests, reverify: args.reverify, attempt: args.attempt },
      instructions: [
        'verified = true ONLY IF tests passed AND reverify.allResolved is true AND no new issues were introduced.',
        'Set testsPass boolean. If not verified, put actionable steps in remediation.',
        'Keep summary to one or two sentences.',
      ],
      outputFormat: 'JSON: { verified: boolean, testsPass: boolean, summary: string, remediation: string }',
    },
    outputSchema: { type: 'object', required: ['verified', 'summary'], properties: { verified: { type: 'boolean' }, testsPass: { type: 'boolean' }, summary: { type: 'string' } } },
  },
  io: { inputJsonPath: `tasks/${taskCtx.effectId}/input.json`, outputJsonPath: `tasks/${taskCtx.effectId}/output.json` },
  labels: ['verification', 'review'],
}));

export const revertChangesTask = defineTask('revert-changes', (args, taskCtx) => ({
  kind: 'shell',
  title: 'Revert fix changes',
  description: 'Restore tracked files to their committed state',
  shell: { command: 'git checkout -- .' },
  io: { inputJsonPath: `tasks/${taskCtx.effectId}/input.json`, outputJsonPath: `tasks/${taskCtx.effectId}/output.json` },
  labels: ['rollback'],
}));

export const commitTask = defineTask('commit', (args, taskCtx) => ({
  kind: 'shell',
  title: 'Commit approved leak fixes to main',
  description: 'Stage the modified tracked files and commit to main (only after owner approval)',
  shell: { command: '# Orchestrator stages the fixed tracked files and commits to main with an issue-referencing message' },
  io: { inputJsonPath: `tasks/${taskCtx.effectId}/input.json`, outputJsonPath: `tasks/${taskCtx.effectId}/output.json` },
  labels: ['git', 'commit'],
}));

export const postIssueCommentTask = defineTask('post-issue-comment', (args, taskCtx) => ({
  kind: 'shell',
  title: `Post summary comment to issue #${args.issue}`,
  description: 'Post the required acceptance-criteria summary comment via gh (only after owner approval)',
  shell: { command: '# Orchestrator posts artifacts/issue-summary.md to the issue via: gh issue comment <issue> --body-file <file>' },
  io: { inputJsonPath: `tasks/${taskCtx.effectId}/input.json`, outputJsonPath: `tasks/${taskCtx.effectId}/output.json` },
  labels: ['github', 'issue-comment'],
}));
