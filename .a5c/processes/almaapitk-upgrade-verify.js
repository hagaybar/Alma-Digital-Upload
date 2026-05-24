/**
 * @process alma-digital-upload/almaapitk-upgrade-verify
 * @description Phased, verification-gated upgrade of the almaapitk dependency pin
 *   (currently >=0.3.1) to >=0.4.5 on the main branch, confirming the repo's used
 *   API surface still aligns under the new minor (0.3.x -> 0.4.x) so nothing breaks,
 *   and that the credential-redaction security fix is active in this consumer's
 *   logging path. Offline verification only (no live Alma API calls). Commits only
 *   after explicit owner approval at a diff-review breakpoint.
 * @inputs { oldPin: string, newPin: string, targetVersion: string, maxAttempts: number, issue: number }
 * @outputs { success: boolean, lockedVersion: string, aligned: boolean, committed: boolean }
 * @skill alma-api-expert
 */

import { defineTask } from '@a5c-ai/babysitter-sdk';

export async function process(inputs, ctx) {
  const {
    oldPin = 'almaapitk = ">=0.3.1"',
    newPin = 'almaapitk = ">=0.4.5"',
    targetVersion = '0.4.5',
    maxAttempts = 3,
    issue = 1,
  } = inputs;

  // ==========================================================================
  // PHASE 1: PRE-UPGRADE USAGE INVENTORY
  // Catalogue exactly what the repo uses from almaapitk so the post-upgrade
  // verification has a concrete contract to check against.
  // ==========================================================================
  const inventory = await ctx.task(analyzeUsageTask, { targetVersion });

  // ==========================================================================
  // PHASE 2: APPLY THE UPGRADE (pin bump + re-resolve lock + install)
  // ==========================================================================
  const upgrade = await ctx.task(applyUpgradeTask, { oldPin, newPin, targetVersion });

  // ==========================================================================
  // PHASE 3: VERIFICATION CONVERGENCE LOOP
  // Run tests, confirm the used API surface still resolves under the new
  // version, and confirm the redaction fix is active — all offline. If a
  // misalignment is found, remediate the repo code and re-verify.
  // ==========================================================================
  let attempt = 0;
  let aligned = false;
  let lastFeedback = null;
  let review = null;
  const history = [];

  while (attempt < maxAttempts && !aligned) {
    attempt++;

    if (lastFeedback) {
      await ctx.task(remediateTask, {
        feedback: lastFeedback,
        attempt,
        inventory,
        targetVersion,
      });
    }

    const [tests, apiSurface, redaction] = await ctx.parallel.all([
      () => ctx.task(runTestsTask, { attempt }),
      () => ctx.task(verifyApiSurfaceTask, { inventory, targetVersion, attempt }),
      () => ctx.task(verifyRedactionTask, { attempt }),
    ]);

    review = await ctx.task(compatibilityReviewTask, {
      inventory,
      tests,
      apiSurface,
      redaction,
      targetVersion,
      attempt,
    });

    history.push({ attempt, tests, apiSurface, redaction, review });

    if (review.aligned === true) {
      aligned = true;
    } else {
      lastFeedback = review.remediation || review.summary || 'Misalignment detected; fix repo to align with new version.';
    }
  }

  // If still not aligned after maxAttempts, escalate to the owner rather than
  // committing a broken upgrade.
  if (!aligned) {
    const escalation = await ctx.breakpoint({
      question: `almaapitk ${targetVersion} could not be verified as compatible after ${maxAttempts} attempts. Latest verdict: ${review ? review.summary : 'unknown'}. How do you want to proceed?`,
      title: 'Upgrade verification did not converge',
      expert: 'owner',
      tags: ['upgrade', 'verification-failed'],
      options: ['Abandon upgrade (revert changes)', 'Override and continue to commit review anyway'],
      context: {
        runId: ctx.runId,
        files: [{ path: `artifacts/compatibility-report.md`, format: 'markdown' }],
      },
    });
    if (!escalation.approved || /abandon|revert/i.test(escalation.response || '')) {
      await ctx.task(revertChangesTask, {});
      return {
        success: false,
        lockedVersion: upgrade.lockedVersion || null,
        aligned: false,
        committed: false,
        reverted: true,
        attempts: attempt,
        metadata: { processId: 'alma-digital-upload/almaapitk-upgrade-verify', issue, timestamp: ctx.now() },
      };
    }
  }

  // ==========================================================================
  // PHASE 4: OWNER REVIEW BEFORE COMMIT (the user asked to review first)
  // ==========================================================================
  let committed = false;
  let lastReviewFeedback = null;
  for (let approveAttempt = 0; approveAttempt < maxAttempts; approveAttempt++) {
    const approval = await ctx.breakpoint({
      question: `almaapitk pin ${oldPin} -> ${newPin} (locked ${upgrade.lockedVersion || targetVersion}). Verification: tests ${review && review.testsPass ? 'PASS' : 'see report'}, API surface ${review && review.apiAligned ? 'aligned' : 'see report'}, redaction ${review && review.redactionActive ? 'active' : 'see report'}. Review the diff and approve commit to main?`,
      title: 'Approve commit to main',
      expert: 'owner',
      tags: ['approval-gate', 'commit'],
      previousFeedback: lastReviewFeedback || undefined,
      attempt: approveAttempt > 0 ? approveAttempt + 1 : undefined,
      options: ['Approve commit to main', 'Request changes', 'Skip commit (leave changes uncommitted)'],
      context: {
        runId: ctx.runId,
        files: [
          { path: `artifacts/compatibility-report.md`, format: 'markdown' },
          { path: `artifacts/upgrade.diff`, format: 'code', language: 'diff' },
        ],
      },
    });

    if (approval.approved && !/skip/i.test(approval.response || '')) {
      const commitRes = await ctx.task(commitTask, { newPin, targetVersion, lockedVersion: upgrade.lockedVersion, issue });
      committed = commitRes.committed === true;
      break;
    }
    if (/skip/i.test(approval.response || '')) {
      break; // leave changes in working tree, do not commit
    }
    lastReviewFeedback = approval.response || approval.feedback || 'Changes requested';
    // loop back: remediate then re-review
    await ctx.task(remediateTask, { feedback: lastReviewFeedback, attempt: approveAttempt + 1, inventory, targetVersion });
  }

  return {
    success: aligned,
    lockedVersion: upgrade.lockedVersion || targetVersion,
    aligned,
    committed,
    attempts: attempt,
    metadata: {
      processId: 'alma-digital-upload/almaapitk-upgrade-verify',
      issue,
      timestamp: ctx.now(),
    },
  };
}

// ============================================================================
// TASK DEFINITIONS
// ============================================================================

export const analyzeUsageTask = defineTask('analyze-usage', (args, taskCtx) => ({
  kind: 'agent',
  title: 'Inventory almaapitk usage in the repo',
  description: 'Catalogue every almaapitk symbol, attribute, and call signature the repo depends on',
  agent: {
    name: 'general-purpose',
    prompt: {
      role: 'Python dependency-compatibility analyst',
      task: 'Produce a precise inventory of everything this repository uses from the almaapitk package, so a later step can verify the new version still provides it.',
      context: { targetVersion: args.targetVersion },
      instructions: [
        'Search the repo (exclude .a5c/, .venv/, site-packages) for every import from almaapitk and every use of its symbols.',
        'For each used symbol (classes like AlmaAPIClient, Admin, BibliographicRecords; methods like test_connection; constructor arguments), record: the symbol path, how it is called (positional/keyword args), and the file:line.',
        'Identify any use of almaapitk logging/formatter internals (TextFormatter, JSONFormatter, redact_sensitive_data, alma_logging) if present.',
        'Do NOT read or print secrets, .env files, or shell rc files. Only read project Python source.',
        'Return the structured inventory; do not modify any files.',
      ],
      outputFormat: 'JSON: { symbols: [{ symbol, kind, calledAs, locations:[string] }], loggingUsage: [string], notes: string }',
    },
    outputSchema: {
      type: 'object',
      required: ['symbols'],
      properties: {
        symbols: { type: 'array', items: { type: 'object' } },
        loggingUsage: { type: 'array', items: { type: 'string' } },
        notes: { type: 'string' },
      },
    },
  },
  io: {
    inputJsonPath: `tasks/${taskCtx.effectId}/input.json`,
    outputJsonPath: `tasks/${taskCtx.effectId}/output.json`,
  },
  labels: ['analysis', 'inventory'],
}));

export const applyUpgradeTask = defineTask('apply-upgrade', (args, taskCtx) => ({
  kind: 'shell',
  title: `Bump pin to ${args.newPin} and re-resolve lock`,
  description: 'Edit the pyproject pin, re-resolve poetry.lock, and install the new version',
  shell: {
    command: `# Orchestrator performs: edit pyproject pin (${args.oldPin} -> ${args.newPin}), then 'poetry lock' and 'poetry install', then capture locked version`,
  },
  io: {
    inputJsonPath: `tasks/${taskCtx.effectId}/input.json`,
    outputJsonPath: `tasks/${taskCtx.effectId}/output.json`,
  },
  labels: ['upgrade', 'dependency'],
}));

export const runTestsTask = defineTask('run-tests', (args, taskCtx) => ({
  kind: 'shell',
  title: `Run test suite (attempt ${args.attempt})`,
  description: 'Run the pytest suite under the upgraded environment',
  shell: { command: 'poetry run pytest -q' },
  io: {
    inputJsonPath: `tasks/${taskCtx.effectId}/input.json`,
    outputJsonPath: `tasks/${taskCtx.effectId}/output.json`,
  },
  labels: ['verification', 'tests'],
}));

export const verifyApiSurfaceTask = defineTask('verify-api-surface', (args, taskCtx) => ({
  kind: 'agent',
  title: `Verify used API surface resolves under ${args.targetVersion}`,
  description: 'Introspect the installed almaapitk and confirm every inventoried symbol/signature still exists and is compatible',
  agent: {
    name: 'general-purpose',
    prompt: {
      role: 'Python compatibility verifier',
      task: 'Confirm that every symbol and call signature in the usage inventory still exists and is compatible in the installed almaapitk version.',
      context: { inventory: args.inventory, targetVersion: args.targetVersion },
      instructions: [
        'Use `poetry run python` to import almaapitk and introspect the installed package (inspect.signature, getattr, dir).',
        'For each symbol in the inventory, confirm it still exists with a compatible signature (no removed/renamed symbols, no removed required-compatible parameters).',
        'Flag ANY mismatch: removed symbol, renamed symbol, changed required arguments, changed return contract relevant to repo usage.',
        'Do not make network/API calls. Introspection only.',
        'Return a per-symbol alignment table and an overall aligned boolean.',
      ],
      outputFormat: 'JSON: { aligned: boolean, results: [{ symbol, present: boolean, signatureMatch: boolean, note: string }], breaking: [string] }',
    },
    outputSchema: {
      type: 'object',
      required: ['aligned', 'results'],
      properties: {
        aligned: { type: 'boolean' },
        results: { type: 'array', items: { type: 'object' } },
        breaking: { type: 'array', items: { type: 'string' } },
      },
    },
  },
  io: {
    inputJsonPath: `tasks/${taskCtx.effectId}/input.json`,
    outputJsonPath: `tasks/${taskCtx.effectId}/output.json`,
  },
  labels: ['verification', 'api-surface'],
}));

export const verifyRedactionTask = defineTask('verify-redaction', (args, taskCtx) => ({
  kind: 'shell',
  title: `Offline redaction check (attempt ${args.attempt})`,
  description: 'Confirm the credential-redaction fix is active in the toolkit logging path, offline (no API calls, no real keys)',
  shell: {
    command: '# Orchestrator runs an offline python check exercising almaapitk TextFormatter at DEBUG with a FAKE Authorization header and asserts it is redacted',
  },
  io: {
    inputJsonPath: `tasks/${taskCtx.effectId}/input.json`,
    outputJsonPath: `tasks/${taskCtx.effectId}/output.json`,
  },
  labels: ['verification', 'security', 'redaction'],
}));

export const compatibilityReviewTask = defineTask('compatibility-review', (args, taskCtx) => ({
  kind: 'agent',
  title: `Compatibility verdict (attempt ${args.attempt})`,
  description: 'Aggregate tests, API-surface, and redaction results into an alignment verdict and a human-readable report',
  agent: {
    name: 'general-purpose',
    prompt: {
      role: 'Release-readiness reviewer',
      task: 'Decide whether the almaapitk upgrade is safe to commit, based on test results, API-surface verification, and the redaction check.',
      context: {
        inventory: args.inventory,
        tests: args.tests,
        apiSurface: args.apiSurface,
        redaction: args.redaction,
        targetVersion: args.targetVersion,
      },
      instructions: [
        'aligned = true ONLY IF: the test suite passed, the API surface verification reports aligned=true with no breaking entries, and the redaction check passed.',
        'Write a concise markdown report to artifacts/compatibility-report.md summarizing each check, the pin change, and the verdict.',
        'If aligned=false, populate `remediation` with specific, actionable steps to fix the repo so it aligns with the new version.',
        'Set testsPass, apiAligned, redactionActive booleans to reflect each individual check.',
      ],
      outputFormat: 'JSON: { aligned: boolean, testsPass: boolean, apiAligned: boolean, redactionActive: boolean, summary: string, remediation: string }',
    },
    outputSchema: {
      type: 'object',
      required: ['aligned', 'summary'],
      properties: {
        aligned: { type: 'boolean' },
        testsPass: { type: 'boolean' },
        apiAligned: { type: 'boolean' },
        redactionActive: { type: 'boolean' },
        summary: { type: 'string' },
        remediation: { type: 'string' },
      },
    },
  },
  io: {
    inputJsonPath: `tasks/${taskCtx.effectId}/input.json`,
    outputJsonPath: `tasks/${taskCtx.effectId}/output.json`,
  },
  labels: ['verification', 'review'],
}));

export const remediateTask = defineTask('remediate', (args, taskCtx) => ({
  kind: 'agent',
  title: `Remediate misalignment (attempt ${args.attempt})`,
  description: 'Apply minimal repo code changes to align with the new almaapitk version, per review feedback',
  agent: {
    name: 'general-purpose',
    prompt: {
      role: 'Python maintenance engineer',
      task: 'Make the minimal repo code changes needed to align with the new almaapitk version, following the provided feedback.',
      context: { feedback: args.feedback, inventory: args.inventory, targetVersion: args.targetVersion },
      instructions: [
        'Apply only the minimal changes required to resolve the reported misalignment.',
        'Do not change unrelated behavior. Do not touch secrets, .env, or credentials.',
        'Report exactly which files/lines changed and why.',
      ],
      outputFormat: 'JSON: { filesModified: [string], summary: string }',
    },
    outputSchema: {
      type: 'object',
      required: ['filesModified', 'summary'],
      properties: {
        filesModified: { type: 'array', items: { type: 'string' } },
        summary: { type: 'string' },
      },
    },
  },
  io: {
    inputJsonPath: `tasks/${taskCtx.effectId}/input.json`,
    outputJsonPath: `tasks/${taskCtx.effectId}/output.json`,
  },
  labels: ['remediation'],
}));

export const revertChangesTask = defineTask('revert-changes', (args, taskCtx) => ({
  kind: 'shell',
  title: 'Revert upgrade changes',
  description: 'Restore pyproject.toml and poetry.lock to their committed state',
  shell: { command: 'git checkout -- pyproject.toml poetry.lock' },
  io: {
    inputJsonPath: `tasks/${taskCtx.effectId}/input.json`,
    outputJsonPath: `tasks/${taskCtx.effectId}/output.json`,
  },
  labels: ['rollback'],
}));

export const commitTask = defineTask('commit', (args, taskCtx) => ({
  kind: 'shell',
  title: 'Commit the verified upgrade to main',
  description: 'Stage pyproject.toml + poetry.lock and commit to main (only after owner approval)',
  shell: {
    command: '# Orchestrator stages pyproject.toml and poetry.lock and commits to main with an issue-referencing message',
  },
  io: {
    inputJsonPath: `tasks/${taskCtx.effectId}/input.json`,
    outputJsonPath: `tasks/${taskCtx.effectId}/output.json`,
  },
  labels: ['git', 'commit'],
}));
