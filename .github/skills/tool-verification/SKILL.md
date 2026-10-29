---
name: tool-verification
description: Check a Power Platform ToolBox (PPTB) tool repository URL for Verified badge readiness against every requirement in the current Tool Maturity Model. Use for verification reviews, Get Verified preparation, re-verification, or checking whether a tool meets the Verified criteria; report evidence, blockers, reviewer exceptions, and checks that need marketplace data or live testing.
---

# PPTB Tool Verification Review

Assess an existing tool against the official Verified criteria. A repository URL is enough to start: retrieve the source and public evidence, complete every accessible check, and identify exactly what remains unproven. This assessment cannot grant a badge or substitute for PPTB's human decision.

## 1. Establish scope and current requirements

- Open the live [Tool Maturity Model](https://docs.powerplatformtoolbox.com/tool-development/maturity-model) on every review. Record the review date and policy URL. Read [references/checklist.md](references/checklist.md) for the criterion-by-criterion evidence workflow; reconcile it with the live page, adding or changing rows if policy has changed. If the live policy is inaccessible, label the review provisional and identify the cached baseline date.
- Accept repository, branch/tag, commit, or tool-subfolder URLs. Resolve the exact repository, revision, and tool package. For a monorepo, inspect shared code and the workspace lockfile where they affect that tool. If multiple tool packages are equally plausible, inventory them and ask which to review.
- Use available repository connectors, host APIs, web access, or an isolated clone. Read actual files and history; a README-only review is incomplete. Record the commit SHA and distinguish the default branch from the published version proposed for verification. Match marketplace identity, npm package, release/tag, and distribution where accessible; report mismatches rather than assuming they describe the same artifact.
- Inspect README and linked images, manifests, lockfiles, source, styles, build/packaging scripts, CI/test evidence, releases, contributor activity, and the complete set of open bugs with their comment histories. Follow external issue trackers linked by the project. Record inaccessible resources and pagination limits.

## 2. Collect and check evidence

Work through **every required and optional row** in the reference, plus the submission prerequisites. Continue independent checks when access or runtime limitations prevent others.

- Cite file paths with line numbers or commit-specific links, command results, issue/comment timestamps, release artifacts, and marketplace evidence. README assertions are claims until corroborated by implementation or observed behavior.
- Check the intended released artifact as well as source. An ignored `dist` directory is not a failure by itself: inspect the published archive or build in an isolated checkout. Source containing an icon or theme code does not prove the shipped tool includes or uses it.
- Run checks when the environment permits. Inspect scripts and configuration before executing repository code; use an isolated workspace and avoid changing the user's working tree. Follow its package manager and lockfile. Do not run destructive tool actions against a tenant to establish verification readiness.
- Read the live [validation documentation](https://docs.powerplatformtoolbox.com/tool-development/validation). `pptb-validate` comes from `@pptb/types`, not an assumed standalone npm package. When installed, run `npx pptb-validate --json` against the tool manifest (and distribution manifest if different). Use `--skip-url-checks` only when necessary, and record that URL reachability remains unchecked. Record validator version, command, working directory, exit status, errors, and warnings. Do not install dependencies into or rewrite the reviewed repository just to add a validator; use the isolated review workspace if tooling is missing.
- Run `npm audit --json` against the relevant dependency tree. Do not suppress development dependencies to obtain a clean result. Record package/lockfile identity, audit time, vulnerability severities and dependency paths. Distinguish direct dependencies from transitive (child) dependencies, separately recording runtime versus development/build scope and the direct parent that introduces each affected child; use the manifest and actual dependency tree as evidence. See R3 in [references/checklist.md](references/checklist.md) for reporting and remediation guidance. Distinguish vulnerability findings from registry/network failures. For non-npm projects, preserve their lockfile and use supported audit tooling, explaining any gap in the required npm evidence. For high/critical findings confined to build/development dependencies, use **Needs Manual Validation** (❔) pending human review of scope, advisory applicability and remediation. Confirmed runtime high/critical findings remain **Fail** (❌), including mixed runtime/build cases; identify build findings as pending manual validation within that response. Never infer build-only scope solely from a devDependencies declaration. Do not auto-fix or upgrade packages as part of a review.
- For runtime checks, use the reviewed build inside PPTB, recording host version and test conditions. Static inspection and browser mocks can support findings but cannot prove actual host integration. If runtime access is unavailable, give a precise test procedure and leave the relevant result unverified.
- Marketplace usage, ratings, ownership, and queue state may need owner-provided evidence. Use public data where available, then request only the missing values or dated dashboard evidence. Never infer them from GitHub stars, npm download estimates, README badges, or the absence of public data.

## 3. Report a complete assessment

Start the report with a **Quick summary**: a concise verdict followed immediately by a simple bullet list of outstanding issues to resolve. Keep each bullet to one line, state a concrete next action, and combine duplicate findings. Put confirmed required failures first, then missing required evidence and any reviewer decisions or waivers needed. Make the distinction clear in the wording (for example, "Publish version 1.0.0 or later", "Provide live PPTB theme-test evidence", or "Meet two usage thresholds or obtain a reviewer waiver"). Keep optional improvements in the detailed report and do not present them as approval blockers. If nothing remains outstanding, say "No outstanding verification issues found" instead of inventing bullets.

After the quick summary, provide the reviewed URL, tool/package, version, commit SHA, artifact identity, policy date, and material access limitations, then the detailed assessment below. Use one of these **assessment** verdicts:

- **Ready to request verification** — submission prerequisites and all required checks have sufficient passing evidence.
- **Needs reviewer judgment** — no confirmed hard failure or missing required evidence, but a documented soft gate or waiver remains for PPTB to decide.
- **Not ready** — at least one confirmed required failure or submission prerequisite failure; also list evidence gaps.
- **Incomplete assessment** — required evidence is missing and no confirmed failure has already established that the tool is not ready.

Include a table with one row per current criterion and prerequisite:

| ID / criterion | Required / optional / prerequisite | Status | Evidence | Finding and next action |
| --- | --- | --- | --- | --- |

Use **Pass**, **Fail**, **Needs evidence**, **Needs Manual Validation**, or **Reviewer judgment**. Build-only high/critical findings use **Needs Manual Validation** and remain unresolved until a human decision is recorded; they cannot support a ready verdict. Use **Not applicable** only where justified (for example, no CSP exceptions after inspecting all applicable manifests and network behavior). Never use it for unavailable usage data, an inaccessible issue tracker, or an untested UI. Report each of the three usage metrics separately beneath the combined usage criterion. Optional failures do not change approval readiness.

After the table, provide prioritized required fixes, evidence still needed (with how to obtain it), reviewer decisions, and optional improvements. Preserve the API migration soft gate, bug-health flag thresholds, and possible usage waiver described in the live policy. Do not invent a recent-activity cutoff, minimum test coverage, dependency-age limit, or strict WCAG gate.

Include the applicable request and badge-maintenance guidance from the reference, then append the reviewer response described in Step 4 as the final report section. Reviewing a URL authorizes an assessment, not publishing changes, contacting maintainers, submitting a verification request, or changing marketplace state. If separately asked to publish, delete, or force-push, identify the irreversible action and obtain authorization before executing it; leave `allowed-tools` unset.

## 4. Draft the reviewer response

Read [references/reviewer-response.md](references/reviewer-response.md) and end every report with a **Reviewer response** section formatted for the verification web form: one list item per current required and optional criterion, including passing checks. Prefix each item with ✅ (pass or justified not applicable), ❌ (confirmed failure), or ❔ (missing evidence, Needs Manual Validation for build-only vulnerability findings, or reviewer judgment), then the criterion name and a self-contained explanation suitable for its individual response text box. Label optional items and explain any failure and remedy clearly; include passing evidence rather than writing only "Passed".

Include a short reviewer-only note before the list when prerequisites, checks or discretionary decisions remain unresolved. Do not convert missing evidence, optional findings, reviewer-tooling limitations, or owner-account access checks into failed tool requirements. Preserve soft gates and usage-waiver discretion; do not claim a waiver, approval, or badge change occurred without an explicit human decision or evidence. If required checks remain unperformed, make clear the draft needs reviewer completion before it can serve as the final outcome. Draft the response only; do not fill or submit the form or alter marketplace state.
