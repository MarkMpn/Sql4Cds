---
name: intake-policy-review
description: Review a Power Platform ToolBox (PPTB) tool or repository against the current PPTB marketplace and AI-assisted-development policies. Use when the user provides a PPTB GitHub repository, tool folder, README, or asks whether a PPTB tool is ready for marketplace intake or policy compliance.
---

# PPTB Marketplace Policy Intake Review

Perform an evidence-based marketplace-readiness review of a Power Platform ToolBox tool. Do not review the README in isolation when source code is available.

## Inputs

Typical inputs are:

- a GitHub repository URL;
- a GitHub path to a tool README or subfolder;
- a local/project repository;
- an already-open PPTB tool codebase.

If the URL points to a tool inside a monorepo, treat that tool folder as the review scope while checking shared code when it materially affects the tool.

## Required sources

1. **Current policies** — always verify the latest live policy pages before making compliance claims:
   - `https://docs.powerplatformtoolbox.com/policies/marketplace`
   - `https://docs.powerplatformtoolbox.com/policies/ai-assisted-development`
   - use current manifest/maturity/API docs when relevant.
2. **Repository evidence** — inspect at minimum:
   - `README.md`;
   - `package.json` or manifest-equivalent metadata;
   - primary source files;
   - destructive/write paths, if any;
   - tests/E2E scripts, if present;
   - icon and screenshots, when relevant;
   - privacy/network/CSP behavior;
   - dependencies and scripts.

Prefer the GitHub connector for GitHub repositories when available. Use web search for current public policy/documentation pages.

## Review workflow

### 1. Establish the tool's behavior

Determine:

- read-only vs state-changing;
- which environment(s) can be targeted;
- which ToolBox APIs and Dataverse/Power Platform APIs are used;
- local file writes/exports;
- any external network calls;
- potentially sensitive data handled;
- whether screenshots are real tool captures or mocked/synthetic.

Do not infer safety from the README alone. Verify against code.

### 2. Check marketplace metadata

Review:

- `displayName` must describe the tool and must not contain publisher/company branding or prefixes;
- publisher attribution belongs in author/contributors/README/npm scope;
- package/manifest naming and required feature flags;
- `connectionRequirement`, `multiConnection`, `minAPI`, and other manifest claims should match actual behavior;
- icon should be functional/tool-oriented rather than a publisher/company logo;
- README claims must match current implementation.

### 3. Check state-changing/destructive behavior

If the tool changes Dataverse, Power Platform, solution metadata, filesystem state beyond explicit exports, or other user data/configuration:

- no destructive writes on load;
- require explicit user action;
- preview the target environment and scope before write;
- use meaningful confirmation;
- provide backup/export/snapshot/undo where feasible;
- where rollback is not feasible, state that plainly in the confirmation;
- README must contain an exact `## What this tool changes` section documenting every class of change;
- identify production safeguards;
- verify environment/connection locking between preview and apply;
- verify fresh validation immediately before destructive writes where relevant;
- verify operations are bounded, cancellable where possible, and failures are surfaced.

If the tool is genuinely read-only, say so explicitly and do not invent destructive requirements such as backup/restore.

### 4. Check AI-assisted-development policy

Where applicable, review:

- real-environment human testing, not only mocked/unit/build validation;
- API calls against current ToolBox/API documentation;
- bounded loops, pagination, batching and concurrency;
- unsafe patterns such as `eval`, untrusted HTML injection, unsafe shell/process execution, uncontrolled dynamic code, or unvalidated interpolation;
- dependency necessity and security;
- CSP/network exceptions and external services;
- screenshot authenticity;
- AI Assistance disclosure if a substantial portion of the tool was generated with AI;
- human accountability for destructive code paths.

Do not assume an API is valid simply because its name looks plausible. Confirm it when material.

### 5. Check privacy and data handling

Review:

- what data leaves ToolBox/Dataverse;
- whether any publisher-controlled endpoint receives tenant/user/environment data;
- debug logs and exports for potentially sensitive information;
- README privacy statements against actual implementation;
- external telemetry/analytics if present.

### 6. Check verification/maturity readiness

When relevant, recommend:

- `npm audit` and resolution of Critical/High findings;
- `npm run validate` / `pptb-validate`;
- light/dark-theme testing;
- real-environment acceptance testing;
- final packaged/dist verification rather than source-only verification.

Do not claim these commands have passed unless you actually ran them or repository evidence proves it.

## Severity model

Use these levels consistently:

- `❌ Blocker` — likely prevents marketplace acceptance or creates a material security/safety issue.
- `⚠️ Needs attention` — should be fixed or verified before submission, but is not clearly an automatic rejection by itself.
- `✅ Pass` — inspected evidence aligns with policy/expected practice.
- `✅ / verify` — design appears correct but requires real-environment or current-documentation confirmation.

Do not label a recommendation as a blocker unless supported by current policy or a material security/safety concern.

## Required output structure

Start with a concise overall assessment, then provide a table with columns:

| Area | Status | Finding |

Cover at least:

- Marketplace name
- Publisher attribution
- Icon
- Distinct functionality
- Read/write behavior
- Preview/confirmation/rollback if applicable
- `What this tool changes` if applicable
- API usage
- Bounded operations/paging/concurrency
- Privacy/network/CSP
- Screenshots
- Real-environment testing
- AI disclosure if applicable
- Dependencies/audit
- Validation/theme testing

Then explain the material findings in numbered sections. Include exact replacement snippets when a concrete change is obvious (for example `displayName`, manifest flags, README sections, or warning text).

End with:

1. a prioritized **Recommended pre-submission changes** list;
2. a concise **Final verdict** using one of:
   - `🟢 Marketplace-ready`;
   - `🟡 Very close / minor changes`;
   - `🟠 Not ready yet / material changes required`;
   - `🔴 Significant compliance or safety concerns`.

State the specific blockers immediately below the verdict.

## Review principles

- Separate policy requirements from good-practice recommendations.
- Distinguish code evidence from README claims.
- Do not penalize a read-only tool for lacking destructive-operation controls.
- Prefer precise, actionable remediation over generic advice.
- Highlight unusually good safety patterns as well as defects.
- If screenshots are described as synthetic/mock data, treat that as a publication issue only when current policy requires actual tool captures; do not claim synthetic test data itself is forbidden if the capture is from the real running tool.
- If AI assistance is unknown, phrase disclosure as conditional: `required if substantially AI-assisted`.
- When repo evidence says a feature is `UNVERIFIED against a live environment`, treat it as a material pre-submission issue for consequential behavior.
