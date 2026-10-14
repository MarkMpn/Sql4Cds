# Verification evidence checklist

Baseline checked: **2026-10-08**. Authority: [PPTB Tool Maturity Model](https://docs.powerplatformtoolbox.com/tool-development/maturity-model). Refresh that page for each assessment; this reference supplies investigation guidance, not a competing policy.

## Submission prerequisites

Report these separately from the reviewer checklist:

- Identify the marketplace listing and the owner's ability to request verification. An unpublished candidate can receive a repository assessment, but its listing/ownership prerequisite remains unmet or unproven.
- Match the published, tested candidate to the assessed commit and archive. Record release differences. Development-branch fixes do not establish that a published version passes.
- Obtain a validator result for that candidate. Separate genuine validation errors from tooling failures and skipped checks. A validator pass alone does not establish Verified readiness: in particular, automated validation can accept a missing icon while verification requires one.
- Seek dated real-PPTB test evidence for the candidate; record who tested it, the host version, and the relevant results. Generic CI success does not prove live testing occurred.

Sources: [maturity model request process](https://docs.powerplatformtoolbox.com/tool-development/maturity-model#request-verification), [local validation](https://docs.powerplatformtoolbox.com/tool-development/validation), [publishing](https://docs.powerplatformtoolbox.com/tool-development/publishing).

## Required criteria

### R1. README onboarding

Inspect the rendered README and follow its asset links. Assess whether a new user can understand the purpose and complete installation and initial use. Check that screenshots/GIFs show the tool interface, resolve correctly on the repository host, and correspond to the candidate. Inspect linked instructions for missing steps; do not accept a heading with no usable content as evidence.

### R2. CSP justification

Enumerate all `cspExceptions` from applicable source and distribution manifests. For each directive/domain, map its explanation to actual code or resource usage; record unused entries and unexplained external access. No exceptions can be marked not applicable only after inspection.

Entries can be strings or objects; lack of an `exceptionReason` property alone does not prove lack of documentation. Check the README or linked docs too. Conversely, an explanation in the manifest does not prove the exception is needed. Inspect dynamic URLs, CDN resources, telemetry, and optional features as well as literal `fetch` calls. Report overly broad origins with the supporting CSP rule.

Sources: [manifest CSP format](https://docs.powerplatformtoolbox.com/tool-development/manifest#csp-exceptions-object), [CSP guidance](https://docs.powerplatformtoolbox.com/tool-development/csp-configuration).

### R3. Dependency vulnerabilities

Pass only when the audit establishes no high or critical vulnerabilities. Use the full audit evidence described in SKILL.md. List high/critical findings individually with affected package, installed version, advisory, dependency path, and a feasible remediation. Lower severities are additional findings. An empty or missing lockfile, old CI badge, failed audit request, or clean production-only audit cannot establish the full dependency tree is clean. Note differences between the candidate release's dependencies and the current branch.

Distinguish **direct** dependencies declared by the reviewed tool's manifest from **transitive (child)** dependencies brought in by another package. Cross-check audit `isDirect` with the relevant manifest, lockfile and `npm ls <package>` or `npm explain <package>`; do not equate a hoisted `node_modules/<package>` path with a direct declaration. In workspaces, classify relative to the reviewed tool and identify dependencies introduced by shared workspace tooling separately.

Report dependency relationship and scope as separate dimensions: a direct dependency can be development-only, and a transitive dependency can be shipped at runtime. Show multiple affected versions/paths and both runtime/build paths where relevant; label a package **Direct and transitive** if it has both relationships. Keep source and released-tree classifications separate when they differ.

Use a vulnerability table with these columns:

| Package / installed version | Severity / advisory | Relationship | Runtime / build scope | Direct introducer and dependency path | Remediation |
| --- | --- | --- | --- | --- | --- |

For a direct finding, identify the declared dependency to update. For a child finding, identify the direct dependency whose upgrade or refreshed resolution can bring in a patched child, retaining the complete path (for example, `vite → postcss → source-map-js`). Do not suggest adding a child as a new direct dependency as the default fix. Consider a tested override only where a compatible parent/resolution update is unavailable; do not apply it during an assessment.

Summarize affected direct and transitive packages separately, without double-counting shared children or multiple advisory records as additional packages. If an audit entry is high only because its `via` references another vulnerable package, distinguish that propagated finding from an advisory against the parent itself. For high/critical findings confined to build/development dependencies, report **Needs Manual Validation** (❔). Retain every finding and have the human reviewer assess actual scope, advisory applicability, build-process exposure and remediation. A devDependencies declaration or clean production-only audit alone does not prove build-only scope: check dependency paths and shipped artifacts. If scope is uncertain, use ❔ and request evidence. Confirmed runtime high/critical findings use **Fail** (❌); in mixed cases the criterion remains ❌ while build findings are identified as needing manual validation. This is the skill's reporting convention, not a claimed official exemption from the full-tree requirement. Do not mark the criterion passed or the tool ready while manual validation is pending; record the reviewer's decision and rationale.

### R4. PPTB API support

Inventory host API calls, including wrappers, aliases, computed access, and shared code. Compare each used method/signature with the current [API reference](https://docs.powerplatformtoolbox.com/tool-development/api-reference), relevant host release notes, and supported minimum host version. Do not rely on stale type declarations as the sole authority.

Distinguish removed/unsupported calls from deprecated-but-supported ones. For a migration exception, cite concrete work such as migration commits, a tracked plan, and current compatibility evidence; a bare promise is insufficient. Leave that exception to the human reviewer. For ambiguous docs/runtime drift, identify the conflicting sources and host version instead of declaring the method invented.

### R5. Host theme integration

Trace initial theme acquisition and handling of subsequent host theme changes through code and styles. The documented `toolboxAPI.utils.getCurrentTheme()` returns a promise for the host theme; consult the current [ToolBox API](https://docs.powerplatformtoolbox.com/tool-development/api-reference/toolbox-api) and [events API](https://docs.powerplatformtoolbox.com/tool-development/api-reference/events) for the actual contract instead of inventing a theme event.

In PPTB, load the candidate in each theme and switch the app theme while it is open. Inspect text, controls, dialogs, loading/error states, and charts for legibility. A manual tool toggle or OS `prefers-color-scheme` alone does not demonstrate reaction to the PPTB theme. If implementation looks correct but no live observations exist, report the static evidence and the remaining runtime check.

### R6. Packaged SVG icon

Resolve the top-level `icon` relative to the distribution root, confirm the SVG exists in the released archive, parse it as SVG, and inspect that it renders. Do not resolve `icons/tool.svg` relative to repository root when the manifest means `dist/icons/tool.svg`. Reject remote URL values, stale `configurations.iconURL`, missing packaged files, and invalid/non-SVG content. Inspect build copy rules when the archive is inaccessible and keep packaging evidence unverified until demonstrated.

Source: [package manifest](https://docs.powerplatformtoolbox.com/tool-development/manifest).

### R7. Released version

The required published version is **1.0.0 or greater**. Compare the candidate's actual published manifest/release with that floor using SemVer, not lexical string comparison. Check prerelease ordering (for example, `1.0.0-beta.1` precedes `1.0.0`). Record prerelease production-readiness ambiguity for reviewer judgment rather than inventing a blanket ban on every prerelease above the floor. A development manifest bump does not establish a release was published.

### R8. Bug response health

Inventory **all open bug reports** relevant to the tool, including unlabeled reports that describe defects and any external tracker. Exclude feature requests; show ambiguous classification. Paginate both issues and comments. Verify the responding account's maintainer/contributor role; automated acknowledgements and unrelated user comments do not establish an accountable human response.

For each bug, record its URL, creation timestamp, first maintainer response timestamp (if any), and elapsed time in days. Use creation-to-first-response, not last update, closure time, or latest comment. For unanswered reports, compute age at review time. A later response does not rewrite the historical first-response delay. Show the oldest unanswered bug and explain the exact threshold breached. If a previously breached threshold has since been addressed, record both the history and current evidence for reviewer judgment.

The current policy thresholds are:

| Result | Open bugs and response delay |
| --- | --- |
| Pass | Fewer than 5; each first response within 10 days. |
| Reviewer judgment | At least 5, or a delay over 10 days. |
| Fail / blocker | A delay over 30 days. |

A new unanswered report still within its response window has not yet breached a threshold, but does not prove every bug has received a response; record that pending evidence. Zero bugs is positive evidence only if tracker coverage is established. Never treat disabled issues or a truncated query as zero bugs.

### R9. Accountable active contributor

Correlate named contributors, maintainer identities, contact/issue channels, and dated human commits or issue responses. Identify at least one person and the evidence of accountability, reachability, and recent activity. A bot dependency update alone is insufficient. The policy defines no numerical recency window: show dates and explain the assessment without manufacturing one. Do not contact a person to test reachability as part of this review.

### R10. Breaking-change maintenance

Compare used dependency and host API versions with breaking changes applicable to the tool. Trace migrations through implementation, changelog, commits, and released artifacts. Staying on an older dependency major is not automatically a failure: establish whether an unaddressed breaking change actually affects the supported runtime or released package. Keep dependency freshness suggestions separate from proven compatibility failures.

### R11. Usage and trust

Check and report each metric, its source, period, observation date, and whether it applies to this tool:

| Metric | Policy threshold |
| --- | --- |
| Monthly active users | At least 10. |
| Cumulative downloads | At least 50. |
| Qualifying user review | At least one rating of 3 or higher, submitted through the PPTB app. |

Two demonstrated thresholds satisfy the combined requirement even if the third is unknown. With fewer than two passes, distinguish proven shortfall from missing data. When unknown metrics could change the outcome, leave the result as needs evidence. Use official marketplace/dashboard data or dated owner evidence with clear provenance; do not invent a public metrics endpoint or equate npm downloads to PPTB adoption.

A new tool without usage history may be considered for a reviewer waiver when other criteria meet a high standard. Describe the candidate evidence and request a human decision; the agent cannot award the waiver or waive missing evidence elsewhere.

## Optional criteria

- **O1. Contrast:** Observe text and controls in both themes, recording obvious issues with screenshots or affected components. Quantitative contrast measurements may help but are not an additional strict WCAG approval gate.
- **O2. Initial-load console:** Capture the console while the candidate loads inside PPTB; attribute errors to the tool or host and note warnings separately. A successful build or unit test is not a clean runtime console. Missing access means needs evidence; observed errors are optional findings, not required blockers by themselves.

## Request and ongoing badge guidance

Use the live maturity page for the exact request flow, current SLA, and outcome process. The baseline flow is My Tools → the owner's Unverified tool → Get Verified, followed by a queue confirmation email and a single approval/rejection email; the stated SLA is 1–2 weeks. Review has no back-and-forth stage, and rejection permits immediate resubmission after fixes. Warn that updating a queued/under-review candidate cancels the request. Submission, approval, and any waiver belong to PPTB; this skill produces preparation evidence.

For existing Verified tools, assess changes since the reviewed release and consult [Keeping the Badge](https://docs.powerplatformtoolbox.com/tool-development/maturity-model#keeping-the-badge). The baseline governance triggers are high/critical CVEs and added CSP exceptions (immediate removal), bug-health breaches (two-week grace), and unaddressed API breaking changes (two weeks after release). Reinstatement requires full review. Distinguish a potential trigger found in code from an observed badge removal; do not claim automated governance has run unless there is evidence.
