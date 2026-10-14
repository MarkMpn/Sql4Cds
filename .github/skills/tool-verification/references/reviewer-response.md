# Reviewer web-form responses

Append a **Reviewer response** section at the end of every report. The reviewer uses a web form with a separate response text box for each check. Produce a simple list with one item per current required and optional criterion, including passes; each explanation must stand alone when copied into its text box. Use the current policy's criterion names and order, reconciling the template below with the live [maturity model](https://docs.powerplatformtoolbox.com/tool-development/maturity-model).

## Status and wording

| Report status | Prefix | Response content |
| --- | --- | --- |
| Pass | ✅ | State what was checked and the concrete passing evidence. Preserve scope qualifiers such as static inspection. |
| Not applicable | ✅ | Explicitly say "Not applicable" and explain the inspected evidence that justifies it. |
| Fail | ❌ | Describe the confirmed issue, affected version/file/component where useful, and the action to resolve it. |
| Needs evidence | ❔ | State what remains unverified, why, and the evidence or test needed. Do not describe an unperformed check as a tool defect. |
| Needs Manual Validation | ❔ | High/critical findings are confined to build/development dependencies. Begin the explanation with "Needs Manual Validation" and identify the scope, findings and human review needed. |
| Reviewer judgment | ❔ | Explain the flag or soft gate, supporting evidence, and the decision reserved for the human reviewer. |

Start each item with the icon, criterion name and requirement level, followed by one concise paragraph. Use enough detail to explain the issue clearly; do not compress away distinct high/critical audit findings just to fit one line. Include all three usage metrics within the single usage criterion. Do not create separate items for each metric when the form has one combined check.

Within the CVE response, distinguish affected **direct dependencies** from **transitive (child) dependencies**, naming the direct introducer/path for child findings and separately stating runtime or development/build scope. Explain whether remediation means updating a declared dependency or refreshing/updating its child resolution. Both groups belong in the same CVE text-box item. When high/critical findings are confined to build/development dependencies, use ❔ and explicitly say **Needs Manual Validation**, describing the applicability/exposure review and remediation options. Confirmed runtime high/critical findings use ❌; mixed cases also use ❌, with build findings separately described as needing manual validation. Uncertain scope uses ❔ pending evidence. Retain the full audit and do not claim that build findings are exempt, resolved or approved before the human decision.

Label optional checks **Optional**. An observed optional failure still uses ❌, with an explicit statement that it does not block approval. Keep submission prerequisites and any overall recommendation in a short reviewer-only note before the list; do not mix them into criterion text boxes or invent form fields.

Avoid email subjects, salutations, closing paragraphs, rejection/approval letters and cross-references such as "see above". Include brief evidence directly in each response; URLs may be included when useful, but the text must remain intelligible without opening another report section. Mention the assessed tool/version when an item could otherwise confuse source and shipped-artifact findings.

Where required checks or decisions remain unresolved, the reviewer-only note must say the draft needs completion before an official outcome. A readiness assessment cannot grant a badge, waive a requirement, or establish that a request was submitted. Draft only; do not enter or submit the web form without separate user authorization.

Preserve these distinctions:

- Unavailable live host access means ❔ for untested theme behavior, not ❌.
- An outdated validator or network error means ❔ for unavailable validation evidence, not a proven tool defect.
- An unverified owner's My Tools access is a prerequisite note, not a failed quality criterion.
- Deprecated-but-supported APIs with a demonstrated migration and flagged bug-health thresholds use ❔ pending reviewer judgment, rather than automatic rejection.
- Usage shortfalls require accurate metrics and the applicable waiver possibility. An unresolved waiver uses ❔; do not award it or let it hide other failures. Use ❌ for a confirmed failure with no applicable or granted exception, preserving any recorded human decision.

## Per-criterion template

Replace each `[icon]` with exactly one of ✅, ❌ or ❔ and replace all bracketed guidance with evidence from the review. These are baseline criteria, not fixed web-form field identifiers; add or rename entries if the live policy changes.

- [icon] **README quality (Required):** [Purpose, screenshot/GIF and installation/run findings; describe any missing onboarding step and remedy.]
- [icon] **CSP exceptions documented (Required):** [Exceptions and their necessity/justification, or explicit not-applicable finding after manifest/network inspection.]
- [icon] **No critical or high CVEs (Required):** [Full audit identity/result; separate direct and transitive high/critical packages with versions, child introducers/paths and fixes; state runtime/build scope and source/release differences. For build-only findings use ❔ and "Needs Manual Validation"; for confirmed runtime findings use ❌.]
- [icon] **No deprecated PPTB APIs or unsupported methods (Required):** [Used API support, removed/unsupported calls, or migration evidence and unresolved reviewer decision.]
- [icon] **Reacts to the PPTB app theme (Required):** [Observed initial light/dark behavior, switching and legibility; identify live testing still needed.]
- [icon] **Has an icon (Required):** [Top-level icon path, released SVG presence and validity/rendering evidence, or concrete packaging defect.]
- [icon] **Basic colour contrast (Optional):** [Observed text/control contrast in both themes or missing visual evidence; optional failures do not block approval.]
- [icon] **No console errors on load (Optional):** [Actual PPTB load-console result or missing capture; distinguish warnings and non-blocking errors.]
- [icon] **Version 1.0.0 or greater (Required):** [Actual published version and source, or the needed release.]
- [icon] **Healthy bug response (Required):** [Complete bug inventory and first-response timing; explain any threshold flag/blocker and remedy.]
- [icon] **Active contributor (Required):** [Named accountable contributor, reachable channel and dated human activity, or missing ownership evidence.]
- [icon] **Up to date with breaking changes (Required):** [Applicable migrations and compatibility evidence; identify a proven unresolved change or missing check.]
- [icon] **Meets 2 of 3 usage metrics (Required):** [MAU, downloads and qualifying app ratings with dated provenance; combined result and any pending waiver decision.]

## Wording examples

- ✅ **Has an icon (Required):** The published archive contains `dist/icon.svg`, referenced by the top-level `icon` field; it parses and renders as SVG.
- ❌ **README quality (Required):** The README explains the tool and includes a screenshot, but omits installation and launch instructions. Add the steps needed to install the tool, select a connection and start using it.
- ❔ **Reacts to the PPTB app theme (Required):** Source handles the initial host theme and theme updates, but live light/dark switching and legibility have not been tested. Complete these checks in PPTB on the reviewed release and record the host version and results.

- ❔ **No critical or high CVEs (Required):** Needs Manual Validation — the full audit finds high vulnerabilities in the build tree: direct Vite and transitive PostCSS via Vite. Inspect the advisory applicability and build-process exposure, review patched parent/child resolutions and record the human decision. No runtime high/critical findings were identified; the full audit remains unresolved.
