# MFCE allocation roadmap

## Authority boundary

MFCE owns economic classification and requested source-risk size. It may request less risk than the source transition authorizes; it may never create leverage or override downstream portfolio constraints. Portfolio projection, execution validity, reduce-only behavior, target-ledger accounting, and root integrity remain authoritative.

## MFCE2B — copytrade-first exploration bootstrap

- Compute live `net_q50 = q50 - friction`, `conservative_edge = net_q50 - uncertainty`, `net_q10`, and `score = max(conservative_edge, 0) / max(q50 - q10, 1 bp)`.
- Before an incumbent exists, classify every structurally valid source transition as Explore, including the first transition before empirical backoff support exists.
- Seed each cold-start allocation from normalized aggregate copytrade conviction, then shrink the small probe by conditional q10 loss and uncertainty.
- After promotion, Reject only strong negative expectancy whose upper uncertainty bound remains non-positive; uncertain or weakly negative states remain Explore.
- Exploit positive confidence-adjusted model predictions.
- Evaluate the complete timestamp-consistent candidate set before mutating any target or tail reservation.
- Divide the available source sleeve by relative risk-adjusted score with deterministic tie-breaking and request-size caps.
- Bound all simultaneous Explore opportunities to one 25% global source-sleeve pool; never multiply an Explore percentage by opportunity count. Concentrate probes in conviction order instead of producing non-executable proportional dust across a large cross-section.
- Apply the position-aware q10 dollar budget in score order as a hard sizing cap. It may reduce an allocation to an arbitrarily small positive amount, but it may never be overridden.
- Promote the first model on chronological sanity and q10/q50 calibration; require comparative non-inferiority only when an incumbent actually exists.
- Feed both Exploit and Explore targets through the existing unsigned planned executor and downstream hard-risk projection.
- Publish Exploit, Explore, and Reject counts in rolling status.

Forward coverage objective: Explore the majority of structurally valid opportunities and support roughly 75–150 executions per day when source transitions, exchange minimums, live liquidity, and the unchanged portfolio-risk envelope permit. This is an observation target, never a trade quota or permission to override negative learned edge or q10 capacity.

Exit criteria: deterministic replay equivalence, order-independent allocations, unchanged hard-risk invariant tests, cold-start execution from label zero, calibrated first-model promotion, and forward planned opportunity coverage without worse predicted tail utilization.

## MFCE3 — signed calibration

- Continue quantile calibration from the same bounded MFCE samples and forward allocation outcomes; do not create a separate qualification phase, service, database, or artifact family.
- Keep rejected-transition counterfactual labels and selected-transition settled economics in the existing bounded state and rolling status.
- Attach the minimal signed micro-capital path only after unsigned forward evidence shows positive gross and net edge, profit factor above one, improving Sharpe, controlled drawdown, and sufficient qualified turnover.
- Keep deterministic portfolio risk, reduce-only semantics, accounting, reconciliation, and root conservation sovereign over learned allocation.

Do not add SHAP pipelines, feature-importance artifacts, offline dashboards, training bundles, policy replay packages, or trade-count quotas. The required evidence remains: what MFCE predicted, what it allocated, and what settled.
