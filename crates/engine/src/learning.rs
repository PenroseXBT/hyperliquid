use crate::domain::decision::Side;
use crate::domain::ioc::{plan_marketable_ioc, quote_ioc, ExecutableBook};
use crate::domain::portfolio_risk::MarketRules;
use ::mfce::delayed::{DelayedLearning, ExecutableAnchor};
use ::mfce::lifecycle::{MfceDirection, MfceFeatureVector};
use rust_decimal::Decimal;

/// Quote the exact quantity through the production IOC planner and walker.
/// Missing depth is not completed at a synthetic limit price.
pub fn executable_anchor(
    book: &ExecutableBook,
    side: Side,
    quantity: Decimal,
    rules: &MarketRules,
    fee_rate: Decimal,
    cushion: Decimal,
    slippage: Decimal,
) -> Option<ExecutableAnchor> {
    if quantity <= Decimal::ZERO
        || fee_rate < Decimal::ZERO
        || book.bids.first()?.price >= book.asks.first()?.price
    {
        return None;
    }
    let plan = plan_marketable_ioc(
        side,
        quantity,
        book,
        book.midpoint,
        cushion,
        slippage,
        rules.price_tick,
    )
    .ok()?;
    let (filled, value) = quote_ioc(side, quantity, plan.limit_price, book).ok()?;
    (filled == quantity).then_some(ExecutableAnchor {
        quantity,
        midpoint: book.midpoint,
        fill_price: value.checked_div(filled)?,
        fees: value.checked_mul(fee_rate)?,
        actual: false,
    })
}

pub fn observe_executable(
    state: &mut DelayedLearning,
    asset: &str,
    now: u64,
    book: &ExecutableBook,
    rules: &MarketRules,
    fee_rate: Decimal,
    cushion: Decimal,
    slippage: Decimal,
) -> Vec<(MfceFeatureVector, MfceDirection, u64, Decimal)> {
    state.observe(asset, now, |direction, quantity| {
        let side = if direction == MfceDirection::Long {
            Side::Sell
        } else {
            Side::Buy
        };
        executable_anchor(book, side, quantity, rules, fee_rate, cushion, slippage)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::decision::MarketSnapshotId;
    use crate::domain::ioc::DepthLevel;
    use ::mfce::delayed::*;
    use ::mfce::lifecycle::MfcePolicyState;

    fn book(mid: i64, depth: i64) -> ExecutableBook {
        ExecutableBook {
            snapshot_id: MarketSnapshotId([0; 32]),
            observed_at_mono: 0,
            midpoint: Decimal::from(mid),
            bids: vec![DepthLevel {
                price: Decimal::from(mid - 1),
                quantity: Decimal::from(depth),
            }],
            asks: vec![DepthLevel {
                price: Decimal::from(mid + 1),
                quantity: Decimal::from(depth),
            }],
        }
    }
    fn rules() -> MarketRules {
        MarketRules {
            mark_price: Decimal::from(100),
            price_tick: Decimal::ONE,
            size_step: Decimal::ONE,
        }
    }
    fn sample(kind: DecisionKind, direction: MfceDirection) -> DecisionSample {
        let side = if (direction == MfceDirection::Long)
            != matches!(
                kind,
                DecisionKind::Exit | DecisionKind::Reduce | DecisionKind::Hold
            ) {
            Side::Buy
        } else {
            Side::Sell
        };
        let mut features = MfceFeatureVector::new([Decimal::ZERO; crate::mfce::MFCE_FEATURE_COUNT]);
        features.context = Some([Decimal::ZERO; 16]);
        DecisionSample {
            id: 0,
            asset: "BTC".into(),
            decision_at: 0,
            kind,
            origin: AlphaOrigin::Source,
            provenance: DecisionProvenance::MfceRejected,
            mode: Some(MfcePolicyState::Reject),
            direction,
            features,
            prediction: None,
            proposed_delta: Decimal::ONE,
            actual_delta: Decimal::ZERO,
            reentry_after_exit: false,
            cloid: Some("action".into()),
            anchor: executable_anchor(
                &book(100, 10),
                side,
                Decimal::ONE,
                &rules(),
                Decimal::new(1, 2),
                Decimal::ZERO,
                Decimal::new(5, 2),
            ),
            forward: Default::default(),
            pending_mask: 31,
            mfe_net_bps: None,
            mae_net_bps: None,
            time_to_mfe_ms: None,
            entry_regret_bps: Default::default(),
            exit_regret_bps: Default::default(),
            hold_regret_bps: Default::default(),
            reentry_regret_bps: Default::default(),
            cost_drag_bps: Default::default(),
            funding_credit: Some(Decimal::ZERO),
            funding_start: 0,
            actual_result: None,
            size_regret_bps: Default::default(),
            prediction_curve: Vec::new(),
            larger_anchors: Vec::new(),
            size_regret_curve_bps: Default::default(),
            label_unavailable: Default::default(),
            component_unavailable: Default::default(),
            size_regret_unavailable: Default::default(),
        }
    }
    #[test]
    fn additive_provenance_roundtrip_and_legacy_bytes_preserve_frozen_action() {
        // Exact pre-addition six-field sequence, including a nonempty ring.
        let mut old = DelayedLearning::default();
        let mut decision = sample(DecisionKind::Open, MfceDirection::Long);
        decision.cloid = None;
        old.capture(decision.clone());
        let bytes = rmp_serde::to_vec(&(
            &old.samples,
            old.next_id,
            old.dropped,
            &old.flow,
            &old.turnover,
            old.funding_at,
        ))
        .unwrap();
        let decoded: DelayedLearning = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(rmp_serde::to_vec(&decoded).unwrap(), bytes);
        assert!(decoded.provenance.actions.is_empty());

        let mut state = decoded;
        decision.cloid = Some("source-open".into());
        decision.origin = AlphaOrigin::Source;
        state.capture(decision.clone());
        // An action-only snapshot must not deserialize as episode history.
        let bytes = rmp_serde::to_vec(&state).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<DelayedLearning>(&bytes).unwrap(),
            state
        );
        state.position_fill("BTC", None, Some("episode"), "source-open", 10);
        state.observe_episode_origin("episode", "BTC", 10, 20, AlphaOrigin::Flow);
        decision.origin = AlphaOrigin::Flow;
        state.capture(decision); // duplicate association cannot relabel issuance
        assert_eq!(
            state.provenance.actions["source-open"].origin,
            AlphaOrigin::Source
        );
        assert_eq!(
            state.provenance.episodes["episode"].transitions,
            vec![(10, AlphaOrigin::Source), (20, AlphaOrigin::Flow)]
        );
        state.compact_provenance_before(100);
        assert!(state.provenance.actions.contains_key("source-open"));
        let bytes = rmp_serde::to_vec(&state).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<DelayedLearning>(&bytes).unwrap(),
            state
        );
        state.position_fill("BTC", Some("episode"), None, "manual-exit", 101);
        state.compact_provenance_before(102);
        assert!(state.provenance.actions.is_empty());
        assert!(state.provenance.episodes.is_empty());
    }

    fn observe(
        state: &mut DelayedLearning,
        at: u64,
        mid: i64,
        depth: i64,
    ) -> Vec<(MfceFeatureVector, MfceDirection, u64, Decimal)> {
        observe_executable(
            state,
            "BTC",
            at,
            &book(mid, depth),
            &rules(),
            Decimal::new(1, 2),
            Decimal::ZERO,
            Decimal::new(5, 2),
        )
    }

    #[test]
    fn gross_cost_and_net_namespaces_do_not_double_charge() {
        for direction in [MfceDirection::Long, MfceDirection::Short] {
            let mut state = DelayedLearning::default();
            state.capture(sample(DecisionKind::Reject, direction));
            assert!(observe(&mut state, 899_999, 110, 10).is_empty());
            let labels = observe(&mut state, 901_000, 110, 10);
            let outcome = state.samples[0].forward[HORIZON_15M].as_ref().unwrap();
            let expected_net_bps = outcome.net_pnl / Decimal::from(100) * Decimal::from(10_000);
            assert_eq!(labels.last().unwrap().3, expected_net_bps);
            assert_eq!(
                outcome.net_pnl,
                outcome.gross_pnl - outcome.fees - outcome.slippage + outcome.funding
            );
            assert!(outcome.net_pnl < outcome.gross_pnl);
            assert!(state.samples[0].entry_regret_bps[HORIZON_15M].is_some());
            assert!(state.samples[0].size_regret_bps[HORIZON_15M].is_none());
            assert!(state.valid());
        }
    }

    #[test]
    fn reductions_and_exits_value_only_the_removed_slice() {
        for kind in [DecisionKind::Reduce, DecisionKind::Exit] {
            let mut state = DelayedLearning::default();
            state.capture(sample(kind, MfceDirection::Long));
            observe(&mut state, 900_000, 110, 10);
            let outcome = state.samples[0].forward[HORIZON_15M].as_ref().unwrap();
            // Sell now at 99 (fee .99), or later at 109 (fee 1.09).
            assert_eq!(outcome.net_pnl, Decimal::new(990, 2));
            assert!(state.samples[0].exit_regret_bps[HORIZON_15M].unwrap() > Decimal::ZERO);
        }
    }

    #[test]
    fn continuation_value_never_recharges_sunk_entry_costs() {
        let mut state = DelayedLearning::default();
        let clean = sample(DecisionKind::Hold, MfceDirection::Long);
        let mut costly_entry = clean.clone();
        costly_entry.provenance = DecisionProvenance::Selected;
        costly_entry.actual_result = RealizedOutcome::new(
            Decimal::ZERO,
            Decimal::from(50),
            Decimal::from(25),
            Decimal::ZERO,
        );
        state.capture(clean);
        state.capture(costly_entry);
        observe(&mut state, 900_000, 110, 10);
        let first = state.samples[0].forward[HORIZON_15M].as_ref().unwrap();
        let second = state.samples[1].forward[HORIZON_15M].as_ref().unwrap();
        assert_eq!(first, second);
        // Executable liquidation later (109 - 1.09 fee) minus executable
        // liquidation now (99 - .99 fee). Original entry cost is sunk.
        assert_eq!(first.net_pnl, Decimal::new(990, 2));
    }

    #[test]
    fn size_regret_requires_depth_at_both_sizes_and_both_times() {
        for depth in [1, 10] {
            let mut state = DelayedLearning::default();
            let mut pending = sample(DecisionKind::Open, MfceDirection::Long);
            pending.larger_anchors = [Decimal::from(2), Decimal::from(3)]
                .into_iter()
                .filter_map(|quantity| {
                    executable_anchor(
                        &book(100, 10),
                        Side::Buy,
                        quantity,
                        &rules(),
                        Decimal::new(1, 2),
                        Decimal::ZERO,
                        Decimal::new(5, 2),
                    )
                })
                .collect();
            state.capture(pending);
            assert_eq!(observe(&mut state, 900_000, 110, depth).len(), 1);
            assert_eq!(
                state.samples[0].size_regret_bps[HORIZON_15M].is_some(),
                depth == 10
            );
            assert_eq!(
                state.samples[0].size_regret_curve_bps[HORIZON_15M].len(),
                if depth == 10 { 2 } else { 0 }
            );
            if depth == 10 {
                let curve = &state.samples[0].size_regret_curve_bps[HORIZON_15M];
                assert_eq!(curve[0].additional_quantity, Decimal::ONE);
                assert_eq!(curve[1].additional_quantity, Decimal::ONE);
                assert_eq!(curve[0].cumulative_additional_quantity, Decimal::ONE);
                assert_eq!(curve[1].cumulative_additional_quantity, Decimal::from(2));
            }
        }
    }

    #[test]
    fn held_position_size_curve_values_independent_marginal_adds() {
        let mut state = DelayedLearning::default();
        let mut pending = sample(DecisionKind::Hold, MfceDirection::Long);
        pending.larger_anchors.push(
            executable_anchor(
                &book(100, 10),
                Side::Buy,
                Decimal::ONE,
                &rules(),
                Decimal::new(1, 2),
                Decimal::ZERO,
                Decimal::new(5, 2),
            )
            .unwrap(),
        );
        state.capture(pending);
        observe(&mut state, 900_000, 110, 10);
        let curve = &state.samples[0].size_regret_curve_bps[HORIZON_15M];
        assert_eq!(curve.len(), 1);
        assert_eq!(curve[0].additional_quantity, Decimal::ONE);
        assert_eq!(curve[0].cumulative_additional_quantity, Decimal::ONE);
        assert!(curve[0].net_edge_bps > Decimal::ZERO);
    }

    #[test]
    fn capacity_curve_uses_adjacent_not_cumulative_marginal_economics() {
        let current = ExecutableBook {
            snapshot_id: MarketSnapshotId([1; 32]),
            observed_at_mono: 0,
            midpoint: Decimal::from(100),
            bids: vec![DepthLevel {
                price: Decimal::from(99),
                quantity: Decimal::from(10),
            }],
            asks: [101, 105, 115]
                .into_iter()
                .map(|price| DepthLevel {
                    price: Decimal::from(price),
                    quantity: Decimal::ONE,
                })
                .collect(),
        };
        let future = ExecutableBook {
            snapshot_id: MarketSnapshotId([2; 32]),
            observed_at_mono: 900_000,
            midpoint: Decimal::from(110),
            bids: [109, 105, 95]
                .into_iter()
                .map(|price| DepthLevel {
                    price: Decimal::from(price),
                    quantity: Decimal::ONE,
                })
                .collect(),
            asks: vec![DepthLevel {
                price: Decimal::from(111),
                quantity: Decimal::from(10),
            }],
        };
        let mut pending = sample(DecisionKind::Open, MfceDirection::Long);
        let anchor = |book: &ExecutableBook, side, quantity| {
            executable_anchor(
                book,
                side,
                quantity,
                &rules(),
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::new(5, 1),
            )
            .unwrap()
        };
        pending.anchor = Some(anchor(&current, Side::Buy, Decimal::ONE));
        pending.larger_anchors = [2, 3]
            .into_iter()
            .map(|quantity| anchor(&current, Side::Buy, Decimal::from(quantity)))
            .collect();
        let mut state = DelayedLearning::default();
        state.capture(pending);
        state.observe("BTC", 900_000, |direction, quantity| {
            Some(anchor(
                &future,
                if direction == MfceDirection::Long {
                    Side::Sell
                } else {
                    Side::Buy
                },
                quantity,
            ))
        });
        let curve = &state.samples[0].size_regret_curve_bps[HORIZON_15M];
        assert_eq!(curve.len(), 2);
        assert_eq!(curve[0].net_edge_bps, Decimal::ZERO);
        assert_eq!(curve[1].net_edge_bps, Decimal::from(-2_000));
    }

    #[test]
    fn missing_depth_gaps_restart_and_late_books_stay_missing() {
        for cause in 0..4 {
            let mut state = DelayedLearning::default();
            let mut pending = sample(DecisionKind::Open, MfceDirection::Long);
            if cause == 0 {
                pending.anchor = None;
            }
            state.capture(pending);
            if cause == 1 {
                state.gap();
            }
            let at = if cause == 2 { 903_000 } else { 900_000 };
            assert!(observe(&mut state, at, 110, if cause == 3 { 0 } else { 10 }).is_empty());
            assert!(state.samples[0].forward[HORIZON_15M].is_none());
            state.expire(14_403_000);
            assert!(state.markets().is_empty());
            assert_eq!(state.samples[0].pending_mask, 0);
            assert_eq!(
                state.samples[0].label_unavailable[HORIZON_15M],
                Some(match cause {
                    0 => LabelUnavailableReason::MissingDecisionAnchor,
                    1 => LabelUnavailableReason::MissingFundingContinuity,
                    2 => LabelUnavailableReason::NoTimelyObservation,
                    _ => LabelUnavailableReason::MissingExecutableDepth,
                })
            );
            assert!(state.samples[0].component_unavailable[HORIZON_15M]
                .net_remaining_edge
                .is_some());
        }
    }

    #[test]
    fn objective_specific_horizons_bound_dense_subminute_state() {
        for (kind, expected) in [
            (DecisionKind::Open, 0b111_1111),
            (DecisionKind::Add, 0b011_1111),
            (DecisionKind::Hold, 0b000_1111),
            (DecisionKind::Reduce, 0b000_1111),
            (DecisionKind::Exit, 0b010_1111),
        ] {
            let mut state = DelayedLearning::default();
            state.capture(sample(kind, MfceDirection::Long));
            assert_eq!(state.samples[0].pending_mask, expected);
        }
    }

    #[test]
    fn repeated_counterfactual_repricing_is_coalesced_until_material_change() {
        let mut state = DelayedLearning::default();
        let initial = sample(DecisionKind::BudgetConstrained, MfceDirection::Long);
        state.capture(initial.clone());

        let mut repriced = initial.clone();
        repriced.decision_at = 1_000;
        repriced.anchor.as_mut().unwrap().midpoint = Decimal::from(101);
        repriced.anchor.as_mut().unwrap().fill_price = Decimal::from(102);
        state.capture(repriced);
        assert_eq!(state.samples.len(), 1);
        assert_eq!(
            state.dropped, 0,
            "intentional coalescing is not sample loss"
        );

        let mut materially_new = initial;
        materially_new.decision_at = 2_000;
        materially_new.proposed_delta = Decimal::from(2);
        state.capture(materially_new);
        assert_eq!(state.samples.len(), 2);
    }

    #[test]
    fn selected_decisions_always_retain_and_counterfactuals_get_bounded_heartbeats() {
        let mut selected_state = DelayedLearning::default();
        let mut selected = sample(DecisionKind::Open, MfceDirection::Long);
        selected.provenance = DecisionProvenance::Selected;
        selected.mode = Some(MfcePolicyState::Explore);
        selected_state.capture(selected.clone());
        selected.decision_at = 1;
        selected_state.capture(selected);
        assert_eq!(selected_state.samples.len(), 2);

        let mut counterfactual_state = DelayedLearning::default();
        let mut rejected = sample(DecisionKind::Reject, MfceDirection::Long);
        counterfactual_state.capture(rejected.clone());
        rejected.decision_at = 3_600_000;
        counterfactual_state.capture(rejected);
        assert_eq!(counterfactual_state.samples.len(), 2);
    }

    #[test]
    fn selected_entries_record_after_cost_entry_and_reentry_regret() {
        let mut state = DelayedLearning::default();
        state.turnover.insert(
            "BTC".into(),
            Turnover {
                last_exit: 1,
                ..Default::default()
            },
        );
        let mut selected = sample(DecisionKind::Open, MfceDirection::Long);
        selected.decision_at = 2;
        selected.provenance = DecisionProvenance::Selected;
        selected.mode = Some(MfcePolicyState::Explore);
        state.capture(selected);

        assert!(state.samples[0].reentry_after_exit);
        observe(&mut state, 900_002, 90, 10);
        let index = HORIZON_15M;
        let net = state.samples[0].forward[index].as_ref().unwrap().net_pnl / Decimal::from(100)
            * Decimal::from(10_000);
        assert_eq!(state.samples[0].entry_regret_bps[index], Some(net));
        assert_eq!(state.samples[0].reentry_regret_bps[index], Some(net));
        assert!(
            net < Decimal::ZERO,
            "round-trip costs and price loss are retained"
        );
    }

    #[test]
    fn regretted_exit_produces_worse_exit_quality_target_than_correct_exit() {
        // EXIT → delayed observation → ExitQuality training label. A missed
        // favorable continuation (bad exit, later reacquired) must produce a
        // worse ExitQuality target than an otherwise equivalent correct exit.
        // Labels use actual executable costs (authenticated fee/slippage in
        // production) and flow into MfceTrainingSample rows the model trains on.
        use ::mfce::lifecycle::{LearningObjective, MfceEngine};

        let mut bad = DelayedLearning::default();
        let mut exit = sample(DecisionKind::Exit, MfceDirection::Long);
        exit.provenance = DecisionProvenance::Selected;
        exit.mode = Some(MfcePolicyState::Exploit);
        bad.capture(exit);
        let mut good = DelayedLearning::default();
        let mut exit = sample(DecisionKind::Exit, MfceDirection::Long);
        exit.provenance = DecisionProvenance::Selected;
        exit.mode = Some(MfcePolicyState::Exploit);
        good.capture(exit);

        // Favorable continuation after the exit: retention was valuable, so
        // the exit is regretted. Adverse continuation: the exit was correct.
        let bad_labels = observe(&mut bad, 900_000, 110, 10);
        let good_labels = observe(&mut good, 900_000, 90, 10);
        assert!(!bad_labels.is_empty() && !good_labels.is_empty());
        let bad_outcome = bad.samples[0].forward[HORIZON_15M].as_ref().unwrap();
        let good_outcome = good.samples[0].forward[HORIZON_15M].as_ref().unwrap();
        // Actual after-cost truth drives both labels: executable fees and
        // slippage are deducted (retention nets the saved entry fee, so the
        // signed fee residual may be positive or negative), and the identity
        // holds exactly.
        for outcome in [bad_outcome, good_outcome] {
            assert_eq!(
                outcome.net_pnl,
                outcome.gross_pnl - outcome.fees - outcome.slippage + outcome.funding
            );
            assert_ne!(outcome.net_pnl, outcome.gross_pnl);
        }
        for state in [&bad, &good] {
            // Decision-time executable anchor carries a real quoted fee.
            assert!(state.samples[0].anchor.as_ref().unwrap().fees > Decimal::ZERO);
            assert!(state.samples[0].cost_drag_bps[HORIZON_15M].is_some());
        }
        assert!(bad.samples[0].exit_regret_bps[HORIZON_15M].unwrap() > Decimal::ZERO);
        let bad_edge = bad_labels.iter().map(|label| label.3).max().unwrap();
        let good_edge = good_labels.iter().map(|label| label.3).max().unwrap();
        assert!(
            bad_edge > good_edge,
            "regretted exit target {bad_edge} must exceed correct exit target {good_edge}"
        );

        // Both labels become ExitQuality training rows consumed by training.
        let mut engine = MfceEngine::default();
        for (labels, completed) in [(&bad_labels, 900_000), (&good_labels, 900_001)] {
            for (features, direction, opened, edge) in labels {
                engine.learn_delayed(
                    "BTC",
                    features.clone(),
                    *direction,
                    *opened,
                    completed,
                    *edge,
                );
            }
        }
        assert_eq!(
            engine.state().samples.len(),
            bad_labels.len() + good_labels.len()
        );
        for row in &engine.state().samples {
            assert_eq!(row.objective, LearningObjective::ExitQuality);
            assert_eq!(row.features.objective(), LearningObjective::ExitQuality);
        }
        let bad_target = engine
            .state()
            .samples
            .iter()
            .take(bad_labels.len())
            .map(|row| row.remaining_net_edge_bps)
            .max()
            .unwrap();
        let good_target = engine
            .state()
            .samples
            .iter()
            .skip(bad_labels.len())
            .map(|row| row.remaining_net_edge_bps)
            .max()
            .unwrap();
        assert_eq!(bad_target, bad_edge);
        assert_eq!(good_target, good_edge);
        assert!(bad_target > good_target);
    }

    #[test]
    fn exit_then_rapid_same_side_reopen_marks_reentry_regret() {
        // R4: OPEN -> EXIT -> rapid OPEN LONG is negative ExitQuality/regret
        // evidence. The reopened entry carries reentry_after_exit and
        // reentry_regret; the EXIT itself retains exit_regret over the
        // retention counterfactual so the model learns bad abandonment.
        let mut state = DelayedLearning::default();
        let mut exit = sample(DecisionKind::Exit, MfceDirection::Long);
        exit.decision_at = 0;
        exit.provenance = DecisionProvenance::Selected;
        exit.mode = Some(MfcePolicyState::Exploit);
        state.capture(exit);
        // Settle the exit: flat after removing the full position.
        state.fill(
            "BTC",
            "exit-cloid",
            1,
            Decimal::new(-1, 0),
            Decimal::from(100),
            Decimal::new(1, 2),
            Decimal::ONE,
            Decimal::from(100),
            RealizedOutcome::settled(
                Decimal::ZERO,
                Decimal::new(1, 2),
                Decimal::ZERO,
                Decimal::ZERO,
            )
            .unwrap(),
        );
        assert!(state.turnover.get("BTC").is_some_and(|t| t.last_exit == 1));
        let mut reopen = sample(DecisionKind::Open, MfceDirection::Long);
        reopen.decision_at = 2;
        reopen.provenance = DecisionProvenance::Selected;
        reopen.mode = Some(MfcePolicyState::Explore);
        state.capture(reopen);
        assert!(state.samples[1].reentry_after_exit);
        observe(&mut state, 900_002, 90, 10);
        let index = HORIZON_15M;
        // The exit's retention counterfactual is labeled...
        assert!(state.samples[0].exit_regret_bps[index].is_some());
        // ...and the rapid reacquisition carries entry + reentry regret
        // including both legs' executable costs.
        assert!(state.samples[1].entry_regret_bps[index].is_some());
        assert_eq!(
            state.samples[1].entry_regret_bps[index],
            state.samples[1].reentry_regret_bps[index]
        );
    }

    #[test]
    fn retaining_decisions_record_observed_hold_and_exit_regret() {
        let mut state = DelayedLearning::default();
        state.capture(sample(DecisionKind::Hold, MfceDirection::Long));
        observe(&mut state, 900_000, 110, 10);
        assert_eq!(
            state.samples[0].hold_regret_bps[HORIZON_15M],
            state.samples[0].exit_regret_bps[HORIZON_15M]
        );
        assert!(state.samples[0].hold_regret_bps[HORIZON_15M].is_some());
    }

    #[test]
    fn selected_decisions_displace_counterfactual_capacity_and_bypass_market_cap() {
        let mut state = DelayedLearning::default();
        for index in 0..MAX_DECISIONS {
            let mut pending = sample(DecisionKind::Reject, MfceDirection::Long);
            pending.asset = format!("A{index}");
            pending.cloid = None;
            state.capture(pending);
        }
        let mut selected = sample(DecisionKind::Open, MfceDirection::Long);
        selected.asset = "SELECTED".into();
        selected.provenance = DecisionProvenance::Selected;
        selected.mode = Some(MfcePolicyState::Explore);
        state.capture(selected);
        assert_eq!(state.samples.len(), MAX_DECISIONS);
        assert!(state.samples.iter().any(|sample| {
            sample.asset == "SELECTED" && sample.provenance == DecisionProvenance::Selected
        }));
        assert_eq!(state.dropped, 0);

        let mut one_market = DelayedLearning::default();
        for index in 0..(MAX_PER_MARKET + 8) {
            let mut selected = sample(DecisionKind::Open, MfceDirection::Long);
            selected.decision_at = index as u64;
            selected.provenance = DecisionProvenance::Selected;
            selected.mode = Some(MfcePolicyState::Explore);
            selected.cloid = Some(format!("selected-{index}"));
            one_market.capture(selected);
        }
        assert_eq!(one_market.samples.len(), MAX_PER_MARKET + 8);
        assert_eq!(one_market.dropped, 0);
    }

    #[test]
    fn four_hour_open_position_keeps_structural_label_without_dense_queue_growth() {
        let mut state = DelayedLearning::default();
        state.capture(sample(DecisionKind::Open, MfceDirection::Long));
        let opening_id = state.samples[0].id;
        for at in (30_000..=14_400_000).step_by(30_000) {
            let mut hold = sample(DecisionKind::Hold, MfceDirection::Long);
            hold.decision_at = at;
            hold.features
                .set_position_learning(None, crate::mfce::LearningObjective::ContinuationQuality);
            state.capture(hold);
            assert!(
                state
                    .samples
                    .iter()
                    .filter(|sample| sample.asset == "BTC" && sample.pending_mask != 0)
                    .count()
                    <= MAX_PER_MARKET
            );
        }
        let opening = state
            .samples
            .iter()
            .find(|sample| sample.id == opening_id)
            .unwrap();
        assert_ne!(opening.pending_mask & (1 << 6), 0);
        assert_eq!(state.dropped, 0);
        state.expire(14_403_000);
        assert_eq!(state.samples[0].pending_mask, 0);
    }

    #[test]
    fn material_continuation_change_is_debounced_but_not_heartbeat_only() {
        let mut state = DelayedLearning::default();
        let mut frozen = sample(DecisionKind::Hold, MfceDirection::Long);
        frozen.decision_at = 1_000;
        frozen
            .features
            .set_position_learning(None, crate::mfce::LearningObjective::ContinuationQuality);
        let mut changed = frozen.features.clone();
        changed.context.as_mut().unwrap()[1] = Decimal::new(2, 1);
        state.capture(frozen);
        assert!(!state.position_feature_change_due("BTC", 1_500, &changed));
        assert!(state.position_feature_change_due("BTC", 2_001, &changed));
        assert!(!state.position_feature_change_due("BTC", 2_001, &state.samples[0].features));
    }

    #[test]
    fn delayed_exit_walks_observed_slices_and_never_uses_admission_remainder() {
        for direction in [MfceDirection::Long, MfceDirection::Short] {
            let mut state = DelayedLearning::default();
            let mut pending = sample(DecisionKind::BudgetConstrained, direction);
            pending.mode = Some(MfcePolicyState::Explore);
            pending.provenance = DecisionProvenance::AllocatorDisplaced;
            state.capture(pending);
            let mut exit = book(
                if direction == MfceDirection::Long {
                    110
                } else {
                    90
                },
                1,
            );
            let levels = if direction == MfceDirection::Long {
                &mut exit.bids
            } else {
                &mut exit.asks
            };
            levels[0].quantity = Decimal::new(4, 1);
            levels.push(DepthLevel {
                price: levels[0].price - Decimal::from(direction.sign()),
                quantity: Decimal::new(6, 1),
            });
            let mut missing = exit.clone();
            let side = if direction == MfceDirection::Long {
                Side::Sell
            } else {
                Side::Buy
            };
            if side == Side::Sell {
                missing.bids.truncate(1);
            } else {
                missing.asks.truncate(1);
            }
            let admission = plan_marketable_ioc(
                side,
                Decimal::ONE,
                &missing,
                missing.midpoint,
                Decimal::ZERO,
                Decimal::new(5, 2),
                Decimal::ONE,
            )
            .unwrap();
            assert_eq!(
                admission.pricing_mode,
                crate::domain::ioc::MarketableIocPricingMode::BoundedReferenceFallback
            );
            assert!(observe_executable(
                &mut state,
                "BTC",
                900_000,
                &missing,
                &rules(),
                Decimal::new(1, 2),
                Decimal::ZERO,
                Decimal::new(5, 2)
            )
            .is_empty());
            assert!(state.samples[0].forward[HORIZON_15M].is_none());
            let labels = observe_executable(
                &mut state,
                "BTC",
                901_000,
                &exit,
                &rules(),
                Decimal::new(1, 2),
                Decimal::ZERO,
                Decimal::new(5, 2),
            );
            let net_bps = state.samples[0].forward[HORIZON_15M]
                .as_ref()
                .unwrap()
                .net_pnl
                / Decimal::from(100)
                * Decimal::from(10_000);
            assert_eq!(labels.last().unwrap().3, net_bps);
            assert_eq!(
                state.samples[0].forward[HORIZON_15M]
                    .as_ref()
                    .unwrap()
                    .slippage,
                Decimal::new(26, 1)
            );
            assert_eq!(state.samples[0].mode, Some(MfcePolicyState::Explore));
            assert!(state.samples[0].entry_regret_bps[HORIZON_15M].is_some());
        }
    }

    #[test]
    fn actual_fill_replaces_model_anchor_and_preserves_settled_costs() {
        let mut state = DelayedLearning::default();
        state.capture(sample(DecisionKind::Open, MfceDirection::Long));
        let frozen = state.samples[0].features.clone();
        let actual = RealizedOutcome::settled(
            Decimal::ZERO,
            Decimal::ONE,
            Decimal::from(2),
            Decimal::new(-5, 1),
        )
        .unwrap();
        state.fill(
            "BTC",
            "action",
            1_000,
            Decimal::ONE,
            Decimal::from(102),
            Decimal::ONE,
            Decimal::ZERO,
            Decimal::from(100),
            actual.clone(),
        );
        observe(&mut state, 900_000, 110, 10);
        assert_eq!(state.samples[0].actual_result, Some(actual));
        assert_eq!(state.samples[0].features, frozen);
        assert_eq!(
            state.samples[0].anchor.as_ref().unwrap().fill_price,
            Decimal::from(102)
        );
        assert_eq!(
            state.samples[0].forward[HORIZON_15M]
                .as_ref()
                .unwrap()
                .slippage,
            Decimal::from(3)
        );
    }

    #[test]
    fn bounded_scheduler_and_family_books_are_independent() {
        let mut state = DelayedLearning::default();
        for i in 0..=MAX_DECISIONS {
            let mut pending = sample(DecisionKind::Open, MfceDirection::Long);
            pending.asset = format!("M{}", i / MAX_PER_MARKET);
            pending.provenance = DecisionProvenance::Selected;
            state.capture(pending);
        }
        assert_eq!(state.samples.len(), MAX_DECISIONS);
        assert_eq!(state.dropped, 1);
        let encoded = serde_json::to_vec(&state).unwrap();
        let restored: DelayedLearning = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(restored, state);
        assert!(restored.valid());
        state.samples.clear();
        for origin in [
            AlphaOrigin::Source,
            AlphaOrigin::Flow,
            AlphaOrigin::SourceFlowConfluence,
        ] {
            let mut pending = sample(DecisionKind::Open, MfceDirection::Long);
            pending.origin = origin;
            state.capture(pending);
        }
        observe(&mut state, 900_000, 110, 10);
        assert_eq!(state.performance().len(), 3);
        assert!(state
            .performance()
            .values()
            .all(|p| p.samples == 1 && p.wins == 1));
    }

    #[test]
    fn provenance_and_economic_quality_books_remain_independent() {
        let mut state = DelayedLearning::default();
        for (kind, origin, horizon) in [
            (DecisionKind::Open, AlphaOrigin::Source, HORIZON_15M),
            (DecisionKind::Hold, AlphaOrigin::Flow, HORIZON_5M),
            (
                DecisionKind::Exit,
                AlphaOrigin::SourceFlowConfluence,
                HORIZON_5M,
            ),
        ] {
            let mut decision = sample(kind, MfceDirection::Long);
            decision.origin = origin;
            if kind == DecisionKind::Hold {
                decision.features.set_position_learning(
                    None,
                    crate::mfce::LearningObjective::ContinuationQuality,
                );
            }
            decision.forward[horizon] = RealizedOutcome::new(
                Decimal::from(10),
                Decimal::ONE,
                Decimal::ZERO,
                Decimal::ZERO,
            );
            state.capture(decision);
        }

        let books = state.quality_performance();
        assert_eq!(
            books[&AlphaOrigin::Source][&crate::mfce::LearningObjective::EntryQuality].samples,
            1
        );
        assert_eq!(
            books[&AlphaOrigin::Flow][&crate::mfce::LearningObjective::ContinuationQuality].samples,
            1
        );
        assert_eq!(
            books[&AlphaOrigin::SourceFlowConfluence][&crate::mfce::LearningObjective::ExitQuality]
                .samples,
            1
        );
        assert_eq!(books.len(), 3);
    }

    #[test]
    fn position_trajectory_tracks_path_without_turning_profit_into_an_exit_rule() {
        let mut state = DelayedLearning::default();
        let initial = state
            .observe_position(
                "BTC",
                "episode",
                1_000,
                1_000,
                Decimal::ONE,
                Decimal::from(100),
                Decimal::ZERO,
                Decimal::from(100),
                Decimal::from(1_000),
                Some(MfcePolicyState::Explore),
            )
            .unwrap();
        assert_eq!(initial.mfe_bps, Decimal::ZERO);
        assert_eq!(initial.mae_bps, Decimal::ZERO);

        let peak = state
            .observe_position(
                "BTC",
                "episode",
                1_000,
                11_000,
                Decimal::ONE,
                Decimal::from(100),
                Decimal::ZERO,
                Decimal::from(110),
                Decimal::from(1_000),
                Some(MfcePolicyState::Exploit),
            )
            .unwrap();
        assert_eq!(peak.unrealized_return_bps, Decimal::from(1_000));
        assert_eq!(peak.current_policy_code, Decimal::from(2));

        let retrace = state
            .observe_position(
                "BTC",
                "episode",
                1_000,
                21_000,
                Decimal::ONE,
                Decimal::from(100),
                Decimal::ZERO,
                Decimal::from(105),
                Decimal::from(1_000),
                Some(MfcePolicyState::Explore),
            )
            .unwrap();
        assert_eq!(retrace.unrealized_return_bps, Decimal::from(500));
        assert_eq!(retrace.mfe_bps, Decimal::from(1_000));
        assert_eq!(retrace.mae_bps, Decimal::ZERO);
        assert_eq!(retrace.drawdown_from_mfe_bps, Decimal::from(500));

        let bytes = rmp_serde::to_vec(&state).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<DelayedLearning>(&bytes).unwrap(),
            state
        );
        state.retain_open_trajectories(&std::collections::BTreeSet::new());
        assert!(state.provenance.trajectories.is_empty());
    }

    #[test]
    fn flow_uses_all_aggressors_and_hash_ids_are_not_assumed_ordered() {
        let mut flow = MarketFlow::default();
        for (id, buy) in [(90, true), (3, true), (4, false), (90, true)] {
            flow.trade(60_000, id, Decimal::from(100), buy);
        }
        flow.last_sent_ms = 60_000;
        let features = flow.features(60_000).unwrap();
        assert_eq!(features[1], Decimal::ONE / Decimal::from(3));
        assert_eq!(features[3], Decimal::new(1, 2));
        assert!(flow.features(120_000).is_none());
    }
}
