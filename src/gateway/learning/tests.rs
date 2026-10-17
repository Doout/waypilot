use super::*;
fn learner(mode: LearningMode) -> Arc<Learner> {
    let mut config: Config = toml::from_str(include_str!("../../../examples/local.toml")).unwrap();
    config.speculation = Some(crate::config::Speculation {
        max_in_flight: 2,
        min_samples: 3,
        confidence_percent: 75,
        max_duration_ms: 1000,
        rules: vec![],
        learning: Some(Learning {
            mode,
            evaluation_samples: 3,
            recovery_samples: 4,
            ..Learning::default()
        }),
    });
    Learner::new(&config, BTreeMap::new()).unwrap()
}
fn outcome(id: i64) -> Outcome {
    Outcome {
        result: json!({"structuredContent":{"employee_id":id}}),
        catalog_revision: "schema-1".into(),
        disposition: "executed",
        elapsed_us: 100_000,
        lineage: Lineage::default(),
    }
}
fn prior(learner: &Learner, n: i64) -> Observation {
    learner
        .observation(
            "hr__employee",
            &json!({"requested_id":n}),
            &outcome(n + 100),
        )
        .unwrap()
}
fn observe(learner: &Learner, n: i64, name: &str, matched: bool) {
    learner.observe(
        "alice",
        1,
        &prior(learner, n),
        name,
        &json!({"employee":{"id":n+100}}),
        &outcome(n + 100),
        60_000.0,
        100_000.0,
        false,
        matched,
    );
}
fn trained(mode: LearningMode) -> Arc<Learner> {
    let l = learner(mode);
    for n in 1..=3 {
        observe(&l, n, "hr__balance", true);
    }
    l
}
#[test]
fn discovers_nested_parameters_with_new_values_and_excludes_other_users() {
    let l = trained(LearningMode::Auto);
    let nomination = (0..5)
        .find_map(|_| l.nominate("alice", 1, &prior(&l, 77)))
        .unwrap();
    assert_eq!(nomination.arguments, json!({"employee":{"id":177}}));
    assert_eq!(nomination.tool, "hr__balance");
    assert!(l.nominate("bob", 1, &prior(&l, 77)).is_none());
    let report = l.report(1);
    assert_eq!(report["rules"][0]["confidence_percent"], 100.0);
    assert_eq!(report["rules"][0]["estimated_net_saving_ms"], 60.0);
    assert_eq!(
        report["rules"][0]["arguments"]["/employee/id"]["sources"][0]["pointer"],
        "/structuredContent/employee_id"
    );
    assert!(!report.to_string().contains("177"));
}
#[test]
fn approval_observation_rejection_and_holdouts_control_dispatch() {
    let l = trained(LearningMode::Approval);
    let id = l.report(1)["rules"][0]["id"].as_str().unwrap().to_string();
    assert!(l.nominate("alice", 1, &prior(&l, 4)).is_none());
    assert_eq!(l.report(1)["rules"][0]["status"], "pending_approval");
    l.decide(&id, true);
    assert_eq!(
        (4..9)
            .filter(|n| l.nominate("alice", 1, &prior(&l, *n)).is_some())
            .count(),
        4
    );
    l.mode(LearningMode::Observe);
    assert!(l.nominate("alice", 1, &prior(&l, 9)).is_none());
    l.mode(LearningMode::Auto);
    let trial = l.start(&id, 1).unwrap();
    l.decide(&id, false);
    assert!(trial.cancel.is_cancelled());
    assert!(l.nominate("alice", 1, &prior(&l, 10)).is_none());
    assert_eq!(l.report(1)["rules"][0]["status"], "rejected");
}
#[test]
fn coincidence_ambiguous_identity_and_changed_schema_fail_closed() {
    let l = learner(LearningMode::Auto);
    for _ in 0..4 {
        observe(&l, 1, "hr__balance", true);
    }
    assert!(
        l.nominate("alice", 1, &prior(&l, 2)).is_none(),
        "constant identity provided no provenance evidence"
    );
    let l = learner(LearningMode::Auto);
    for n in 1..=3 {
        let p = l
            .observation("hr__employee", &json!({"id":n}), &outcome(n))
            .unwrap();
        l.observe(
            "alice",
            1,
            &p,
            "hr__balance",
            &json!({"id":n}),
            &outcome(n),
            60_000.0,
            100_000.0,
            false,
            true,
        );
    }
    let changed = l
        .observation("hr__employee", &json!({"id":9}), &outcome(900))
        .unwrap();
    assert!(
        l.nominate("alice", 1, &changed).is_none(),
        "conflicting source pointers cannot guess an identity"
    );
    l.observe(
        "alice",
        1,
        &changed,
        "hr__balance",
        &json!({"id":900}),
        &outcome(900),
        60_000.0,
        100_000.0,
        false,
        true,
    );
    assert_eq!(
        (0..5)
            .find_map(|_| l.nominate("alice", 1, &changed))
            .unwrap()
            .arguments,
        json!({"id":900})
    );
    let mut changed_schema = changed;
    changed_schema.revision = "schema-2".into();
    assert!(l.nominate("alice", 1, &changed_schema).is_none());
    assert!(l.report(2)["rules"].as_array().unwrap().is_empty());
    observe(&l, 9, "hr__balance", true);
    assert_eq!(l.report(1)["generation"], 2);
    assert!(l.report(2)["rules"].as_array().unwrap().is_empty());
}
#[test]
fn misses_and_no_gap_prevent_activation_and_bad_trials_retire_then_relearn() {
    let l = trained(LearningMode::Auto);
    for n in 4..=6 {
        observe(&l, n, "hr__other", false);
    }
    assert!(l.nominate("alice", 1, &prior(&l, 7)).is_none());
    let l = learner(LearningMode::Auto);
    for n in 1..=4 {
        l.observe(
            "alice",
            1,
            &prior(&l, n),
            "hr__balance",
            &json!({"id":n+100}),
            &outcome(n),
            0.0,
            100_000.0,
            false,
            true,
        );
    }
    assert!(l.nominate("alice", 1, &prior(&l, 7)).is_none());
    let l = trained(LearningMode::Auto);
    let id = l.report(1)["rules"][0]["id"].as_str().unwrap().to_string();
    for _ in 0..3 {
        drop(l.start(&id, 1).unwrap());
    }
    assert_eq!(l.report(1)["rules"][0]["status"], "retired");
    assert_eq!(l.report(1)["rules"][0]["unused"], 3);
    for n in 4..8 {
        observe(&l, n, "hr__balance", true);
    }
    assert_eq!(l.report(1)["rules"][0]["status"], "active");
    for _ in 0..3 {
        let mut trial = l.start(&id, 1).unwrap();
        trial.finish(true, 120_000.0);
    }
    assert_eq!(l.report(1)["rules"][0]["retired_reason"], "no_wait_benefit");
    assert_eq!(l.report(1)["rules"][0]["recovery_observations_required"], 8);
    for n in 8..12 {
        observe(&l, n, "hr__balance", true);
    }
    assert_eq!(l.report(1)["rules"][0]["status"], "retired");
}
#[test]
fn context_budget_is_released_and_unsupported_arguments_are_not_guessed() {
    let l = learner(LearningMode::Auto);
    let p = prior(&l, 1);
    assert!(l.report(1)["context_bytes"].as_u64().unwrap() > 0);
    drop(p);
    assert_eq!(l.report(1)["context_bytes"], 0);
    assert!(l
        .observation(
            "hr__employee",
            &json!({"id":"x".repeat(CONTEXT_LIMIT)}),
            &outcome(1)
        )
        .is_none());
    l.observe(
        "alice",
        1,
        &prior(&l, 1),
        "hr__balance",
        &json!({"unbound":"unknown","fixed":true}),
        &outcome(1),
        60_000.0,
        100_000.0,
        false,
        true,
    );
    assert!(l.report(1)["rules"].as_array().unwrap().is_empty());
}

#[test]
fn rejected_candidates_release_capacity_without_losing_rejection() {
    let mut l = learner(LearningMode::Auto);
    Arc::get_mut(&mut l).unwrap().config.max_rules = 1;
    for n in 1..=3 {
        observe(&l, n, "hr__balance", true);
    }
    let id = l.report(1)["rules"][0]["id"].as_str().unwrap().to_string();
    l.decide(&id, true);
    observe(&l, 4, "hr__other", true);
    assert_eq!(l.report(1)["rules"][0]["id"], id);
    assert_eq!(l.report(1)["dropped_candidates"], 1);
    l.decide(&id, false);
    observe(&l, 5, "hr__other", true);
    assert_eq!(l.report(1)["rules"].as_array().unwrap().len(), 1);
    assert_eq!(l.report(1)["rules"][0]["tool"], "hr__other");
    l.report(2);
    let p = prior(&l, 6);
    l.observe(
        "alice",
        2,
        &p,
        "hr__balance",
        &json!({"employee":{"id":106}}),
        &outcome(106),
        60_000.0,
        100_000.0,
        false,
        true,
    );
    assert_eq!(l.report(2)["rules"][0]["id"], id);
    assert_eq!(l.report(2)["rules"][0]["status"], "rejected");
}

#[test]
fn late_outcomes_from_a_retired_trial_do_not_poison_recovery() {
    let l = trained(LearningMode::Auto);
    let id = l.report(1)["rules"][0]["id"].as_str().unwrap().to_string();
    let late = l.start(&id, 1).unwrap();
    for _ in 0..3 {
        drop(l.start(&id, 1).unwrap());
    }
    for n in 4..8 {
        observe(&l, n, "hr__balance", true);
    }
    assert_eq!(l.report(1)["rules"][0]["status"], "active");
    drop(late);
    assert_eq!(l.report(1)["rules"][0]["prediction_samples"], 0);
    assert_eq!(l.report(1)["rules"][0]["unused"], 4);
}

#[test]
fn late_outcomes_from_an_evicted_rule_do_not_poison_rediscovery() {
    let mut l = learner(LearningMode::Auto);
    Arc::get_mut(&mut l).unwrap().config.max_rules = 1;
    for n in 1..=3 {
        observe(&l, n, "hr__balance", true);
    }
    let id = l.report(1)["rules"][0]["id"].as_str().unwrap().to_string();
    let late = l.start(&id, 1).unwrap();
    for _ in 0..3 {
        drop(l.start(&id, 1).unwrap());
    }
    observe(&l, 4, "hr__other", true);
    let replacement = l.report(1)["rules"][0]["id"].as_str().unwrap().to_string();
    assert_ne!(id, replacement);
    l.decide(&replacement, false);
    for n in 5..=7 {
        observe(&l, n, "hr__balance", true);
    }
    assert_eq!(l.report(1)["rules"][0]["id"], id);
    assert_eq!(l.report(1)["rules"][0]["status"], "active");
    drop(late);
    assert_eq!(l.report(1)["rules"][0]["prediction_samples"], 0);
    assert_eq!(l.report(1)["rules"][0]["unused"], 0);
}

#[test]
fn unselected_cache_hits_reduce_benefit_but_predictions_do_not_train_the_baseline() {
    let l = trained(LearningMode::Auto);
    let id = l.report(1)["rules"][0]["id"].as_str().unwrap().to_string();
    let trial = l.start(&id, 1).unwrap();
    for selected in [true, false] {
        for n in 4..36 {
            let mut cached = outcome(n + 100);
            cached.disposition = "reused";
            l.observe(
                "alice",
                1,
                &prior(&l, n),
                "hr__balance",
                &json!({"employee":{"id":n+100}}),
                &cached,
                60_000.0,
                1_000.0,
                selected,
                true,
            );
        }
        let report = l.report(1);
        if selected {
            assert_eq!(report["rules"][0]["baseline_mean_ms"], 100.0);
            assert_eq!(report["rules"][0]["status"], "active");
        } else {
            assert_eq!(report["rules"][0]["baseline_mean_ms"], 1.0);
            assert_eq!(report["rules"][0]["retired_reason"], "low_expected_benefit");
            assert!(trial.cancel.is_cancelled());
        }
    }
}
