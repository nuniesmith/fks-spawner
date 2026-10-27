// =============================================================================
// ladder.rs — the edge ladder: hypotheses, experiments, deployments
//
// EDGE_PROCESS_2026-10-26 (fks-governance) is the process this serves: every
// edge climbs one ladder, capture → hypothesis → search → validate → forward
// → live → monitor, and a rung is passed by evidence the platform records,
// never by a decision made in chat. Postgres enforces the rungs (fks
// src/sql/spawner): 018 and 020 guard `edge_deployments` (forward needs a
// USABLE verdict and a session count declared in advance, then frozen; live
// needs the sessions observed; retired is terminal), and 024 freezes a
// registered hypothesis and refuses an experiment that finished before it.
//
// This module validates requests before they reach the database and turns
// the database's refusals into answers: a guard's RAISE EXCEPTION reaches the
// operator VERBATIM as a 409 (`SpawnerError::Refused`), because the trigger's
// message ("backtest->forward requires the experiment verdict to be USABLE
// (experiment 3 is DO NOT PASTE)") is the explanation. Nothing here re-checks
// a rung the database already guards: two copies of a rule drift, and the
// one in the database is the one no code path can skip.
//
// The routes (api.rs): GET /ladder (the whole board in one read),
// POST /edges/{id}/hypotheses, POST /hypotheses/{id}/withdraw,
// POST /deployments, POST /deployments/{id}/promote,
// POST /deployments/{id}/retire. The SQL is in db.rs (`BotRunStore`).
// =============================================================================

use serde::Deserialize;
use serde_json::Value;

/// Parameters a hypothesis may tune. EDGE_PROCESS §2.3: keep them few; the
/// 2026-09 Pine optimizer tuned 64 against ~13 trades and its winner took
/// zero trades on the holdout. This is a sanity cap, not the advice.
pub const MAX_PARAMS: usize = 12;
/// Grid points per numeric parameter, so a typo'd step can't ask for a
/// billion-point search.
pub const MAX_GRID_POINTS: f64 = 10_000.0;

/// `POST /edges/{id}/hypotheses`: stage 2, written before the search.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HypothesisRequest {
    /// The idea in one sentence, with the mechanism.
    pub idea: String,
    pub symbols: Vec<String>,
    /// The data in words (lake folder and date range) until dataset
    /// manifests exist (plan 4.1/4.3).
    pub dataset: String,
    /// The last share of the window nothing touches until the holdout is
    /// scored once.
    pub holdout_share: f64,
    pub params: Vec<ParamSpec>,
    /// What the edge must beat on the same holdout.
    pub baseline: String,
    /// The score, net of `costs`.
    pub metric: String,
    pub costs: Costs,
    /// Who registered it. The web UI's adapter sets this from the session;
    /// whatever a browser sent is overwritten there.
    #[serde(default)]
    pub registered_by: Option<String>,
}

/// One tuned parameter: a numeric range (`min`, `max`, optional `step`) or a
/// list of `choices`, not both.
#[derive(Debug, Clone, Deserialize, serde::Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ParamSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub choices: Option<Vec<Value>>,
}

/// Costs in basis points: Kraken tier 1 is 40 maker, 80 taker
/// (EDGE_PROCESS §2.5), plus a stated slippage.
#[derive(Debug, Clone, Deserialize, serde::Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Costs {
    pub maker_bps: f64,
    pub taker_bps: f64,
    pub slippage_bps: f64,
}

/// A hypothesis that passed validation, normalized for the INSERT.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidHypothesis {
    pub idea: String,
    pub symbols: Vec<String>,
    pub dataset: String,
    pub holdout_share: f64,
    pub params: Value,
    pub baseline: String,
    pub metric: String,
    pub costs: Value,
    pub registered_by: Option<String>,
}

fn is_symbol(s: &str) -> bool {
    (2..=20).contains(&s.len())
        && s.chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

fn is_param_name(s: &str) -> bool {
    let mut chars = s.chars();
    (1..=40).contains(&s.len())
        && chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Text that must be there, within a length.
fn text(field: &str, raw: &str, min: usize, max: usize, errors: &mut Vec<String>) -> String {
    let t = raw.trim().to_string();
    let n = t.chars().count();
    if n < min {
        errors.push(if min <= 1 {
            format!("{field} is required")
        } else {
            format!("{field} needs at least {min} characters (it has {n})")
        });
    } else if n > max {
        errors.push(format!(
            "{field} takes at most {max} characters (it has {n})"
        ));
    }
    t
}

fn check_param(i: usize, p: &ParamSpec, errors: &mut Vec<String>) {
    let at = format!("params[{}] {:?}", i + 1, p.name);
    if !is_param_name(&p.name) {
        errors.push(format!(
            "{at}: a name is 1-40 lower-case letters, digits and '_', starting with a letter"
        ));
    }
    let numeric = p.min.is_some() || p.max.is_some() || p.step.is_some();
    match (&p.choices, numeric) {
        (Some(_), true) => errors.push(format!(
            "{at}: give a range (min, max, step) or choices, not both"
        )),
        (None, false) => errors.push(format!("{at}: give a range (min, max, step) or choices")),
        (Some(choices), false) => {
            if choices.is_empty() || choices.len() > 50 {
                errors.push(format!("{at}: choices takes 1 to 50 values"));
            }
            if choices
                .iter()
                .any(|c| !(c.is_string() || c.is_number() || c.is_boolean()))
            {
                errors.push(format!(
                    "{at}: each choice is a string, number or true/false"
                ));
            }
        }
        (None, true) => {
            let (Some(min), Some(max)) = (p.min, p.max) else {
                errors.push(format!("{at}: a range needs both min and max"));
                return;
            };
            if !(min.is_finite() && max.is_finite()) || min >= max {
                errors.push(format!("{at}: min must be below max"));
                return;
            }
            if let Some(step) = p.step {
                if !(step.is_finite() && step > 0.0) {
                    errors.push(format!("{at}: step must be positive"));
                } else if (max - min) / step > MAX_GRID_POINTS {
                    errors.push(format!(
                        "{at}: that step makes more than {MAX_GRID_POINTS} grid points"
                    ));
                }
            }
        }
    }
}

/// Validate a registration. Every problem is reported, not just the first,
/// so the form can show them all at once.
pub fn validate_hypothesis(req: &HypothesisRequest) -> Result<ValidHypothesis, Vec<String>> {
    let mut errors = Vec::new();
    let idea = text("idea", &req.idea, 20, 1000, &mut errors);
    let dataset = text("dataset", &req.dataset, 1, 500, &mut errors);
    let baseline = text("baseline", &req.baseline, 1, 200, &mut errors);
    let metric = text("metric", &req.metric, 1, 200, &mut errors);

    let mut symbols: Vec<String> = Vec::new();
    for s in &req.symbols {
        let s = s.trim().to_ascii_uppercase();
        if !is_symbol(&s) {
            errors.push(format!(
                "symbol {s:?} isn't a symbol (A-Z and 0-9, 2-20 long, e.g. BTCUSDT)"
            ));
        } else if !symbols.contains(&s) {
            symbols.push(s);
        }
    }
    if req.symbols.is_empty() {
        errors.push("symbols: name at least one".into());
    } else if symbols.len() > 50 {
        errors.push("symbols: at most 50".into());
    }

    if !(req.holdout_share.is_finite() && (0.1..=0.5).contains(&req.holdout_share)) {
        errors
            .push("holdout_share must be between 0.1 and 0.5 (EDGE_PROCESS proposes 0.25)".into());
    }

    if req.params.is_empty() {
        errors.push("params: a hypothesis tunes at least one parameter".into());
    } else if req.params.len() > MAX_PARAMS {
        errors.push(format!(
            "params: {} is more than {MAX_PARAMS}; keep them few (EDGE_PROCESS §2.3)",
            req.params.len()
        ));
    }
    for (i, p) in req.params.iter().enumerate() {
        check_param(i, p, &mut errors);
        if req.params[..i].iter().any(|q| q.name == p.name) {
            errors.push(format!("params: {:?} is listed twice", p.name));
        }
    }

    let c = &req.costs;
    for (name, v) in [
        ("maker_bps", c.maker_bps),
        ("taker_bps", c.taker_bps),
        ("slippage_bps", c.slippage_bps),
    ] {
        if !(v.is_finite() && (0.0..=1000.0).contains(&v)) {
            errors.push(format!("costs.{name} must be between 0 and 1000 bps"));
        }
    }

    let registered_by = req
        .registered_by
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(100).collect::<String>());

    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(ValidHypothesis {
        idea,
        symbols,
        dataset,
        holdout_share: req.holdout_share,
        params: serde_json::to_value(&req.params).unwrap_or(Value::Null),
        baseline,
        metric,
        costs: serde_json::to_value(&req.costs).unwrap_or(Value::Null),
        registered_by,
    })
}

/// `POST /hypotheses/{id}/withdraw` and `POST /deployments/{id}/retire`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReasonRequest {
    pub reason: String,
}

/// A reason, required and kept: "a demotion nobody can explain later is a
/// demotion that teaches nothing" (018).
pub fn validate_reason(req: &ReasonRequest) -> Result<String, Vec<String>> {
    let mut errors = Vec::new();
    let reason = text("reason", &req.reason, 3, 500, &mut errors);
    if errors.is_empty() {
        Ok(reason)
    } else {
        Err(errors)
    }
}

/// `POST /deployments`: a deployment row at `backtest`, for an experiment.
/// The database refuses anything but `backtest` on insert, and `auto` on a
/// prop account.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentRequest {
    pub experiment_id: i64,
    /// An `accounts` row (007).
    pub account_id: String,
    pub symbol: String,
    /// The parameters actually traded, frozen with the row: "the object being
    /// traded and the object that was tested must be provably the same".
    pub params_snapshot: Value,
    /// `manual` (default) or `auto`.
    #[serde(default)]
    pub execution: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidDeployment {
    pub experiment_id: i64,
    pub account_id: String,
    pub symbol: String,
    pub params_snapshot: Value,
    pub execution: String,
}

pub fn validate_deployment(req: &DeploymentRequest) -> Result<ValidDeployment, Vec<String>> {
    let mut errors = Vec::new();
    if req.experiment_id <= 0 {
        errors.push("experiment_id must name an experiment".into());
    }
    let account_id = text("account_id", &req.account_id, 1, 100, &mut errors);
    let symbol = req.symbol.trim().to_ascii_uppercase();
    if !is_symbol(&symbol) {
        errors.push(format!(
            "symbol {symbol:?} isn't a symbol (A-Z and 0-9, 2-20 long)"
        ));
    }
    if !req.params_snapshot.is_object() {
        errors.push(
            "params_snapshot must be a JSON object: the parameters this deployment trades".into(),
        );
    }
    let execution = req
        .execution
        .as_deref()
        .unwrap_or("manual")
        .trim()
        .to_string();
    if execution != "manual" && execution != "auto" {
        errors.push(format!(
            "execution must be manual or auto, not {execution:?}"
        ));
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(ValidDeployment {
        experiment_id: req.experiment_id,
        account_id,
        symbol,
        params_snapshot: req.params_snapshot.clone(),
        execution,
    })
}

/// `POST /deployments/{id}/promote`: one rung up. `forward` needs the session
/// count declared now (it is frozen from then on); `live` needs nothing here,
/// and the database checks the sessions observed.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromoteRequest {
    pub to: String,
    #[serde(default)]
    pub forward_sessions_required: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Promotion {
    Forward { sessions_required: i32 },
    Live,
}

pub fn validate_promotion(req: &PromoteRequest) -> Result<Promotion, Vec<String>> {
    match (req.to.trim(), req.forward_sessions_required) {
        ("forward", Some(n)) if (1..=1000).contains(&n) => Ok(Promotion::Forward { sessions_required: n }),
        ("forward", Some(_)) => Err(vec!["forward_sessions_required must be between 1 and 1000".into()]),
        ("forward", None) => Err(vec![
            "forward needs forward_sessions_required, declared now: it is frozen once the forward test starts"
                .into(),
        ]),
        ("live", None) => Ok(Promotion::Live),
        ("live", Some(_)) => Err(vec![
            "forward_sessions_required was declared when the forward test started and can't change".into(),
        ]),
        (to, _) => Err(vec![format!(
            "to must be forward or live, not {to:?} (retire has its own route)"
        )]),
    }
}

/// The database's refusal, if this is one: a guard trigger's RAISE
/// EXCEPTION (check_violation), a CHECK constraint, a foreign key or a
/// unique index (one live deployment per account and symbol). Its message is
/// the explanation, so it is passed on verbatim.
#[cfg(feature = "db")]
pub fn refusal(e: &sqlx::Error) -> Option<String> {
    let db = e.as_database_error()?;
    let code = db.code()?;
    if !matches!(code.as_ref(), "23514" | "23503" | "23505" | "23502") {
        return None;
    }
    let message = db.message().to_string();
    // A foreign key's message names the constraint; its detail names the
    // missing row ("Key (experiment_id)=(99) is not present ..."), which is
    // the part the operator needs.
    match db
        .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
        .and_then(|pg| pg.detail())
    {
        Some(detail) => Some(format!("{message}: {detail}")),
        None => Some(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hypothesis() -> HypothesisRequest {
        serde_json::from_value(json!({
            "idea": "Liquidation clusters overshoot; fading the 5-minute move earns the snap-back.",
            "symbols": ["btcusdt", "BTCUSDT", "ETHUSDT"],
            "dataset": "lake raw/binance/usdm/btc, 2026-10-27..2027-01-27",
            "holdout_share": 0.25,
            "params": [
                {"name": "threshold_usd", "min": 1000000, "max": 20000000, "step": 1000000},
                {"name": "side", "choices": ["long", "short"]}
            ],
            "baseline": "buy-and-hold",
            "metric": "net return after fees",
            "costs": {"maker_bps": 40, "taker_bps": 80, "slippage_bps": 5},
            "registered_by": "  jordan  "
        }))
        .unwrap()
    }

    #[test]
    fn a_good_hypothesis_is_normalized() {
        let h = validate_hypothesis(&hypothesis()).unwrap();
        assert_eq!(h.symbols, vec!["BTCUSDT", "ETHUSDT"]);
        assert_eq!(h.registered_by.as_deref(), Some("jordan"));
        assert_eq!(h.params[0]["name"], "threshold_usd");
        assert!(
            h.params[1].get("min").is_none(),
            "unset fields are not stored: {}",
            h.params[1]
        );
        assert_eq!(
            h.costs,
            json!({"maker_bps": 40.0, "taker_bps": 80.0, "slippage_bps": 5.0})
        );
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let mut r = hypothesis();
        r.idea = "it goes up".into();
        r.symbols = vec!["BTC-USDT".into()];
        r.holdout_share = 0.9;
        r.params = vec![
            ParamSpec {
                name: "Lookback".into(),
                min: Some(5.0),
                max: Some(1.0),
                step: None,
                choices: None,
            },
            ParamSpec {
                name: "side".into(),
                min: Some(0.0),
                max: Some(1.0),
                step: None,
                choices: Some(vec![json!("x")]),
            },
            ParamSpec {
                name: "side".into(),
                min: None,
                max: None,
                step: None,
                choices: None,
            },
        ];
        r.costs.taker_bps = -1.0;
        let errors = validate_hypothesis(&r).unwrap_err();
        let all = errors.join("\n");
        for needle in [
            "idea needs at least 20 characters",
            "\"BTC-USDT\" isn't a symbol",
            "holdout_share must be between 0.1 and 0.5",
            "starting with a letter",
            "min must be below max",
            "not both",
            "give a range (min, max, step) or choices",
            "\"side\" is listed twice",
            "costs.taker_bps",
        ] {
            assert!(all.contains(needle), "missing {needle:?} in:\n{all}");
        }
    }

    #[test]
    fn too_many_parameters_or_grid_points_are_refused() {
        let mut r = hypothesis();
        r.params = (0..13)
            .map(|i| ParamSpec {
                name: format!("p{i}"),
                min: Some(0.0),
                max: Some(1.0),
                step: None,
                choices: None,
            })
            .collect();
        assert!(
            validate_hypothesis(&r)
                .unwrap_err()
                .join(" ")
                .contains("keep them few")
        );
        r.params = vec![ParamSpec {
            name: "x".into(),
            min: Some(0.0),
            max: Some(1.0),
            step: Some(1e-9),
            choices: None,
        }];
        assert!(
            validate_hypothesis(&r)
                .unwrap_err()
                .join(" ")
                .contains("grid points")
        );
    }

    #[test]
    fn unknown_fields_are_refused_so_a_typo_is_not_silently_dropped() {
        let mut v = serde_json::to_value(json!({
            "idea": "x", "symbols": [], "dataset": "", "holdout_share": 0.25, "params": [],
            "baseline": "", "metric": "", "costs": {"maker_bps": 0, "taker_bps": 0, "slippage_bps": 0}
        }))
        .unwrap();
        v["registered_at"] = json!("2020-01-01T00:00:00Z");
        let err = serde_json::from_value::<HypothesisRequest>(v)
            .unwrap_err()
            .to_string();
        assert!(err.contains("registered_at"), "{err}");
    }

    #[test]
    fn promotions_need_the_right_fields() {
        let p = |to: &str, n: Option<i32>| {
            validate_promotion(&PromoteRequest {
                to: to.into(),
                forward_sessions_required: n,
            })
        };
        assert_eq!(
            p("forward", Some(20)),
            Ok(Promotion::Forward {
                sessions_required: 20
            })
        );
        assert!(p("forward", None).unwrap_err()[0].contains("declared now"));
        assert!(p("forward", Some(0)).is_err());
        assert_eq!(p("live", None), Ok(Promotion::Live));
        assert!(p("live", Some(5)).unwrap_err()[0].contains("can't change"));
        assert!(p("retired", None).unwrap_err()[0].contains("retire has its own route"));
    }

    #[test]
    fn deployments_and_reasons() {
        let req = DeploymentRequest {
            experiment_id: 3,
            account_id: " kraken-spot ".into(),
            symbol: "btcusd".into(),
            params_snapshot: json!({"band": 0.05}),
            execution: None,
        };
        let d = validate_deployment(&req).unwrap();
        assert_eq!(
            (
                d.account_id.as_str(),
                d.symbol.as_str(),
                d.execution.as_str()
            ),
            ("kraken-spot", "BTCUSD", "manual")
        );
        let bad = DeploymentRequest {
            params_snapshot: json!([1]),
            execution: Some("yolo".into()),
            ..req
        };
        let errors = validate_deployment(&bad).unwrap_err().join("\n");
        assert!(
            errors.contains("params_snapshot") && errors.contains("yolo"),
            "{errors}"
        );
        assert!(
            validate_reason(&ReasonRequest {
                reason: " x ".into()
            })
            .is_err()
        );
        assert_eq!(
            validate_reason(&ReasonRequest {
                reason: " dataset hole ".into()
            })
            .unwrap(),
            "dataset hole"
        );
    }
}
