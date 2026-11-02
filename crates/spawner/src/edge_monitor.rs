//! Delivers what the edge monitor did (EDGE_PROCESS §7).
//!
//! The monitor itself lives in Postgres (fks `030_edge_monitor.sql`): after
//! every session the forward runner records, the database judges the
//! deployment against its experiment's holdout (the forward band, the
//! CUSUM) and on a breach retires it in the same transaction, leaving an
//! `edge_monitor_events` row. Nothing here decides anything. This loop
//! turns each undelivered row into an `edge_decay` notification through
//! the ordinary dispatcher (channels, filters, the delivery ledger), then
//! sets its `delivered_at`.
//!
//! The runner can't send it: `POST /events` takes the internal token, which
//! a research instance never holds.
//!
//! At-least-once: a row is marked only after its dispatch returns, so a
//! spawner that dies in between sends it again on the next start. Rows wait
//! while notifications are off (`NOTIFY_ENABLED=false`) rather than being
//! marked unsent-but-done.

use std::time::Duration;

use tracing::{info, warn};

use crate::api::AppState;
use crate::notifications::{EVENT_EDGE_DECAY, NotificationDispatcher, NotificationEvent};

/// How often undelivered monitor events are looked for.
pub const POLL: Duration = Duration::from_secs(60);

/// One undelivered `edge_monitor_events` row.
#[derive(Debug, Clone)]
pub struct MonitorEvent {
    pub id: i64,
    pub deployment_id: i64,
    pub kind: String,
    pub reason: String,
    pub symbol: String,
}

/// The notification for one monitor event. The bot handle names the
/// deployment (the Edges board's id), the detail is the database's reason.
pub fn notification(ev: &MonitorEvent) -> NotificationEvent {
    NotificationEvent::from_ingest(
        EVENT_EDGE_DECAY,
        &format!("deployment-{}", ev.deployment_id),
        "",
        &format!(
            "{} {} by the edge monitor: {}",
            ev.symbol, ev.kind, ev.reason
        ),
    )
}

/// Run forever: every [`POLL`], deliver what the monitor left. Never panics;
/// a failed pass is a warn and the next one retries.
pub async fn run(state: AppState) {
    loop {
        tokio::time::sleep(POLL).await;
        tick(&state).await;
    }
}

async fn tick(state: &AppState) {
    if !state.config.notify_enabled {
        return;
    }
    let Some(store) = state.store.as_ref() else {
        return;
    };
    let events = match store.undelivered_monitor_events().await {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "edge monitor: reading undelivered events failed");
            return;
        }
    };
    for ev in events {
        NotificationDispatcher::new(store.clone())
            .dispatch(notification(&ev))
            .await;
        match store.mark_monitor_event_delivered(ev.id).await {
            Ok(()) => info!(
                event_id = ev.id,
                deployment_id = ev.deployment_id,
                "edge monitor: delivered"
            ),
            Err(e) => warn!(error = %e, event_id = ev.id, "edge monitor: marking delivered failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retirement_reads_as_edge_decay_naming_the_deployment() {
        let n = notification(&MonitorEvent {
            id: 1,
            deployment_id: 7,
            kind: "retired".into(),
            reason: "monitor (v): the CUSUM passed 7.3".into(),
            symbol: "MES+MNQ".into(),
        });
        assert_eq!(n.event, EVENT_EDGE_DECAY);
        assert_eq!(n.bot_id, "deployment-7");
        let detail = n.detail.unwrap();
        assert!(
            detail.starts_with("MES+MNQ retired by the edge monitor: monitor (v)"),
            "{detail}"
        );
    }
}
