// =============================================================================
// task_supervisor.rs — lightweight supervision for the spawner's bare
// `tokio::spawn` critical loops.
//
// Several always-on background loops (the net-worth sampler, the crash-aware
// prune supervisor) are launched from `main` as detached `tokio::spawn(...)`
// handles that are only ever dropped at process exit. A panic or an unexpected
// early return inside one of them ends the task PERMANENTLY and SILENTLY, with
// the parent still reporting healthy — so a panicked sampler stops pruning
// `notification_log` forever (unbounded growth) and a dead supervisor silently
// ends all crash paging, until a container bounce.
//
// [`supervise`] closes that gap without changing a task's happy-path behaviour.
// It runs the task inside an isolated child spawn and, if the child panics or
// returns before shutdown was requested, re-spawns it with capped exponential
// backoff. The factory is re-invoked on every restart, so any per-run setup (a
// fresh store/pool handle, a reset counter, …) is re-established cleanly.
//
// Backoff is capped (default 60s) so a hot-looping panic cannot spin the CPU,
// and every respawn is logged for visibility. Giving up permanently is never
// the desired outcome for these money-adjacent loops, so there is no circuit
// breaker — restarts continue indefinitely at the capped cadence. Mirrors the
// janus `supervisor/respawn.rs` idiom.
// =============================================================================

use std::future::Future;
use std::time::{Duration, Instant};

use tracing::{error, info, warn};

/// First restart delay; doubles up to [`MAX_BACKOFF`].
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Ceiling on the restart delay — caps the restart rate of a persistently
/// failing task so it cannot busy-loop.
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// A task that ran healthy at least this long before dying has its backoff
/// reset to [`INITIAL_BACKOFF`] — a single hiccup after hours of uptime should
/// not inherit a maxed-out delay.
const HEALTHY_RESET: Duration = Duration::from_secs(60);

/// Supervise a critical task until `is_shutdown` returns `true`.
///
/// `factory` is called to produce the task future for each run; it is
/// re-invoked on every restart so the task re-acquires its handles fresh. The
/// future is driven inside an isolated [`tokio::spawn`], so a panic is caught
/// (as a [`JoinError`](tokio::task::JoinError)) rather than unwinding the
/// supervisor.
///
/// Restart triggers: the task **panics**, or it **returns** while shutdown has
/// not been requested. The loop stops when `is_shutdown()` is true, or when the
/// child was aborted/cancelled (the shutdown path).
pub async fn supervise<F, Fut>(name: &str, is_shutdown: impl Fn() -> bool, factory: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    supervise_with_backoff(name, is_shutdown, INITIAL_BACKOFF, MAX_BACKOFF, factory).await
}

/// [`supervise`] with an explicit backoff floor/ceiling (used by tests that
/// want a tight restart cadence).
pub async fn supervise_with_backoff<F, Fut>(
    name: &str,
    is_shutdown: impl Fn() -> bool,
    initial_backoff: Duration,
    max_backoff: Duration,
    mut factory: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut backoff = initial_backoff;

    loop {
        if is_shutdown() {
            break;
        }

        let started = Instant::now();
        let handle = tokio::spawn(factory());

        match handle.await {
            Ok(()) => {
                if is_shutdown() {
                    break;
                }
                warn!(
                    task = name,
                    "supervised task returned unexpectedly before shutdown; restarting"
                );
            }
            Err(join_err) if join_err.is_panic() => {
                if is_shutdown() {
                    break;
                }
                error!(task = name, error = %join_err, "supervised task PANICKED; restarting");
            }
            Err(_cancelled) => {
                // Aborted — only happens on the shutdown path; stop supervising.
                break;
            }
        }

        // A task that stayed up a healthy while resets the backoff; a hot-loop
        // keeps climbing toward the cap.
        if started.elapsed() >= HEALTHY_RESET {
            backoff = initial_backoff;
        }

        warn!(
            task = name,
            backoff_ms = backoff.as_millis() as u64,
            "respawning supervised task after backoff"
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }

    info!(task = name, "supervised task stopped (shutdown)");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    fn fast() -> (Duration, Duration) {
        (Duration::from_millis(1), Duration::from_millis(5))
    }

    #[tokio::test]
    async fn respawns_until_a_run_succeeds() {
        // Fails (panics) 3 times, then the 4th run succeeds and requests
        // shutdown — proving the task was respawned after each death rather
        // than dying permanently on the first panic.
        let runs = Arc::new(AtomicU32::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        let runs_f = runs.clone();
        let shutdown_f = shutdown.clone();
        let factory = move || {
            let runs = runs_f.clone();
            let shutdown = shutdown_f.clone();
            async move {
                let n = runs.fetch_add(1, Ordering::SeqCst) + 1;
                if n >= 4 {
                    shutdown.store(true, Ordering::SeqCst);
                    return;
                }
                panic!("simulated task death #{n}");
            }
        };

        let (lo, hi) = fast();
        let shutdown_check = shutdown.clone();
        supervise_with_backoff(
            "test-respawn",
            move || shutdown_check.load(Ordering::SeqCst),
            lo,
            hi,
            factory,
        )
        .await;

        assert_eq!(runs.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn respawns_after_early_return() {
        let runs = Arc::new(AtomicU32::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        let runs_f = runs.clone();
        let shutdown_f = shutdown.clone();
        let factory = move || {
            let runs = runs_f.clone();
            let shutdown = shutdown_f.clone();
            async move {
                let n = runs.fetch_add(1, Ordering::SeqCst) + 1;
                if n >= 3 {
                    shutdown.store(true, Ordering::SeqCst);
                }
                // Returns immediately every run (the "gave up and returned"
                // mode) — supervisor must re-spawn it.
            }
        };

        let (lo, hi) = fast();
        let shutdown_check = shutdown.clone();
        supervise_with_backoff(
            "test-early-return",
            move || shutdown_check.load(Ordering::SeqCst),
            lo,
            hi,
            factory,
        )
        .await;

        assert_eq!(runs.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn does_not_start_when_already_shutdown() {
        let runs = Arc::new(AtomicU32::new(0));
        let runs_f = runs.clone();
        let factory = move || {
            let runs = runs_f.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
            }
        };

        let (lo, hi) = fast();
        supervise_with_backoff("test-noop", || true, lo, hi, factory).await;
        assert_eq!(runs.load(Ordering::SeqCst), 0);
    }
}
