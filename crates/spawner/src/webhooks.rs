// =============================================================================
// webhooks.rs — a bot's own webhooks, from Settings → Notifications
//
// A saved config (or a spawn) may name notification channels for its env:
//
//     webhooks: { CRYPTO_DISCORD_WEBHOOK_URL: crypto }
//
// At every spawn the spawner decrypts channel `crypto`'s URL from the channel
// store (encrypted at rest with SPAWNER_SECRETS_KEY, never returned over HTTP)
// and sets it as CRYPTO_DISCORD_WEBHOOK_URL in the container's env, the way
// stored exchange keys are injected (`api::inject_secrets`). The URL is never
// in the saved config, in GitHub, or in the deploy's .env: rotating it is
// editing the channel in Settings and respawning the bot.
//
// Unlike a missing exchange key, a missing channel does NOT stop the spawn:
// a live trader must not stay down because its Discord reports can't be
// wired. The bot runs without that variable, and the spawner says so in its
// log and in `fks_spawner_webhook_unresolved_total{channel}`.
//
// A channel a bot uses this way is usually its own: set its "platform
// events" off in Settings so the dispatcher doesn't also send it every
// bot's lifecycle events (`notifications.rs`).
// =============================================================================

use std::collections::{BTreeMap, HashMap};

/// At most this many webhooks per bot.
pub const MAX_WEBHOOKS: usize = 10;

/// Env vars the spawner (or a job instance) sets itself.
const RESERVED: &[&str] = &[
    "FKS_BOT_ID",
    "FKS_BOT_MODE",
    "DATA_DIR",
    "LAKE_DIR",
    "FKS_DB_URL",
    "SPAWNER_EVENTS_URL",
    "SPAWNER_EVENTS_TOKEN",
];

fn is_env_name(s: &str) -> bool {
    let mut chars = s.chars();
    (1..=64).contains(&s.len())
        && chars.next().is_some_and(|c| c.is_ascii_uppercase())
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// A channel name as Settings stores it: trimmed, 1-64 characters.
fn is_channel_name(s: &str) -> bool {
    !s.trim().is_empty() && s.trim().len() <= 64 && s.trim() == s
}

/// Check a `webhooks` mapping against the config's own env. Every problem is
/// reported. A variable set both ways is refused: two sources for one value
/// is how a rotated webhook keeps the old one.
pub fn validate(
    webhooks: &BTreeMap<String, String>,
    env: &HashMap<String, String>,
) -> Result<(), String> {
    let mut errors = Vec::new();
    if webhooks.len() > MAX_WEBHOOKS {
        errors.push(format!("at most {MAX_WEBHOOKS} webhooks per bot"));
    }
    for (var, channel) in webhooks {
        if !is_env_name(var) {
            errors.push(format!(
                "webhooks: {var:?} isn't an env var name (A-Z, 0-9 and _, starting with a letter)"
            ));
        } else if RESERVED.contains(&var.as_str()) {
            errors.push(format!("webhooks: {var} is set by the spawner"));
        } else if env.contains_key(var) {
            errors.push(format!(
                "{var} is set both in env and in webhooks; keep the webhook in Settings and \
                 take it out of env"
            ));
        }
        if !is_channel_name(channel) {
            errors.push(format!(
                "webhooks: {var} names channel {channel:?}, which isn't a channel name"
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

/// Resolve each webhook from the channel store into `env`. Never fails: an
/// unknown channel or an undecryptable one is skipped, logged and counted,
/// so the bot still starts. Returns the variables it couldn't set.
#[cfg(feature = "db")]
pub async fn inject(
    store: Option<&crate::db::BotRunStore>,
    webhooks: &BTreeMap<String, String>,
    env: &mut HashMap<String, String>,
) -> Vec<String> {
    let mut unresolved = Vec::new();
    for (var, channel) in webhooks {
        let resolved = match store {
            Some(store) => store.get_channel_target(channel).await,
            None => Ok(None),
        };
        match resolved {
            Ok(Some(url)) => {
                env.insert(var.clone(), url);
                // The channel's name only; never the URL.
                tracing::info!(var = %var, channel = %channel, "webhook set from Settings");
            }
            Ok(None) => {
                tracing::warn!(
                    var = %var,
                    channel = %channel,
                    "no notification channel by that name (Settings → Notifications); \
                     the bot starts without this webhook"
                );
                crate::metrics::WEBHOOK_UNRESOLVED_TOTAL
                    .with_label_values(&[channel])
                    .inc();
                unresolved.push(var.clone());
            }
            Err(e) => {
                tracing::warn!(
                    var = %var,
                    channel = %channel,
                    error = %e,
                    "couldn't read the channel's webhook; the bot starts without it"
                );
                crate::metrics::WEBHOOK_UNRESOLVED_TOTAL
                    .with_label_values(&[channel])
                    .inc();
                unresolved.push(var.clone());
            }
        }
    }
    unresolved
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect()
    }

    #[test]
    fn a_good_mapping_passes() {
        let env = HashMap::from([("TZ".to_string(), "UTC".to_string())]);
        assert!(validate(&map(&[("CRYPTO_DISCORD_WEBHOOK_URL", "crypto")]), &env).is_ok());
        assert!(validate(&BTreeMap::new(), &env).is_ok());
    }

    #[test]
    fn every_problem_is_reported() {
        let env = HashMap::from([("CRYPTO_DISCORD_WEBHOOK_URL".to_string(), "old".to_string())]);
        let err = validate(
            &map(&[
                ("CRYPTO_DISCORD_WEBHOOK_URL", "crypto"),
                ("lower_case", "crypto"),
                ("FKS_BOT_ID", "crypto"),
                ("REPORTS", " padded "),
            ]),
            &env,
        )
        .unwrap_err();
        for needle in [
            "set both in env and in webhooks",
            "\"lower_case\" isn't an env var name",
            "FKS_BOT_ID is set by the spawner",
            "isn't a channel name",
        ] {
            assert!(err.contains(needle), "missing {needle:?} in {err}");
        }
    }

    #[test]
    fn too_many_are_refused() {
        let many: BTreeMap<String, String> = (0..11)
            .map(|i| (format!("HOOK_{i}"), "crypto".to_string()))
            .collect();
        assert!(
            validate(&many, &HashMap::new())
                .unwrap_err()
                .contains("at most 10")
        );
    }
}
