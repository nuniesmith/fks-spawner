// =============================================================================
// redact.rs — credentials in a saved config's env never leave in a read
//
// WHY: a saved config's `env` is stored as plain JSON and `GET /configs`
// returned it whole. On 2026-10-26 that included the live Kraken bot's
// CRYPTO_DISCORD_WEBHOOK_URL, token and all, to anything that could read the
// config list (the web UI's Bots page, and anyone with an API session). A
// Discord webhook URL is a credential: whoever holds it can post as the bot.
//
// THE RULE: a value is redacted when its NAME looks like a credential (a
// `_`-separated segment such as KEY, TOKEN, SECRET, PASSWORD or WEBHOOK) or its
// VALUE carries one (a URL with `user:pass@`, or a webhook path). Exchange keys
// are not in env at all (they are injected at spawn from the encrypted store);
// this covers what operators type into env.
//
// ROUND TRIPS: the web UI loads a config into its form and can save it back.
// A redacted value comes back as the marker, and `restore_redacted` puts the
// stored value back in its place, so editing a config never overwrites a
// secret with "<redacted>". A raw `POST /spawn` has no stored config to
// restore from, so a marker there is refused (`marker_keys`).
//
// False positives only hide a harmless value in reads; they never lose it.
// =============================================================================

use std::collections::HashMap;

/// What a redacted env value reads as.
pub const REDACTED: &str = "<redacted>";

/// Name segments that mark an env var as a credential. Matched against the
/// `_`/`-`/`.`-separated segments of the upper-cased name, so `API_KEY` and
/// `CRYPTO_DISCORD_WEBHOOK_URL` match while `MONKEY_COUNT` does not.
const SENSITIVE_SEGMENTS: &[&str] = &[
    "APIKEY",
    "AUTH",
    "COOKIE",
    "CREDENTIAL",
    "CREDENTIALS",
    "DSN",
    "KEY",
    "KEYS",
    "PASS",
    "PASSPHRASE",
    "PASSWD",
    "PASSWORD",
    "PRIVATE",
    "SECRET",
    "SECRETS",
    "SESSION",
    "SIGNATURE",
    "TOKEN",
    "TOKENS",
    "WEBHOOK",
    "WEBHOOKS",
];

/// Whether an env var's value must not be returned by a read.
pub fn is_sensitive(name: &str, value: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    upper
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|segment| SENSITIVE_SEGMENTS.contains(&segment))
        || value_carries_credentials(value)
}

/// A URL with credentials in its authority (`postgres://user:pass@host/db`),
/// or a webhook URL whose path is the token (Discord, Slack).
fn value_carries_credentials(value: &str) -> bool {
    let Some((scheme, rest)) = value.trim().split_once("://") else {
        return false;
    };
    let scheme_ok = !scheme.is_empty()
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    if !scheme_ok {
        return false;
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    authority.contains('@')
        || rest.contains("/api/webhooks/")
        || rest.starts_with("hooks.slack.com/")
}

/// `env` with every sensitive value replaced by [`REDACTED`], and the names
/// that were redacted, sorted.
pub fn redact_env(env: &HashMap<String, String>) -> (HashMap<String, String>, Vec<String>) {
    let mut names = Vec::new();
    let out = env
        .iter()
        .map(|(k, v)| {
            if is_sensitive(k, v) {
                names.push(k.clone());
                (k.clone(), REDACTED.to_string())
            } else {
                (k.clone(), v.clone())
            }
        })
        .collect();
    names.sort();
    (out, names)
}

/// The names whose value is the redaction marker.
pub fn marker_keys(env: &HashMap<String, String>) -> Vec<String> {
    let mut names: Vec<String> = env
        .iter()
        .filter(|(_, v)| v.as_str() == REDACTED)
        .map(|(k, _)| k.clone())
        .collect();
    names.sort();
    names
}

/// Put the stored value back wherever an incoming config carries the marker.
/// `Err` names the markers with no stored value to restore (a new key, or a
/// config that does not exist yet): those must be typed in, not saved as the
/// marker.
pub fn restore_redacted(
    incoming: &mut HashMap<String, String>,
    stored: Option<&HashMap<String, String>>,
) -> Result<(), Vec<String>> {
    let mut missing = Vec::new();
    for name in marker_keys(incoming) {
        match stored.and_then(|s| s.get(&name)) {
            Some(real) if real != REDACTED => {
                incoming.insert(name, real.clone());
            }
            _ => missing.push(name),
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // Built from parts so secret scanners (GitGuardian) do not read a test
    // fixture as a leaked credential.
    fn webhook() -> String {
        ["https://discord.com/api/", "webhooks", "/1/t"].concat()
    }
    fn slack_hook() -> String {
        ["https://hooks.", "slack.com/services/T0/B0/x"].concat()
    }
    fn url_with_userinfo(scheme: &str) -> String {
        [scheme, "://user", ":", "x", "@host:5432/db"].concat()
    }

    #[test]
    fn names_that_look_like_credentials_are_sensitive() {
        for name in [
            "API_KEY",
            "KRAKEN_API_SECRET",
            "JANUS_API_TOKEN",
            "CRYPTO_DISCORD_WEBHOOK_URL",
            "DB_PASSWORD",
            "KUCOIN_PASSPHRASE",
            "SENTRY_DSN",
            "spawner-secrets-key",
        ] {
            assert!(is_sensitive(name, "x"), "{name} should be sensitive");
        }
    }

    #[test]
    fn ordinary_names_and_values_are_not() {
        for (name, value) in [
            ("RECORDER_SYMBOLS", "BTCUSDT,ETHUSDT"),
            ("BOT_STATUS_PORT", "9091"),
            ("TZ", "America/Toronto"),
            ("MONKEY_COUNT", "3"),
            ("BYPASS_CACHE", "true"),
            ("JANUS_URL", "http://janus:8080/api"),
        ] {
            assert!(
                !is_sensitive(name, value),
                "{name}={value} should not be sensitive"
            );
        }
    }

    #[test]
    fn values_that_carry_credentials_are_sensitive_whatever_the_name() {
        assert!(is_sensitive("NOTIFY", &webhook()));
        assert!(is_sensitive("OUT", &slack_hook()));
        assert!(is_sensitive("STORE", &url_with_userinfo("postgres")));
        assert!(is_sensitive("PROXY", &url_with_userinfo("socks5h")));
        assert!(!is_sensitive("HOME_PAGE", "https://example.com/path?q=a@b"));
        assert!(!is_sensitive("NOTE", "mail me: a@b.example"));
    }

    #[test]
    fn redact_env_hides_values_and_names_them() {
        let hook = webhook();
        let (out, names) = redact_env(&env(&[
            ("CRYPTO_DISCORD_WEBHOOK_URL", &hook),
            ("TZ", "UTC"),
        ]));
        assert_eq!(out["CRYPTO_DISCORD_WEBHOOK_URL"], REDACTED);
        assert_eq!(out["TZ"], "UTC");
        assert_eq!(names, vec!["CRYPTO_DISCORD_WEBHOOK_URL".to_string()]);
    }

    #[test]
    fn restore_puts_stored_values_back_and_keeps_edits() {
        let hook = webhook();
        let stored = env(&[("HOOK_URL", &hook), ("TZ", "UTC")]);
        let mut incoming = env(&[("HOOK_URL", REDACTED), ("TZ", "America/Toronto")]);
        restore_redacted(&mut incoming, Some(&stored)).unwrap();
        assert_eq!(incoming["HOOK_URL"], hook);
        assert_eq!(
            incoming["TZ"], "America/Toronto",
            "an edited plain value is kept"
        );
    }

    #[test]
    fn restore_refuses_a_marker_with_nothing_behind_it() {
        let stored = env(&[("TZ", "UTC")]);
        let mut incoming = env(&[("NEW_TOKEN", REDACTED), ("TZ", "UTC")]);
        assert_eq!(
            restore_redacted(&mut incoming, Some(&stored)),
            Err(vec!["NEW_TOKEN".to_string()])
        );
        let mut fresh = env(&[("API_KEY", REDACTED)]);
        assert_eq!(
            restore_redacted(&mut fresh, None),
            Err(vec!["API_KEY".to_string()])
        );
    }

    #[test]
    fn marker_keys_lists_only_markers() {
        let e = env(&[("A_TOKEN", REDACTED), ("B", "plain"), ("C_KEY", REDACTED)]);
        assert_eq!(
            marker_keys(&e),
            vec!["A_TOKEN".to_string(), "C_KEY".to_string()]
        );
    }
}
