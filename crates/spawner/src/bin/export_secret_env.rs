//! export-secret-env — decrypt one `exchange_secrets` row and write it as
//! `.env`-shaped lines to a file, for services that are NOT spawner-managed
//! bots and therefore never go through [`spawner::api::inject_secrets`].
//!
//! # Why this exists
//!
//! Every credential this platform stores goes through the webui into
//! `exchange_secrets`, encrypted at rest under `SPAWNER_SECRETS_KEY`. For a
//! spawner-managed bot (Kraken/KuCoin/Crypto.com spot-portfolio), that's the
//! whole story: `POST /spawn` and `POST /configs/{name}/respawn` call
//! [`spawner::db::BotRunStore::get_secret`] and inject the decrypted value
//! straight into the new container's environment.
//!
//! `rithmic-connector` is not a bot in that sense — it's a plain
//! `docker-compose.yml` service, gated on `RITHMIC_ENABLED` + the `rithmic`
//! profile, with no spawner lifecycle around it. Nothing bridged
//! `exchange_secrets` to its `RITHMIC_USER`/`RITHMIC_PASSWORD` env vars, so a
//! credential entered through the webui (the only sanctioned input path —
//! see the settings page's `testable: false` Rithmic entry) had nowhere to
//! go. This binary is that bridge, built generic rather than Rithmic-specific
//! because the next non-bot integration will hit the identical gap.
//!
//! # Security posture
//!
//! - Runs in the SAME trust boundary as the spawner itself: same
//!   `SPAWNER_DATABASE_URL`/`DATABASE_URL`, same `SPAWNER_SECRETS_KEY`. It
//!   grants no new access — it is the existing `get_secret` path, exposed as
//!   a one-shot CLI instead of an HTTP handler, specifically so no route ever
//!   returns a decrypted value (the platform's standing invariant).
//! - Never logs a value. Only exchange name, variable names, and whether each
//!   field was present.
//! - Writes via temp-file + rename so there is no window where a partial file
//!   is readable at the final path, and chmod's the temp file to 0600 BEFORE
//!   the rename (not after), so it is never briefly world-readable.
//! - Fails loudly (non-zero exit, no file written) on any error — a missing
//!   row, an unset `SPAWNER_SECRETS_KEY`, a bad DB connection. The intended
//!   caller is a compose one-shot service gated with
//!   `depends_on: condition: service_completed_successfully`, so a failure
//!   here must prevent the credentialed service from starting with an empty
//!   or stale secrets file — starting silently-unauthenticated is worse than
//!   not starting.
//!
//! # Usage
//!
//! ```text
//! export-secret-env <exchange> <out_path> <VAR>=<field> [<VAR>=<field> ...]
//! ```
//!
//! `<field>` is one of `api_key` / `api_secret` / `api_passphrase` (the same
//! 3-slot model every other exchange uses). Example, matching the webui's
//! Rithmic form (User -> api_key, Password -> api_secret, System ->
//! api_passphrase):
//!
//! ```text
//! export-secret-env rithmic /secrets/rithmic.env \
//!     RITHMIC_USER=api_key RITHMIC_PASSWORD=api_secret RITHMIC_SYSTEM_NAME=api_passphrase
//! ```

use std::{env, fs, path::Path};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use spawner::db::BotRunStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 3 {
        anyhow::bail!(
            "usage: export-secret-env <exchange> <out_path> <VAR>=<field> [<VAR>=<field> ...]\n  \
             field is one of: api_key api_secret api_passphrase"
        );
    }
    let exchange = &args[0];
    let out_path = Path::new(&args[1]);
    let mappings: Vec<(String, String)> = args[2..]
        .iter()
        .map(|a| {
            a.split_once('=')
                .map(|(v, f)| (v.to_string(), f.to_string()))
                .ok_or_else(|| anyhow::anyhow!("bad mapping '{a}', expected VAR=field"))
        })
        .collect::<Result<_, _>>()?;

    let database_url = env::var("SPAWNER_DATABASE_URL")
        .or_else(|_| env::var("DATABASE_URL"))
        .map_err(|_| anyhow::anyhow!("SPAWNER_DATABASE_URL / DATABASE_URL not set"))?;

    let store = BotRunStore::try_connect(&database_url)
        .await
        .ok_or_else(|| {
            anyhow::anyhow!(
                "could not open the spawner DB (bad DATABASE_URL, or SPAWNER_SECRETS_KEY \
                 present-but-invalid — see try_connect's fail-safe)"
            )
        })?;

    let creds = store
        .get_secret(exchange)
        .await
        .map_err(|e| anyhow::anyhow!("decrypting exchange_secrets[{exchange}]: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("no exchange_secrets row for '{exchange}'"))?;

    let mut body = String::new();
    let mut present = Vec::new();
    let mut missing = Vec::new();
    for (var, field) in &mappings {
        let value = match field.as_str() {
            "api_key" => Some(creds.api_key.clone()),
            "api_secret" => Some(creds.api_secret.clone()),
            "api_passphrase" => creds.api_passphrase.clone(),
            other => anyhow::bail!("unknown field '{other}' (want api_key/api_secret/api_passphrase)"),
        };
        match value {
            // .env-shaped: strip any embedded newline so one credential can
            // never become two lines (or an env-parser injection) — these are
            // operator-supplied broker creds, not attacker input, but the
            // discipline costs nothing.
            Some(v) => {
                body.push_str(&format!("{var}={}\n", v.replace(['\n', '\r'], "")));
                present.push(var.as_str());
            }
            None => missing.push(var.as_str()),
        }
    }
    if present.is_empty() {
        anyhow::bail!(
            "none of the requested fields were present on exchange_secrets[{exchange}] \
             (requested: {mappings:?})"
        );
    }

    let parent = out_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(
        ".{}.tmp",
        out_path.file_name().unwrap().to_string_lossy()
    ));
    fs::write(&tmp, &body)?;
    #[cfg(unix)]
    fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))?;
    fs::rename(&tmp, out_path)?;

    eprintln!(
        "export-secret-env: wrote {} -> {} var(s) set: {:?}{}",
        exchange,
        out_path.display(),
        present,
        if missing.is_empty() {
            String::new()
        } else {
            format!(" (no value stored for: {missing:?})")
        }
    );
    Ok(())
}
