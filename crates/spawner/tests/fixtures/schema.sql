-- =============================================================================
-- schema.sql — test-only schema fixture for the testcontainers Postgres path.
-- =============================================================================
-- This is a faithful, self-contained transcription of the spawner tables the
-- DB layer queries. The production DDL lives in the fks repo under
-- src/sql/spawner/ (001..009) and is baked into the postgres image; those
-- files are psql scripts (\getenv / \connect / \gexec, CREATE DATABASE, a
-- scoped LOGIN role) that only run through the psql client. This fixture keeps
-- the table/constraint/trigger/index definitions verbatim but drops the psql
-- meta-commands, the database/role bootstrap, and the GRANT ... TO :fks_user
-- lines (the test connects as the image superuser, which already owns
-- everything). Keep it in sync with src/sql/spawner/002..008.
--
-- Applied by piping this file into `psql` inside the throwaway container, so
-- the plpgsql $$...$$ function bodies and multiple statements execute cleanly.
-- gen_random_uuid() is core PostgreSQL (>= 13), so no extension is required.
-- =============================================================================

-- ── shared trigger function (002_spawner.sql) ────────────────────────────────
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

-- ── bot_configs (002_spawner.sql) ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bot_configs (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT        NOT NULL,
    config_yaml     TEXT,
    config_json     JSONB,
    mode            TEXT        NOT NULL DEFAULT 'paper',
    image           TEXT        NOT NULL DEFAULT 'fks-bot-generic:latest',
    template_name   TEXT,
    cpu_limit       NUMERIC(5, 2),
    memory_mb       INTEGER,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT bot_configs_name_unique UNIQUE (name),
    CONSTRAINT bot_configs_mode_valid  CHECK (
        mode IN ('paper', 'live', 'backtest', 'optimise', 'train')
    )
);

DROP TRIGGER IF EXISTS trg_bot_configs_updated_at ON bot_configs;
CREATE TRIGGER trg_bot_configs_updated_at
    BEFORE UPDATE ON bot_configs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── bot_runs (002_spawner.sql) ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bot_runs (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    bot_config_id   UUID        REFERENCES bot_configs(id) ON DELETE SET NULL,
    container_id    TEXT        NOT NULL,
    container_name  TEXT,
    image           TEXT        NOT NULL,
    mode            TEXT        NOT NULL DEFAULT 'paper',
    status          TEXT        NOT NULL DEFAULT 'spawning',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stopped_at      TIMESTAMPTZ,
    runtime_secs    INTEGER,
    pnl_final       NUMERIC(20, 8),
    signal_count    INTEGER     NOT NULL DEFAULT 0,
    trade_count     INTEGER     NOT NULL DEFAULT 0,
    win_rate        NUMERIC(5, 4),
    last_cpu_pct    NUMERIC,
    last_mem_mb     NUMERIC,
    last_heartbeat  TIMESTAMPTZ,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT bot_runs_status_valid CHECK (
        status IN ('spawning', 'running', 'stopping', 'stopped', 'error', 'pruned')
    )
);

CREATE INDEX IF NOT EXISTS idx_bot_runs_status       ON bot_runs(status);
CREATE INDEX IF NOT EXISTS idx_bot_runs_started_at   ON bot_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_bot_runs_container_id ON bot_runs(container_id);

CREATE OR REPLACE FUNCTION compute_bot_run_runtime()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.stopped_at IS NOT NULL AND OLD.stopped_at IS NULL THEN
        NEW.runtime_secs = EXTRACT(EPOCH FROM (NEW.stopped_at - NEW.started_at))::INTEGER;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_bot_runs_runtime ON bot_runs;
CREATE TRIGGER trg_bot_runs_runtime
    BEFORE UPDATE ON bot_runs
    FOR EACH ROW EXECUTE FUNCTION compute_bot_run_runtime();

-- ── exchange_secrets (003_secrets.sql) ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS exchange_secrets (
    exchange        TEXT        PRIMARY KEY,
    api_key         TEXT        NOT NULL,
    api_secret      TEXT        NOT NULL,
    api_passphrase  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_exchange_secrets_updated_at ON exchange_secrets;
CREATE TRIGGER trg_exchange_secrets_updated_at
    BEFORE UPDATE ON exchange_secrets
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── notification_channels (004_notifications.sql) ────────────────────────────
CREATE TABLE IF NOT EXISTS notification_channels (
    name        TEXT        PRIMARY KEY,
    kind        TEXT        NOT NULL DEFAULT 'discord_webhook',
    target      TEXT        NOT NULL,
    events      JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_notification_channels_updated_at ON notification_channels;
CREATE TRIGGER trg_notification_channels_updated_at
    BEFORE UPDATE ON notification_channels
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── ui_layouts (005_ui_layouts.sql) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ui_layouts (
    name        TEXT        PRIMARY KEY,
    layout      JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_ui_layouts_updated_at ON ui_layouts;
CREATE TRIGGER trg_ui_layouts_updated_at
    BEFORE UPDATE ON ui_layouts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── net_worth_snapshots (006_net_worth_snapshots.sql) ────────────────────────
CREATE TABLE IF NOT EXISTS net_worth_snapshots (
    id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bot_id      TEXT        NOT NULL,
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    net_worth   NUMERIC(38, 18) NOT NULL,
    currency    TEXT        NOT NULL DEFAULT 'USD',
    venue       TEXT,
    source      TEXT        NOT NULL DEFAULT 'bot_status'
);

CREATE INDEX IF NOT EXISTS idx_net_worth_snapshots_bot_ts
    ON net_worth_snapshots (bot_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_net_worth_snapshots_ts
    ON net_worth_snapshots (ts DESC);

-- ── transfers (007_treasury.sql) ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transfers (
    id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id  TEXT        NOT NULL,
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    amount      NUMERIC(38, 18) NOT NULL CHECK (amount <> 0),
    currency    TEXT        NOT NULL DEFAULT 'USD',
    kind        TEXT        NOT NULL
                            CHECK (kind IN ('deposit', 'withdrawal', 'payout', 'sweep')),
    source      TEXT        NOT NULL DEFAULT 'manual',
    note        TEXT
);

CREATE INDEX IF NOT EXISTS idx_transfers_account_ts
    ON transfers (account_id, ts DESC);

-- ── accounts (007_treasury.sql) ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS accounts (
    account_id       TEXT        PRIMARY KEY,
    display_name     TEXT,
    tier             SMALLINT    NOT NULL,
    account_class    TEXT        NOT NULL,
    venue            TEXT,
    role             TEXT        NOT NULL
                                 CHECK (role IN ('watch', 'bot-trade',
                                                 'human-trade-source', 'copy-target')),
    firm             TEXT,
    compliance_flag  TEXT        NOT NULL DEFAULT 'manual-mirror'
                                 CHECK (compliance_flag IN ('manual-mirror', 'auto-fill')),
    risk_caps        JSONB       DEFAULT '{}',
    sizing           JSONB       DEFAULT '{}',
    active           BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_accounts_updated_at ON accounts;
CREATE TRIGGER trg_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── edges (008_edge_factory.sql) ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS edges (
    edge_id           TEXT        PRIMARY KEY,
    display_name      TEXT,
    edge_type         TEXT        NOT NULL
                                  CHECK (edge_type IN ('adaptive', 'rule')),
    asset_scope       JSONB       NOT NULL DEFAULT '[]',
    status            TEXT        NOT NULL DEFAULT 'research'
                                  CHECK (status IN ('research', 'paper',
                                                    'live', 'retired')),
    backtest_image    TEXT,
    validation_record JSONB       NOT NULL DEFAULT '{}',
    notes             TEXT,
    active            BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_edges_updated_at ON edges;
CREATE TRIGGER trg_edges_updated_at
    BEFORE UPDATE ON edges
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── backtest_runs (008_edge_factory.sql) ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS backtest_runs (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    edge_id      TEXT        NOT NULL,
    container_id TEXT,
    status       TEXT        NOT NULL DEFAULT 'running'
                             CHECK (status IN ('running', 'completed', 'failed')),
    params       JSONB       NOT NULL DEFAULT '{}',
    results      JSONB,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_edge_started
    ON backtest_runs (edge_id, started_at DESC);
