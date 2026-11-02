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
-- everything). Keep it in sync with src/sql/spawner/002..008 + 022 (account_leases)
-- + 017/018/020/024/029/030 (the edge ladder).
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
        -- 'record' (recorder instances) is fks src/sql/spawner/021_record_mode.sql
        mode IN ('paper', 'live', 'backtest', 'optimise', 'train', 'record')
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

-- fks 026: a bot's own channel opts out of the platform's events.
ALTER TABLE notification_channels
    ADD COLUMN IF NOT EXISTS platform_events BOOLEAN NOT NULL DEFAULT TRUE;

DROP TRIGGER IF EXISTS trg_notification_channels_updated_at ON notification_channels;
CREATE TRIGGER trg_notification_channels_updated_at
    BEFORE UPDATE ON notification_channels
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── notification_log (013_notification_log.sql) ──────────────────────────────
-- Delivery ledger: one row per webhook send attempt. detail is a truncated
-- event snippet, NEVER the URL; outcome is CHECK-bounded to the dispatcher arms.
CREATE TABLE IF NOT EXISTS notification_log (
    id           BIGSERIAL PRIMARY KEY,
    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
    event        TEXT NOT NULL,
    bot_id       TEXT NOT NULL DEFAULT '',
    channel_name TEXT NOT NULL,
    kind         TEXT NOT NULL,
    outcome      TEXT NOT NULL CHECK (outcome IN
                 ('sent','http_error','send_failed','decrypt_failed',
                  'test_sent','test_failed')),
    status_code  SMALLINT,
    detail       TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS notification_log_ts_idx ON notification_log (ts DESC);
CREATE INDEX IF NOT EXISTS notification_log_event_ts_idx ON notification_log (event, ts DESC);

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

-- ── account_leases (fks src/sql/spawner/022_account_leases.sql) ─────────────
-- One row per leased exchange account: the spawner may run at most one bot
-- per account (crate::leases). account is the PRIMARY KEY by design.
CREATE TABLE IF NOT EXISTS account_leases (
    account      TEXT        PRIMARY KEY,
    bot_id       TEXT        NOT NULL,
    container_id TEXT,
    acquired_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL
);

-- ── The edge ladder: edge_experiments (017), edge_deployments (018, its
-- guard replaced by 020's), edge_hypotheses (024) ───────────────────────────
-- Verbatim from fks src/sql/spawner, minus the psql meta-commands and the
-- GRANTs to fks_webui (no such role here). The guards are the point of
-- these tables, so they are copied whole: the ladder tests drive the store
-- against the real triggers.

-- ── 017_edge_experiments.sql ──
CREATE TABLE IF NOT EXISTS edge_experiments (
    id                     BIGSERIAL PRIMARY KEY,

    -- ── Identity ────────────────────────────────────────────────────────────
    -- The Optuna study this summarises. UNIQUE so re-ingesting the same study
    -- updates rather than duplicating — an experiment counted twice corrupts
    -- exactly the N this table exists to provide.
    study_name             TEXT        NOT NULL UNIQUE,
    -- The optimizer's own identity hash over dataset + folds + risk policy +
    -- search space. Two rows sharing it were searching the same problem; two
    -- rows differing on it are NOT comparable, whatever their scores say.
    config_fingerprint     TEXT        NOT NULL,
    symbol                 TEXT        NOT NULL,
    bar_minutes            INTEGER     NOT NULL,

    -- ── The hypothesis, ideally written FIRST ───────────────────────────────
    hypothesis             TEXT,
    registered_at          TIMESTAMPTZ,

    -- ── What was searched ───────────────────────────────────────────────────
    -- The dimensions actually tuned. A 4-name array and a 71-name array are
    -- different experiments even at identical trial counts, and the difference
    -- is the whole argument about overfitting.
    tuned_params           TEXT[]      NOT NULL DEFAULT '{}',
    n_trials               INTEGER     NOT NULL,
    train_days             INTEGER,
    test_days              INTEGER,
    folds                  INTEGER,
    holdout_folds          INTEGER,

    -- ── What came out ───────────────────────────────────────────────────────
    -- selection_score is what the search MAXIMISED and is therefore the most
    -- flattered number here. holdout_score is the only one nobody selected on.
    selection_score        DOUBLE PRECISION,
    holdout_score          DOUBLE PRECISION,
    baseline_holdout_score DOUBLE PRECISION,
    overfit_gap            DOUBLE PRECISION,
    selection_trades       INTEGER,
    holdout_trades         INTEGER,

    -- ── The verdict, verbatim ───────────────────────────────────────────────
    -- Stored as the optimizer emitted it. Re-deriving a verdict later from
    -- stored scores would let a change in the gate silently rewrite history.
    verdict                TEXT        NOT NULL,
    verdict_reasons        TEXT[]      NOT NULL DEFAULT '{}',

    -- What the OPERATOR did about it, which is not the same as what the gate
    -- said. An adopted row whose verdict was 'DO NOT PASTE' is exactly the kind
    -- of thing this registry should make impossible to forget.
    outcome                TEXT        NOT NULL DEFAULT 'pending',

    finished_at            TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT edge_experiments_verdict_known
        CHECK (verdict IN ('USABLE', 'DO NOT PASTE')),
    CONSTRAINT edge_experiments_outcome_known
        CHECK (outcome IN ('pending', 'adopted', 'rejected', 'superseded')),
    CONSTRAINT edge_experiments_trials_positive
        CHECK (n_trials > 0)
);

-- The DSR query counts trials per symbol/fingerprint family.
CREATE INDEX IF NOT EXISTS edge_experiments_family_idx
    ON edge_experiments (symbol, config_fingerprint);
CREATE INDEX IF NOT EXISTS edge_experiments_finished_idx
    ON edge_experiments (finished_at DESC);

COMMENT ON TABLE edge_experiments IS
    'One row per Optuna study. Exists so the NUMBER of things tried is knowable, '
    'which is the prerequisite for any multiple-testing correction.';
COMMENT ON COLUMN edge_experiments.hypothesis IS
    'Written BEFORE the result, ideally. Compare registered_at with finished_at.';
COMMENT ON COLUMN edge_experiments.selection_score IS
    'What the search maximised — the most flattered number in the row.';
COMMENT ON COLUMN edge_experiments.holdout_score IS
    'Scored once, on folds the search never saw. The only number not selected for.';

-- ── The view the whole table exists for ─────────────────────────────────────
-- Cumulative trial count per family. This is the N that a Deflated Sharpe Ratio
-- needs, and it is deliberately a VIEW so it can never drift from the rows.
CREATE OR REPLACE VIEW edge_search_effort AS
SELECT
    symbol,
    config_fingerprint,
    count(*)                          AS experiments,
    sum(n_trials)                     AS trials_total,
    max(holdout_score)                AS best_holdout,
    count(*) FILTER (WHERE verdict = 'USABLE')   AS usable,
    count(*) FILTER (WHERE outcome  = 'adopted') AS adopted,
    min(finished_at)                  AS first_run,
    max(finished_at)                  AS last_run
FROM edge_experiments
GROUP BY symbol, config_fingerprint;

COMMENT ON VIEW edge_search_effort IS
    'trials_total is the N for a multiple-testing correction: how many draws the '
    'best result is the maximum of. Reporting a winner without it overstates.';

-- ── 018_edge_deployments.sql ──
CREATE TABLE IF NOT EXISTS edge_deployments (
    id                        BIGSERIAL PRIMARY KEY,

    -- ── What is deployed, and on the strength of what ───────────────────────
    -- RESTRICT, not CASCADE: deleting the experiment that justified a live
    -- deployment must not silently succeed and leave the deployment orphaned
    -- of its evidence. Retire the deployment first.
    experiment_id             BIGINT      NOT NULL
                                          REFERENCES edge_experiments (id)
                                          ON DELETE RESTRICT,

    -- Reuses the existing account registry (007) rather than inventing a
    -- second one. `accounts.account_class` is what decides whether `live` may
    -- mean autonomous execution — see the promotion guard below.
    account_id                TEXT        NOT NULL
                                          REFERENCES accounts (account_id)
                                          ON DELETE RESTRICT,

    symbol                    TEXT        NOT NULL,

    -- ── Where it sits on the ladder ─────────────────────────────────────────
    stage                     TEXT        NOT NULL DEFAULT 'backtest',

    -- Who pulls the trigger. Defaults to the safe value; promoting to 'auto'
    -- is refused outright on a prop account.
    execution                 TEXT        NOT NULL DEFAULT 'manual',

    -- ── The thing actually being traded ─────────────────────────────────────
    -- Frozen at deployment so a live edge cannot silently drift from what was
    -- validated. The object being traded and the object that was tested must
    -- be provably the same object; without this, "it passed the holdout" is a
    -- claim about a parameter set nobody can produce afterwards.
    params_snapshot           JSONB       NOT NULL,

    -- ── The forward-test contract, declared BEFORE the evidence arrives ─────
    -- Set when entering `forward` and immutable thereafter (enforced below).
    -- Deciding how many sessions count as proof AFTER seeing the sessions is
    -- how a forward test becomes a second selection set.
    forward_sessions_required INTEGER,
    forward_sessions_observed INTEGER     NOT NULL DEFAULT 0,

    -- ── Live outcome, for the divergence monitor ────────────────────────────
    live_trades               INTEGER     NOT NULL DEFAULT 0,
    live_r_sum                DOUBLE PRECISION NOT NULL DEFAULT 0,
    divergence_vs_backtest    DOUBLE PRECISION,

    -- ── Bookkeeping ─────────────────────────────────────────────────────────
    since                     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Required on retirement. This is a NOTE, not an approval — retirement is
    -- still unconditional — but a demotion nobody can explain later is a
    -- demotion that teaches nothing.
    retired_reason            TEXT,

    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT edge_deployments_stage_known
        CHECK (stage IN ('backtest', 'forward', 'live', 'retired')),
    CONSTRAINT edge_deployments_execution_known
        CHECK (execution IN ('manual', 'auto')),
    CONSTRAINT edge_deployments_forward_required_positive
        CHECK (forward_sessions_required IS NULL OR forward_sessions_required > 0),
    CONSTRAINT edge_deployments_observed_not_negative
        CHECK (forward_sessions_observed >= 0),
    CONSTRAINT edge_deployments_retired_has_reason
        CHECK (stage <> 'retired' OR retired_reason IS NOT NULL)
);

-- One live edge per instrument per account. Two edges both live on GC in the
-- same account is not a portfolio, it is two systems fighting over one
-- position. Partial, so retired/backtest rows may pile up freely.
CREATE UNIQUE INDEX IF NOT EXISTS edge_deployments_one_live_per_symbol_account
    ON edge_deployments (account_id, symbol)
    WHERE stage = 'live';

CREATE INDEX IF NOT EXISTS edge_deployments_stage_idx
    ON edge_deployments (stage);
CREATE INDEX IF NOT EXISTS edge_deployments_experiment_idx
    ON edge_deployments (experiment_id);

-- ── The ladder, enforced ────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION edge_deployments_promotion_guard()
RETURNS TRIGGER AS $$
DECLARE
    v_verdict TEXT;
    v_class   TEXT;
BEGIN
    -- An `auto` deployment on a prop account would be a trading bot on an
    -- account whose firm prohibits automated decisions. Refuse it at the
    -- source, on INSERT and UPDATE alike, so no code path can create one.
    IF NEW.execution = 'auto' THEN
        SELECT account_class INTO v_class
          FROM accounts WHERE account_id = NEW.account_id;
        IF v_class = 'prop' THEN
            RAISE EXCEPTION
                'execution=auto is refused on prop account % (class=%): the '
                'firm prohibits automated decisions, so a human places every '
                'order on this account', NEW.account_id, v_class
                USING ERRCODE = 'check_violation';
        END IF;
    END IF;

    IF TG_OP = 'INSERT' THEN
        -- New deployments start at the bottom. Inserting straight at 'live'
        -- would walk around every rule below, which is the obvious attack on
        -- any state machine whose transitions are guarded but whose entry
        -- point is not.
        IF NEW.stage <> 'backtest' THEN
            RAISE EXCEPTION
                'a new deployment must start at stage=backtest (got %); the '
                'ladder is climbed, not entered partway up', NEW.stage
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    -- Same-stage edits: allowed, EXCEPT moving the goalposts. Once a forward
    -- test is underway its required session count is frozen; lowering it after
    -- seeing results is precisely the thing pre-registration prevents.
    IF NEW.stage = OLD.stage THEN
        IF OLD.stage <> 'backtest'
           AND NEW.forward_sessions_required IS DISTINCT FROM OLD.forward_sessions_required THEN
            RAISE EXCEPTION
                'forward_sessions_required is frozen once a deployment leaves '
                'backtest (% -> %): it is declared in advance or it is not '
                'evidence', OLD.forward_sessions_required, NEW.forward_sessions_required
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    -- Retirement is always available, needs no approval, and is the only
    -- transition anything automatic may perform. Demotion must be cheaper than
    -- promotion, or the system acquires a bias toward staying deployed.
    IF NEW.stage = 'retired' THEN
        RETURN NEW;
    END IF;

    -- ...and it is terminal. Re-deploying is a new decision and deserves a new
    -- row with its own evidence, not a quiet un-retirement of the old one.
    IF OLD.stage = 'retired' THEN
        RAISE EXCEPTION
            'retired is terminal; create a new deployment rather than '
            'un-retiring this one'
            USING ERRCODE = 'check_violation';
    END IF;

    IF OLD.stage = 'backtest' AND NEW.stage = 'forward' THEN
        SELECT verdict INTO v_verdict
          FROM edge_experiments WHERE id = NEW.experiment_id;
        IF v_verdict IS DISTINCT FROM 'USABLE' THEN
            RAISE EXCEPTION
                'backtest->forward requires the experiment verdict to be '
                'USABLE (experiment % is %)', NEW.experiment_id,
                COALESCE(v_verdict, '<missing>')
                USING ERRCODE = 'check_violation';
        END IF;
        IF NEW.forward_sessions_required IS NULL THEN
            RAISE EXCEPTION
                'backtest->forward requires forward_sessions_required to be '
                'declared in advance'
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.stage = 'forward' AND NEW.stage = 'live' THEN
        IF NEW.forward_sessions_observed < NEW.forward_sessions_required THEN
            RAISE EXCEPTION
                'forward->live requires % forward session(s), only % observed',
                NEW.forward_sessions_required, NEW.forward_sessions_observed
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    -- Everything else — skipping a rung, or climbing back down to anything but
    -- retired — is refused. There is no legitimate backtest->live.
    RAISE EXCEPTION
        'illegal stage transition % -> %; the ladder is backtest -> forward -> '
        'live, and any stage may go to retired', OLD.stage, NEW.stage
        USING ERRCODE = 'check_violation';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_edge_deployments_promotion ON edge_deployments;
CREATE TRIGGER trg_edge_deployments_promotion
    BEFORE INSERT OR UPDATE ON edge_deployments
    FOR EACH ROW EXECUTE FUNCTION edge_deployments_promotion_guard();

-- Stamp updated_at on every change. Reuses set_updated_at() from 002_spawner.sql.
DROP TRIGGER IF EXISTS trg_edge_deployments_updated_at ON edge_deployments;
CREATE TRIGGER trg_edge_deployments_updated_at
    BEFORE UPDATE ON edge_deployments
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE edge_deployments IS
    'One row per edge bound to an asset on an account, with its rung on the '
    'promotion ladder. Transitions are enforced by a trigger, not by the UI.';
COMMENT ON COLUMN edge_deployments.execution IS
    'manual = a human places every order (the only value a prop account may '
    'hold) | auto = the machine may place orders';
COMMENT ON COLUMN edge_deployments.params_snapshot IS
    'Frozen at deployment so a live edge cannot drift from what was validated.';
COMMENT ON COLUMN edge_deployments.forward_sessions_required IS
    'Declared on entering forward and immutable after — deciding the bar after '
    'seeing the results turns a forward test into a second selection set.';

-- ── 020_promotion_guard_freeze.sql ──
CREATE OR REPLACE FUNCTION edge_deployments_promotion_guard()
RETURNS TRIGGER AS $$
DECLARE
    v_class   TEXT;
    v_verdict TEXT;
BEGIN
    -- `accounts.account_class`, matching the FK
    -- (edge_deployments.account_id -> accounts.account_id) and unchanged from 018.
    IF NEW.execution = 'auto' THEN
        SELECT account_class INTO v_class
          FROM accounts WHERE account_id = NEW.account_id;
        IF v_class = 'prop' THEN
            RAISE EXCEPTION
                'execution=auto is refused on prop account % (class=%): the '
                'firm prohibits automated decisions, so a human places every '
                'order on this account', NEW.account_id, v_class
                USING ERRCODE = 'check_violation';
        END IF;
    END IF;

    IF TG_OP = 'INSERT' THEN
        -- New deployments start at the bottom. Inserting straight at 'live'
        -- would walk around every rule below, which is the obvious attack on
        -- any state machine whose transitions are guarded but whose entry
        -- point is not.
        IF NEW.stage <> 'backtest' THEN
            RAISE EXCEPTION
                'a new deployment must start at stage=backtest (got %); the '
                'ladder is climbed, not entered partway up', NEW.stage
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    -- ── THE FREEZE, now evaluated on EVERY update ───────────────────────────
    -- Hoisted out of the same-stage branch (defect 1). Once a deployment has
    -- left backtest its declared requirement is immutable, whether or not the
    -- same statement also moves the stage. Retirement is exempt: abandoning a
    -- test must never be harder than completing it.
    IF OLD.stage <> 'backtest' AND NEW.stage <> 'retired'
       AND NEW.forward_sessions_required IS DISTINCT FROM OLD.forward_sessions_required THEN
        RAISE EXCEPTION
            'forward_sessions_required is frozen once a deployment leaves '
            'backtest (% -> %): it is declared in advance or it is not '
            'evidence', OLD.forward_sessions_required, NEW.forward_sessions_required
            USING ERRCODE = 'check_violation';
    END IF;

    IF NEW.stage = OLD.stage THEN
        RETURN NEW;
    END IF;

    -- Retirement is always available, needs no approval, and is the only
    -- transition anything automatic may perform. Demotion must be cheaper than
    -- promotion, or the system acquires a bias toward staying deployed.
    IF NEW.stage = 'retired' THEN
        RETURN NEW;
    END IF;

    -- ...and it is terminal. Re-deploying is a new decision and deserves a new
    -- row with its own evidence, not a quiet un-retirement of the old one.
    IF OLD.stage = 'retired' THEN
        RAISE EXCEPTION
            'retired is terminal; create a new deployment rather than '
            'un-retiring this one'
            USING ERRCODE = 'check_violation';
    END IF;

    IF OLD.stage = 'backtest' AND NEW.stage = 'forward' THEN
        SELECT verdict INTO v_verdict
          FROM edge_experiments WHERE id = NEW.experiment_id;
        IF v_verdict IS DISTINCT FROM 'USABLE' THEN
            RAISE EXCEPTION
                'backtest->forward requires the experiment verdict to be '
                'USABLE (experiment % is %)', NEW.experiment_id,
                COALESCE(v_verdict, '<missing>')
                USING ERRCODE = 'check_violation';
        END IF;
        IF NEW.forward_sessions_required IS NULL THEN
            RAISE EXCEPTION
                'backtest->forward requires forward_sessions_required to be '
                'declared in advance'
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.stage = 'forward' AND NEW.stage = 'live' THEN
        -- Fail closed on an unknown requirement (defect 3). `observed < NULL`
        -- is NULL, not TRUE, so without this the exception never fires and a
        -- NULL requirement is a complete bypass.
        IF OLD.forward_sessions_required IS NULL THEN
            RAISE EXCEPTION
                'forward->live refused: forward_sessions_required is NULL, so '
                'there is no declared requirement to have met'
                USING ERRCODE = 'check_violation';
        END IF;
        -- Measure against the requirement declared IN ADVANCE (defect 2).
        IF NEW.forward_sessions_observed IS NULL
           OR NEW.forward_sessions_observed < OLD.forward_sessions_required THEN
            RAISE EXCEPTION
                'forward->live requires % forward session(s), only % observed',
                OLD.forward_sessions_required,
                COALESCE(NEW.forward_sessions_observed::TEXT, 'NULL')
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    -- Everything else — skipping a rung, or climbing back down to anything but
    -- retired — is refused. There is no legitimate backtest->live.
    RAISE EXCEPTION
        'illegal stage transition % -> %; the ladder is backtest -> forward -> '
        'live, and any stage may go to retired', OLD.stage, NEW.stage
        USING ERRCODE = 'check_violation';
END;
$$ LANGUAGE plpgsql;

-- ── 024_edge_hypotheses.sql ──
CREATE TABLE IF NOT EXISTS edge_hypotheses (
    id               BIGSERIAL   PRIMARY KEY,

    -- The edge this is a hypothesis about (edges, 008). RESTRICT: an edge
    -- with registered hypotheses is soft-deleted (active = FALSE), never
    -- dropped from under them.
    edge_id          TEXT        NOT NULL
                                 REFERENCES edges (edge_id) ON DELETE RESTRICT,

    -- §2.1 The idea in one sentence, with the mechanism.
    idea             TEXT        NOT NULL,
    -- §2.2 The data and the split. `dataset` names it in words (the lake
    -- folder and the date range) until dataset manifests exist (plan 4.1,
    -- 4.3); `holdout_share` is the last part of the window that nothing
    -- touches until the holdout is scored once.
    symbols          TEXT[]      NOT NULL,
    dataset          TEXT        NOT NULL,
    holdout_share    DOUBLE PRECISION NOT NULL,
    -- §2.3 The parameters and their ranges, few of them:
    --   [{"name": "lookback", "min": 10, "max": 200, "step": 10},
    --    {"name": "side", "choices": ["long", "short"]}]
    params           JSONB       NOT NULL,
    -- §2.4 What the edge must beat on the same holdout, and §2.5 the score,
    -- net of these costs (bps): {"maker_bps": 40, "taker_bps": 80,
    -- "slippage_bps": 5} is Kraken tier 1 plus a stated slippage.
    baseline         TEXT        NOT NULL,
    metric           TEXT        NOT NULL,
    costs            JSONB       NOT NULL,

    registered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The web UI user (or API token) that registered it.
    registered_by    TEXT,

    status           TEXT        NOT NULL DEFAULT 'registered',
    withdrawn_reason TEXT,
    withdrawn_at     TIMESTAMPTZ,

    CONSTRAINT edge_hypotheses_status_known
        CHECK (status IN ('registered', 'withdrawn')),
    CONSTRAINT edge_hypotheses_withdrawn_has_reason
        CHECK (status <> 'withdrawn' OR (withdrawn_reason IS NOT NULL AND withdrawn_at IS NOT NULL)),
    CONSTRAINT edge_hypotheses_idea_written
        CHECK (length(btrim(idea)) >= 20),
    CONSTRAINT edge_hypotheses_symbols_named
        CHECK (cardinality(symbols) >= 1),
    CONSTRAINT edge_hypotheses_dataset_named
        CHECK (length(btrim(dataset)) > 0),
    CONSTRAINT edge_hypotheses_holdout_sane
        CHECK (holdout_share > 0 AND holdout_share < 1),
    CONSTRAINT edge_hypotheses_params_listed
        CHECK (jsonb_typeof(params) = 'array' AND jsonb_array_length(params) >= 1),
    CONSTRAINT edge_hypotheses_costs_object
        CHECK (jsonb_typeof(costs) = 'object'),
    CONSTRAINT edge_hypotheses_baseline_named
        CHECK (length(btrim(baseline)) > 0),
    CONSTRAINT edge_hypotheses_metric_named
        CHECK (length(btrim(metric)) > 0)
);

CREATE INDEX IF NOT EXISTS edge_hypotheses_edge_idx
    ON edge_hypotheses (edge_id, registered_at DESC);

COMMENT ON TABLE edge_hypotheses IS
    'Stage 2 of the edge process: an idea registered before its search, frozen once '
    'registered. The search that tests it is an edge_experiments row pointing here.';
COMMENT ON COLUMN edge_hypotheses.registered_at IS
    'Set by the database at registration. An experiment that names this hypothesis '
    'must finish after it (U2).';

-- ── 1 + 2: frozen once registered, never deleted ────────────────────────────
CREATE OR REPLACE FUNCTION edge_hypotheses_frozen()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'hypothesis % is registered and is never deleted: withdraw it with a '
            'reason instead (a withdrawn hypothesis still counts as tried)', OLD.id
            USING ERRCODE = 'check_violation';
    END IF;

    IF NEW.edge_id       IS DISTINCT FROM OLD.edge_id
       OR NEW.idea          IS DISTINCT FROM OLD.idea
       OR NEW.symbols       IS DISTINCT FROM OLD.symbols
       OR NEW.dataset       IS DISTINCT FROM OLD.dataset
       OR NEW.holdout_share IS DISTINCT FROM OLD.holdout_share
       OR NEW.params        IS DISTINCT FROM OLD.params
       OR NEW.baseline      IS DISTINCT FROM OLD.baseline
       OR NEW.metric        IS DISTINCT FROM OLD.metric
       OR NEW.costs         IS DISTINCT FROM OLD.costs
       OR NEW.registered_at IS DISTINCT FROM OLD.registered_at
       OR NEW.registered_by IS DISTINCT FROM OLD.registered_by THEN
        RAISE EXCEPTION
            'hypothesis % is frozen since it was registered (%): its idea, data, '
            'split, parameters, baseline, metric and costs cannot change. Register '
            'a new hypothesis instead', OLD.id, OLD.registered_at
            USING ERRCODE = 'check_violation';
    END IF;

    IF OLD.status = 'withdrawn' AND (NEW.status <> 'withdrawn'
       OR NEW.withdrawn_reason IS DISTINCT FROM OLD.withdrawn_reason
       OR NEW.withdrawn_at IS DISTINCT FROM OLD.withdrawn_at) THEN
        RAISE EXCEPTION
            'hypothesis % was withdrawn (%), which is terminal: register a new one',
            OLD.id, OLD.withdrawn_reason
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_edge_hypotheses_frozen ON edge_hypotheses;
CREATE TRIGGER trg_edge_hypotheses_frozen
    BEFORE UPDATE OR DELETE ON edge_hypotheses
    FOR EACH ROW EXECUTE FUNCTION edge_hypotheses_frozen();

-- ── 3: the experiment that searches a hypothesis comes after it ─────────────
ALTER TABLE edge_experiments
    ADD COLUMN IF NOT EXISTS hypothesis_id BIGINT
        REFERENCES edge_hypotheses (id) ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS edge_experiments_hypothesis_idx
    ON edge_experiments (hypothesis_id);

COMMENT ON COLUMN edge_experiments.hypothesis_id IS
    'The registered hypothesis this search tests (024). Its registered_at must come '
    'before this row''s finished_at.';

CREATE OR REPLACE FUNCTION edge_experiments_after_hypothesis()
RETURNS TRIGGER AS $$
DECLARE
    v_registered TIMESTAMPTZ;
BEGIN
    IF NEW.hypothesis_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT registered_at INTO v_registered
      FROM edge_hypotheses WHERE id = NEW.hypothesis_id;
    IF NEW.finished_at IS NOT NULL AND NEW.finished_at <= v_registered THEN
        RAISE EXCEPTION
            'experiment % finished at %, before hypothesis % was registered (%): '
            'a hypothesis written after the result is a description, not a test',
            NEW.study_name, NEW.finished_at, NEW.hypothesis_id, v_registered
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_edge_experiments_after_hypothesis ON edge_experiments;
CREATE TRIGGER trg_edge_experiments_after_hypothesis
    BEFORE INSERT OR UPDATE OF hypothesis_id, finished_at ON edge_experiments
    FOR EACH ROW EXECUTE FUNCTION edge_experiments_after_hypothesis();

-- ── 028_hypothesis_evidence.sql ──
ALTER TABLE edge_hypotheses
    ADD COLUMN IF NOT EXISTS evidence TEXT NOT NULL DEFAULT 'trades';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'edge_hypotheses_evidence_known'
          AND conrelid = 'edge_hypotheses'::regclass
    ) THEN
        ALTER TABLE edge_hypotheses
            ADD CONSTRAINT edge_hypotheses_evidence_known
            CHECK (evidence IN ('trades', 'low-frequency'));
    END IF;
END $$;

COMMENT ON COLUMN edge_hypotheses.evidence IS
    'The evidence path, declared at registration and frozen: trades (U4) or '
    'low-frequency (pooled trades over many coins, split by regime).';

CREATE OR REPLACE FUNCTION edge_hypotheses_frozen()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'hypothesis % is registered and is never deleted: withdraw it with a '
            'reason instead (a withdrawn hypothesis still counts as tried)', OLD.id
            USING ERRCODE = 'check_violation';
    END IF;

    IF NEW.edge_id       IS DISTINCT FROM OLD.edge_id
       OR NEW.idea          IS DISTINCT FROM OLD.idea
       OR NEW.symbols       IS DISTINCT FROM OLD.symbols
       OR NEW.dataset       IS DISTINCT FROM OLD.dataset
       OR NEW.holdout_share IS DISTINCT FROM OLD.holdout_share
       OR NEW.params        IS DISTINCT FROM OLD.params
       OR NEW.baseline      IS DISTINCT FROM OLD.baseline
       OR NEW.metric        IS DISTINCT FROM OLD.metric
       OR NEW.costs         IS DISTINCT FROM OLD.costs
       OR NEW.evidence      IS DISTINCT FROM OLD.evidence
       OR NEW.registered_at IS DISTINCT FROM OLD.registered_at
       OR NEW.registered_by IS DISTINCT FROM OLD.registered_by THEN
        RAISE EXCEPTION
            'hypothesis % is frozen since it was registered (%): its idea, data, '
            'split, parameters, baseline, metric, costs and evidence path cannot change. Register '
            'a new hypothesis instead', OLD.id, OLD.registered_at
            USING ERRCODE = 'check_violation';
    END IF;

    IF OLD.status = 'withdrawn' AND (NEW.status <> 'withdrawn'
       OR NEW.withdrawn_reason IS DISTINCT FROM OLD.withdrawn_reason
       OR NEW.withdrawn_at IS DISTINCT FROM OLD.withdrawn_at) THEN
        RAISE EXCEPTION
            'hypothesis % was withdrawn (%), which is terminal: register a new one',
            OLD.id, OLD.withdrawn_reason
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ── 029_edge_forward_sessions.sql ──
-- ── The search's selection, and experiments that stay as written ─────────────
ALTER TABLE edge_experiments ADD COLUMN IF NOT EXISTS selected_params JSONB;
COMMENT ON COLUMN edge_experiments.selected_params IS
    'The selected parameter set (the search job writes it); a deployment of '
    'this experiment trades exactly this, params_snapshot = selected_params.';

CREATE OR REPLACE FUNCTION edge_experiments_frozen()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            'experiment % is a result and is never deleted: a DO NOT PASTE '
            'counts toward N like any other search', OLD.id
            USING ERRCODE = 'check_violation';
    END IF;
    -- A study still running (no finished_at) is still being written.
    IF OLD.finished_at IS NULL THEN
        RETURN NEW;
    END IF;
    IF (to_jsonb(NEW) - 'outcome' - 'updated_at') IS DISTINCT FROM
       (to_jsonb(OLD) - 'outcome' - 'updated_at') THEN
        RAISE EXCEPTION
            'experiment % finished at % and is frozen as the search wrote it: '
            'only its outcome may change', OLD.id, OLD.finished_at
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_edge_experiments_frozen ON edge_experiments;
CREATE TRIGGER trg_edge_experiments_frozen
    BEFORE UPDATE OR DELETE ON edge_experiments
    FOR EACH ROW EXECUTE FUNCTION edge_experiments_frozen();

-- ── The forward ledger ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS edge_forward_sessions (
    id              BIGSERIAL   PRIMARY KEY,
    deployment_id   BIGINT      NOT NULL REFERENCES edge_deployments (id) ON DELETE RESTRICT,
    session_date    DATE        NOT NULL,
    symbol          TEXT        NOT NULL,
    traded          BOOLEAN     NOT NULL,
    -- side, entry/exit minute and price, return: NULL when flat.
    trade           JSONB,
    -- net, over the notional at entry; 0 when flat.
    session_return  DOUBLE PRECISION NOT NULL,
    -- [{"path": "...", "sha256": "..."}]: the lake files it read.
    data_ref        JSONB       NOT NULL,
    runner          TEXT        NOT NULL,
    code_version    TEXT        NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT edge_forward_sessions_one_per_day UNIQUE (deployment_id, session_date),
    CONSTRAINT edge_forward_sessions_trade_iff_traded CHECK (traded = (trade IS NOT NULL)),
    CONSTRAINT edge_forward_sessions_flat_is_zero CHECK (traded OR session_return = 0),
    CONSTRAINT edge_forward_sessions_return_sane
        CHECK (session_return = session_return AND abs(session_return) < 1),
    CONSTRAINT edge_forward_sessions_data_named
        CHECK (jsonb_typeof(data_ref) = 'array' AND jsonb_array_length(data_ref) >= 1),
    CONSTRAINT edge_forward_sessions_runner_named CHECK (length(btrim(runner)) > 0),
    CONSTRAINT edge_forward_sessions_code_named CHECK (length(btrim(code_version)) > 0)
);

-- How late a session may still be recorded (†, EDGE_PROCESS §9).
CREATE OR REPLACE FUNCTION edge_forward_late_days() RETURNS INTEGER
    LANGUAGE sql IMMUTABLE AS $$ SELECT 10 $$;

CREATE OR REPLACE FUNCTION edge_forward_sessions_admit()
RETURNS TRIGGER AS $$
DECLARE
    v_stage  TEXT;
    v_since  TIMESTAMPTZ;
    v_symbol TEXT;
    v_today  DATE := (now() AT TIME ZONE 'UTC')::DATE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION
            'the forward ledger is append-only: a recorded session is never '
            'changed or deleted (row %)', OLD.id
            USING ERRCODE = 'check_violation';
    END IF;
    SELECT stage, since, symbol INTO v_stage, v_since, v_symbol
      FROM edge_deployments WHERE id = NEW.deployment_id;
    IF v_stage IS DISTINCT FROM 'forward' THEN
        RAISE EXCEPTION
            'deployment % is at stage %, not forward: forward sessions are '
            'observed only while it is forward', NEW.deployment_id, COALESCE(v_stage, '<missing>')
            USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.symbol IS DISTINCT FROM v_symbol THEN
        RAISE EXCEPTION
            'deployment % trades %, not %', NEW.deployment_id, v_symbol, NEW.symbol
            USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.session_date <= (v_since AT TIME ZONE 'UTC')::DATE THEN
        RAISE EXCEPTION
            'session % is not after the day deployment % entered forward (%)',
            NEW.session_date, NEW.deployment_id, (v_since AT TIME ZONE 'UTC')::DATE
            USING ERRCODE = 'check_violation';
    END IF;
    NEW.recorded_at := now();
    IF NEW.session_date >= v_today THEN
        RAISE EXCEPTION
            'session % is not over yet (today is % UTC): a session is recorded '
            'once it has happened', NEW.session_date, v_today
            USING ERRCODE = 'check_violation';
    END IF;
    IF v_today - NEW.session_date > edge_forward_late_days() THEN
        RAISE EXCEPTION
            'session % is % days old: a forward session is recorded as it '
            'happens (within % days), and a later one is a backfill',
            NEW.session_date, v_today - NEW.session_date, edge_forward_late_days()
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_edge_forward_sessions_admit ON edge_forward_sessions;
CREATE TRIGGER trg_edge_forward_sessions_admit
    BEFORE INSERT OR UPDATE OR DELETE ON edge_forward_sessions
    FOR EACH ROW EXECUTE FUNCTION edge_forward_sessions_admit();

-- The counter follows the ledger. SECURITY DEFINER: the runner's role writes
-- ledger rows but has no UPDATE on deployments.
CREATE OR REPLACE FUNCTION edge_forward_sessions_count()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
    UPDATE edge_deployments
       SET forward_sessions_observed =
           (SELECT count(*)::INTEGER FROM edge_forward_sessions WHERE deployment_id = NEW.deployment_id)
     WHERE id = NEW.deployment_id;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_edge_forward_sessions_count ON edge_forward_sessions;
CREATE TRIGGER trg_edge_forward_sessions_count
    AFTER INSERT ON edge_forward_sessions
    FOR EACH ROW EXECUTE FUNCTION edge_forward_sessions_count();

-- The promotion guard, re-created from 020 with 029's rules added (marked).
CREATE OR REPLACE FUNCTION edge_deployments_promotion_guard()
RETURNS TRIGGER AS $$
DECLARE
    v_class    TEXT;
    v_verdict  TEXT;
    v_selected JSONB;
    v_finished TIMESTAMPTZ;
BEGIN
    -- `accounts.account_class`, matching the FK
    -- (edge_deployments.account_id -> accounts.account_id) and unchanged from 018.
    IF NEW.execution = 'auto' THEN
        SELECT account_class INTO v_class
          FROM accounts WHERE account_id = NEW.account_id;
        IF v_class = 'prop' THEN
            RAISE EXCEPTION
                'execution=auto is refused on prop account % (class=%): the '
                'firm prohibits automated decisions, so a human places every '
                'order on this account', NEW.account_id, v_class
                USING ERRCODE = 'check_violation';
        END IF;
    END IF;

    IF TG_OP = 'INSERT' THEN
        -- New deployments start at the bottom. Inserting straight at 'live'
        -- would walk around every rule below, which is the obvious attack on
        -- any state machine whose transitions are guarded but whose entry
        -- point is not.
        IF NEW.stage <> 'backtest' THEN
            RAISE EXCEPTION
                'a new deployment must start at stage=backtest (got %); the '
                'ladder is climbed, not entered partway up', NEW.stage
                USING ERRCODE = 'check_violation';
        END IF;
        -- 029: it trades exactly what its search selected, once that search
        -- has finished.
        SELECT selected_params, finished_at INTO v_selected, v_finished
          FROM edge_experiments WHERE id = NEW.experiment_id;
        IF NOT FOUND THEN
            -- Said here, before NOT NULL or the foreign key word it worse.
            RAISE EXCEPTION
                'experiment % is not present: a deployment deploys a search',
                NEW.experiment_id
                USING ERRCODE = 'check_violation';
        END IF;
        IF v_finished IS NULL THEN
            RAISE EXCEPTION
                'experiment % hasn''t finished: a running search has no verdict to deploy',
                NEW.experiment_id
                USING ERRCODE = 'check_violation';
        END IF;
        IF v_selected IS NULL THEN
            RAISE EXCEPTION
                'experiment % has no selected parameters on record (searched '
                'before 029, or written by hand): nothing to deploy',
                NEW.experiment_id
                USING ERRCODE = 'check_violation';
        END IF;
        IF NEW.params_snapshot IS DISTINCT FROM v_selected THEN
            RAISE EXCEPTION
                'params_snapshot % is not what experiment % selected (%): a '
                'deployment trades exactly what was searched',
                NEW.params_snapshot, NEW.experiment_id, v_selected
                USING ERRCODE = 'check_violation';
        END IF;
        -- 029: no deployment is born with evidence.
        IF NEW.forward_sessions_observed <> 0 THEN
            RAISE EXCEPTION
                'a new deployment observes 0 forward sessions (got %): sessions '
                'are counted from the forward ledger, edge_forward_sessions',
                NEW.forward_sessions_observed
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    -- ── 029: what is deployed is frozen from creation ───────────────────────
    -- The object traded and the object tested must be provably the same; a
    -- different one is a new deployment.
    IF NEW.params_snapshot IS DISTINCT FROM OLD.params_snapshot
       OR NEW.experiment_id IS DISTINCT FROM OLD.experiment_id
       OR NEW.account_id   IS DISTINCT FROM OLD.account_id
       OR NEW.symbol       IS DISTINCT FROM OLD.symbol THEN
        RAISE EXCEPTION
            'deployment % is frozen: its experiment, account, symbol and '
            'params_snapshot cannot change; create a new deployment', OLD.id
            USING ERRCODE = 'check_violation';
    END IF;

    -- ── 029: the observed count is the forward ledger's ─────────────────────
    -- Not a number anyone can write: a change must leave it equal to the
    -- ledger's row count for this deployment.
    IF NEW.forward_sessions_observed IS DISTINCT FROM OLD.forward_sessions_observed
       AND NEW.forward_sessions_observed IS DISTINCT FROM
           (SELECT count(*)::INTEGER FROM edge_forward_sessions WHERE deployment_id = NEW.id) THEN
        RAISE EXCEPTION
            'forward_sessions_observed is counted from edge_forward_sessions '
            '(% rows for deployment %), not set: got %',
            (SELECT count(*) FROM edge_forward_sessions WHERE deployment_id = NEW.id),
            NEW.id, NEW.forward_sessions_observed
            USING ERRCODE = 'check_violation';
    END IF;

    -- ── 029: `since` is stamped here, on every stage change ─────────────────
    -- It is when the deployment entered its stage. A same-stage update can't
    -- move it: moving the forward start earlier would admit older sessions.
    IF NEW.stage IS DISTINCT FROM OLD.stage THEN
        NEW.since := now();
    ELSIF NEW.since IS DISTINCT FROM OLD.since THEN
        RAISE EXCEPTION
            'since is stamped when the stage changes (% -> % refused)', OLD.since, NEW.since
            USING ERRCODE = 'check_violation';
    END IF;

    -- ── THE FREEZE, now evaluated on EVERY update ───────────────────────────
    -- Hoisted out of the same-stage branch (defect 1). Once a deployment has
    -- left backtest its declared requirement is immutable, whether or not the
    -- same statement also moves the stage. Retirement is exempt: abandoning a
    -- test must never be harder than completing it.
    IF OLD.stage <> 'backtest' AND NEW.stage <> 'retired'
       AND NEW.forward_sessions_required IS DISTINCT FROM OLD.forward_sessions_required THEN
        RAISE EXCEPTION
            'forward_sessions_required is frozen once a deployment leaves '
            'backtest (% -> %): it is declared in advance or it is not '
            'evidence', OLD.forward_sessions_required, NEW.forward_sessions_required
            USING ERRCODE = 'check_violation';
    END IF;

    IF NEW.stage = OLD.stage THEN
        RETURN NEW;
    END IF;

    -- Retirement is always available, needs no approval, and is the only
    -- transition anything automatic may perform. Demotion must be cheaper than
    -- promotion, or the system acquires a bias toward staying deployed.
    IF NEW.stage = 'retired' THEN
        RETURN NEW;
    END IF;

    -- ...and it is terminal. Re-deploying is a new decision and deserves a new
    -- row with its own evidence, not a quiet un-retirement of the old one.
    IF OLD.stage = 'retired' THEN
        RAISE EXCEPTION
            'retired is terminal; create a new deployment rather than '
            'un-retiring this one'
            USING ERRCODE = 'check_violation';
    END IF;

    IF OLD.stage = 'backtest' AND NEW.stage = 'forward' THEN
        SELECT verdict INTO v_verdict
          FROM edge_experiments WHERE id = NEW.experiment_id;
        IF v_verdict IS DISTINCT FROM 'USABLE' THEN
            RAISE EXCEPTION
                'backtest->forward requires the experiment verdict to be '
                'USABLE (experiment % is %)', NEW.experiment_id,
                COALESCE(v_verdict, '<missing>')
                USING ERRCODE = 'check_violation';
        END IF;
        IF NEW.forward_sessions_required IS NULL THEN
            RAISE EXCEPTION
                'backtest->forward requires forward_sessions_required to be '
                'declared in advance'
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.stage = 'forward' AND NEW.stage = 'live' THEN
        -- Fail closed on an unknown requirement (defect 3). `observed < NULL`
        -- is NULL, not TRUE, so without this the exception never fires and a
        -- NULL requirement is a complete bypass.
        IF OLD.forward_sessions_required IS NULL THEN
            RAISE EXCEPTION
                'forward->live refused: forward_sessions_required is NULL, so '
                'there is no declared requirement to have met'
                USING ERRCODE = 'check_violation';
        END IF;
        -- Measure against the requirement declared IN ADVANCE (defect 2).
        IF NEW.forward_sessions_observed IS NULL
           OR NEW.forward_sessions_observed < OLD.forward_sessions_required THEN
            RAISE EXCEPTION
                'forward->live requires % forward session(s), only % observed',
                OLD.forward_sessions_required,
                COALESCE(NEW.forward_sessions_observed::TEXT, 'NULL')
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    -- Everything else — skipping a rung, or climbing back down to anything but
    -- retired — is refused. There is no legitimate backtest->live.
    RAISE EXCEPTION
        'illegal stage transition % -> %; the ladder is backtest -> forward -> '
        'live, and any stage may go to retired', OLD.stage, NEW.stage
        USING ERRCODE = 'check_violation';
END;
$$ LANGUAGE plpgsql;

COMMENT ON TABLE edge_forward_sessions IS
    'The forward ledger (EDGE_PROCESS §5): one row per session a forward '
    'runner observed, append-only, recorded as it happens; '
    'edge_deployments.forward_sessions_observed is its row count.';

-- ---------------------------------------------------------------------------
-- Privileges: the research role reads deployments and writes the ledger.
-- ---------------------------------------------------------------------------
-- (029's GRANTs to fks_backtest are left out: this fixture has no roles.)

-- ── 030_edge_monitor.sql ──
-- (verbatim, without the psql meta-commands.)
-- =============================================================================
-- 030_edge_monitor.sql — forward judged against the holdout, and an edge
-- retired when its sessions stop behaving like its backtest
-- =============================================================================
-- EDGE_PROCESS §5 and §7. 029 counts forward sessions but never looks at
-- them: a deployment whose 20 forward sessions all lost could go live like
-- one that matched its backtest, and nothing watched an edge decay.
--
-- THE REFERENCE. edge_experiments.holdout_returns: the selected
-- parameters' session returns over the holdout, in order, as the search
-- scored them (the one series nobody selected on). The search job writes
-- it; 029 freezes it with the rest of a finished row. μ and σ below are its
-- mean and sample standard deviation.
--
-- THE FORWARD BAND (§5). Once the first forward_sessions_required (n)
-- sessions are in, their mean must not fall below
--     floor = μ − 1.645† · σ / √n
-- the 5th percentile of an n-session mean drawn from the holdout (the
-- normal approximation of the bootstrap band: one the database can compute
-- and anyone can check). Evaluated ONCE, on exactly those n sessions: a band
-- re-tested every day fails a sound edge by chance sooner or later.
--
-- THE CUSUM (§7). From the first forward session on, through live: a
-- one-sided lower CUSUM of the standardized session returns,
--     z_t = (x_t − μ) / σ,    S_t = max(0, S_{t−1} − z_t − k),
-- alarming when S_t > h. k is half the holdout's per-session Sharpe (μ/σ:
-- the shift from the backtested edge to none), kept within [0.025, 0.5]†;
-- h is set so that sessions performing exactly like the holdout raise a
-- false alarm once in 500† sessions on average (Siegmund's approximation;
-- checked by simulation for normal and orb-like sessions, 2026-11-02).
-- The cost of that rate: an edge that halves takes on the order of 100-300
-- sessions to catch. Per-session P&L can't do better; mechanics checks are
-- what catch a broken bot quickly.
--
-- ON BREACH the deployment is RETIRED in the same transaction as the
-- session that breached; retired_reason gives the test and its numbers,
-- and an edge_monitor_events row is left for the spawner to deliver
-- (edge_decay). Retirement is terminal (018): testing the edge again is a
-- new deployment with a fresh forward window. Nothing re-promotes.
--
-- WHERE IT APPLIES. Hypotheses with evidence = 'trades' (028's default).
-- A low-frequency edge is judged on mechanics, not P&L (§7): its health
-- says so and neither test runs.
--
-- THE GATES (the promotion guard, re-created from 029, rules marked 030):
--   • backtest→forward needs a holdout series to judge against;
--   • forward→live needs the band to have PASSED.
--
-- THE LEDGER THROUGH LIVE. The forward runner keeps replaying the model
-- once a deployment is live: the model's sessions are what the CUSUM
-- watches. A row records the stage it was observed at (the database sets
-- it), and forward_sessions_observed counts the forward rows only.
--
-- Idempotent. Prerequisites: 017, 018, 020, 024, 025, 028, 029.
-- =============================================================================



-- ── The reference: the holdout, as the search scored it ─────────────────────
ALTER TABLE edge_experiments ADD COLUMN IF NOT EXISTS holdout_returns DOUBLE PRECISION[];
COMMENT ON COLUMN edge_experiments.holdout_returns IS
    'The selected parameters'' session returns over the holdout, in order (the '
    'search job writes it): what forward and live sessions are judged against (030).';

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                    WHERE conname = 'edge_experiments_holdout_returns_finite') THEN
        ALTER TABLE edge_experiments
            ADD CONSTRAINT edge_experiments_holdout_returns_finite
            CHECK (holdout_returns IS NULL
                   OR (cardinality(holdout_returns) >= 1
                       AND NOT ('NaN'::DOUBLE PRECISION = ANY (holdout_returns))
                       AND NOT ('Infinity'::DOUBLE PRECISION = ANY (holdout_returns))
                       AND NOT ('-Infinity'::DOUBLE PRECISION = ANY (holdout_returns))));
    END IF;
END $$;

-- ── The numbers (†, EDGE_PROCESS §9) ────────────────────────────────────────
CREATE OR REPLACE FUNCTION edge_monitor_version() RETURNS TEXT
    LANGUAGE sql IMMUTABLE AS $$ SELECT 'edge-monitor-2026-11-02-proposed'::TEXT $$;

-- The band's one-sided z: its 5th percentile.
CREATE OR REPLACE FUNCTION edge_monitor_band_z() RETURNS DOUBLE PRECISION
    LANGUAGE sql IMMUTABLE AS $$ SELECT 1.6448536269514722::DOUBLE PRECISION $$;

-- Sessions between false alarms, on average, for an edge performing like
-- its holdout.
CREATE OR REPLACE FUNCTION edge_monitor_arl0() RETURNS DOUBLE PRECISION
    LANGUAGE sql IMMUTABLE AS $$ SELECT 500::DOUBLE PRECISION $$;

-- The CUSUM's reference value: half the per-session Sharpe, within bounds.
CREATE OR REPLACE FUNCTION edge_monitor_k(p_mean DOUBLE PRECISION, p_sd DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
    LANGUAGE sql IMMUTABLE AS $$ SELECT least(0.5, greatest(0.025, 0.5 * p_mean / p_sd)) $$;

-- The CUSUM's threshold for reference value k and a mean of p_arl0 sessions
-- between false alarms. Siegmund: ARL0 ≈ (e^u − u − 1) / (2k²) with
-- u = 2k(h + 1.166); solved for u by bisection (e^u − u − 1 rises on u > 0).
CREATE OR REPLACE FUNCTION edge_monitor_cusum_h(p_k DOUBLE PRECISION, p_arl0 DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    v_target DOUBLE PRECISION := 2 * p_k * p_k * p_arl0;
    v_lo     DOUBLE PRECISION := 0;
    v_hi     DOUBLE PRECISION := 60;
    v_mid    DOUBLE PRECISION;
BEGIN
    FOR i IN 1..100 LOOP
        v_mid := (v_lo + v_hi) / 2;
        IF exp(v_mid) - v_mid - 1 < v_target THEN
            v_lo := v_mid;
        ELSE
            v_hi := v_mid;
        END IF;
    END LOOP;
    RETURN greatest(0, (v_lo + v_hi) / 2 / (2 * p_k) - 1.166);
END;
$$;

-- ── The ledger records the stage, and keeps going through live ──────────────
ALTER TABLE edge_forward_sessions
    ADD COLUMN IF NOT EXISTS stage TEXT NOT NULL DEFAULT 'forward';
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                    WHERE conname = 'edge_forward_sessions_stage_known') THEN
        ALTER TABLE edge_forward_sessions
            ADD CONSTRAINT edge_forward_sessions_stage_known
            CHECK (stage IN ('forward', 'live'));
    END IF;
END $$;
COMMENT ON COLUMN edge_forward_sessions.stage IS
    'The deployment''s stage when the session was recorded (set by the database). '
    'forward_sessions_observed counts the forward rows.';

-- Re-created from 029: admitted while forward OR live (030), and the row's
-- stage is the deployment's, whatever the client sent.
CREATE OR REPLACE FUNCTION edge_forward_sessions_admit()
RETURNS TRIGGER AS $$
DECLARE
    v_stage  TEXT;
    v_since  TIMESTAMPTZ;
    v_symbol TEXT;
    v_today  DATE := (now() AT TIME ZONE 'UTC')::DATE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION
            'the forward ledger is append-only: a recorded session is never '
            'changed or deleted (row %)', OLD.id
            USING ERRCODE = 'check_violation';
    END IF;
    SELECT stage, since, symbol INTO v_stage, v_since, v_symbol
      FROM edge_deployments WHERE id = NEW.deployment_id;
    IF v_stage IS NULL OR v_stage NOT IN ('forward', 'live') THEN
        RAISE EXCEPTION
            'deployment % is at stage %: sessions are observed only while it is '
            'forward or live', NEW.deployment_id, COALESCE(v_stage, '<missing>')
            USING ERRCODE = 'check_violation';
    END IF;
    NEW.stage := v_stage;
    IF NEW.symbol IS DISTINCT FROM v_symbol THEN
        RAISE EXCEPTION
            'deployment % trades %, not %', NEW.deployment_id, v_symbol, NEW.symbol
            USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.session_date <= (v_since AT TIME ZONE 'UTC')::DATE THEN
        RAISE EXCEPTION
            'session % is not after the day deployment % entered %s (%)',
            NEW.session_date, NEW.deployment_id, v_stage, (v_since AT TIME ZONE 'UTC')::DATE
            USING ERRCODE = 'check_violation';
    END IF;
    NEW.recorded_at := now();
    IF NEW.session_date >= v_today THEN
        RAISE EXCEPTION
            'session % is not over yet (today is % UTC): a session is recorded '
            'once it has happened', NEW.session_date, v_today
            USING ERRCODE = 'check_violation';
    END IF;
    IF v_today - NEW.session_date > edge_forward_late_days() THEN
        RAISE EXCEPTION
            'session % is % days old: a forward session is recorded as it '
            'happens (within % days), and a later one is a backfill',
            NEW.session_date, v_today - NEW.session_date, edge_forward_late_days()
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ── Health: both tests, computed from the record ────────────────────────────
-- One JSON document per deployment, the same for the guard, the ledger's
-- trigger, the spawner's board and the runner's metrics:
--   applies  false, with `why`, when there's nothing to judge with;
--   holdout  {n, mean, sd};
--   band     {verdict pending|pass|fail, n, required, mean, floor};
--   cusum    {value, peak, k, h, sessions, alarm, alarm_at}.
-- Reads only what the research role may read (SECURITY INVOKER).
CREATE OR REPLACE FUNCTION edge_deployment_health(p_deployment BIGINT)
RETURNS JSONB
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_required INTEGER;
    v_exp      BIGINT;
    v_hold     DOUBLE PRECISION[];
    v_evidence TEXT;
    v_mean     DOUBLE PRECISION;
    v_sd       DOUBLE PRECISION;
    v_n_hold   INTEGER;
    v_k        DOUBLE PRECISION;
    v_h        DOUBLE PRECISION;
    v_s        DOUBLE PRECISION := 0;
    v_peak     DOUBLE PRECISION := 0;
    v_alarm_at DATE;
    v_sessions INTEGER := 0;
    v_n_fwd    INTEGER := 0;
    v_sum_fwd  DOUBLE PRECISION := 0;
    v_band     JSONB;
    r          RECORD;
BEGIN
    SELECT forward_sessions_required, experiment_id INTO v_required, v_exp
      FROM edge_deployments WHERE id = p_deployment;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT e.holdout_returns, COALESCE(h.evidence, 'trades') INTO v_hold, v_evidence
      FROM edge_experiments e LEFT JOIN edge_hypotheses h ON h.id = e.hypothesis_id
     WHERE e.id = v_exp;
    IF v_evidence <> 'trades' THEN
        RETURN jsonb_build_object('applies', false, 'version', edge_monitor_version(),
            'why', 'a low-frequency edge is judged on mechanics, not P&L (EDGE_PROCESS §7)');
    END IF;
    SELECT avg(x), stddev_samp(x), count(*) INTO v_mean, v_sd, v_n_hold
      FROM unnest(v_hold) AS x;
    IF v_hold IS NULL OR v_n_hold < 2 OR v_sd IS NULL OR v_sd = 0 THEN
        RETURN jsonb_build_object('applies', false, 'version', edge_monitor_version(),
            'why', format('experiment %s has no holdout series to judge against (%s)', v_exp,
                          CASE WHEN v_hold IS NULL THEN 'none recorded'
                               WHEN v_n_hold < 2 THEN 'fewer than 2 sessions'
                               ELSE 'every session the same' END));
    END IF;
    v_k := edge_monitor_k(v_mean, v_sd);
    v_h := edge_monitor_cusum_h(v_k, edge_monitor_arl0());

    FOR r IN SELECT session_date, session_return, stage
               FROM edge_forward_sessions
              WHERE deployment_id = p_deployment
              ORDER BY session_date LOOP
        v_sessions := v_sessions + 1;
        v_s := greatest(0, v_s - (r.session_return - v_mean) / v_sd - v_k);
        v_peak := greatest(v_peak, v_s);
        IF v_alarm_at IS NULL AND v_s > v_h THEN
            v_alarm_at := r.session_date;
        END IF;
        IF r.stage = 'forward' THEN
            v_n_fwd := v_n_fwd + 1;
            IF v_n_fwd <= v_required THEN
                v_sum_fwd := v_sum_fwd + r.session_return;
            END IF;
        END IF;
    END LOOP;

    IF v_required IS NULL OR v_n_fwd < v_required THEN
        v_band := jsonb_build_object('verdict', 'pending', 'n', v_n_fwd, 'required', v_required,
            'mean', CASE WHEN v_n_fwd > 0 THEN v_sum_fwd / v_n_fwd END,
            'floor', CASE WHEN v_required IS NOT NULL
                          THEN v_mean - edge_monitor_band_z() * v_sd / sqrt(v_required) END);
    ELSE
        v_band := jsonb_build_object(
            'verdict', CASE WHEN v_sum_fwd / v_required
                                 >= v_mean - edge_monitor_band_z() * v_sd / sqrt(v_required)
                            THEN 'pass' ELSE 'fail' END,
            'n', v_required, 'required', v_required,
            'mean', v_sum_fwd / v_required,
            'floor', v_mean - edge_monitor_band_z() * v_sd / sqrt(v_required));
    END IF;

    RETURN jsonb_build_object(
        'applies', true,
        'version', edge_monitor_version(),
        'holdout', jsonb_build_object('n', v_n_hold, 'mean', v_mean, 'sd', v_sd),
        'band', v_band,
        'cusum', jsonb_build_object('value', v_s, 'peak', v_peak, 'k', v_k, 'h', v_h,
                                    'sessions', v_sessions, 'alarm', v_alarm_at IS NOT NULL,
                                    'alarm_at', v_alarm_at));
END;
$$;

-- ── What the monitor did, for the spawner to deliver ────────────────────────
CREATE TABLE IF NOT EXISTS edge_monitor_events (
    id            BIGSERIAL   PRIMARY KEY,
    deployment_id BIGINT      NOT NULL REFERENCES edge_deployments (id) ON DELETE RESTRICT,
    kind          TEXT        NOT NULL,
    reason        TEXT        NOT NULL,
    health        JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Set once, by the spawner, when it has dispatched the notification.
    delivered_at  TIMESTAMPTZ,

    CONSTRAINT edge_monitor_events_kind_known CHECK (kind IN ('retired'))
);
CREATE INDEX IF NOT EXISTS edge_monitor_events_undelivered_idx
    ON edge_monitor_events (id) WHERE delivered_at IS NULL;

COMMENT ON TABLE edge_monitor_events IS
    'What the edge monitor did (030), oldest first; the spawner delivers each as an '
    'edge_decay notification and sets delivered_at.';

-- A record of what happened: only delivered_at may change, and only once.
CREATE OR REPLACE FUNCTION edge_monitor_events_kept()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'monitor event % is a record and is never deleted', OLD.id
            USING ERRCODE = 'check_violation';
    END IF;
    IF (to_jsonb(NEW) - 'delivered_at') IS DISTINCT FROM (to_jsonb(OLD) - 'delivered_at')
       OR (OLD.delivered_at IS NOT NULL AND NEW.delivered_at IS DISTINCT FROM OLD.delivered_at) THEN
        RAISE EXCEPTION 'monitor event % is kept as written: only delivered_at is set, once', OLD.id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_edge_monitor_events_kept ON edge_monitor_events;
CREATE TRIGGER trg_edge_monitor_events_kept
    BEFORE UPDATE OR DELETE ON edge_monitor_events
    FOR EACH ROW EXECUTE FUNCTION edge_monitor_events_kept();

-- ── The counter, and the monitor, after every recorded session ──────────────
-- Re-created from 029: counts forward rows only (030), then runs both
-- tests and retires on a breach. SECURITY DEFINER: the runner's role can
-- write ledger rows, not deployments or events.
CREATE OR REPLACE FUNCTION edge_forward_sessions_count()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
    v_health JSONB;
    v_why    TEXT;
BEGIN
    UPDATE edge_deployments
       SET forward_sessions_observed =
           (SELECT count(*)::INTEGER FROM edge_forward_sessions
             WHERE deployment_id = NEW.deployment_id AND stage = 'forward')
     WHERE id = NEW.deployment_id;

    v_health := edge_deployment_health(NEW.deployment_id);
    IF NOT COALESCE((v_health ->> 'applies')::BOOLEAN, false) THEN
        RETURN NULL;
    END IF;
    IF v_health -> 'band' ->> 'verdict' = 'fail' THEN
        v_why := format(
            'monitor (%s): the first %s forward sessions averaged %s, below the band floor %s '
            '(holdout mean %s, sd %s, %s sessions)',
            v_health ->> 'version', v_health -> 'band' ->> 'n',
            round((v_health -> 'band' ->> 'mean')::NUMERIC, 6),
            round((v_health -> 'band' ->> 'floor')::NUMERIC, 6),
            round((v_health -> 'holdout' ->> 'mean')::NUMERIC, 6),
            round((v_health -> 'holdout' ->> 'sd')::NUMERIC, 6),
            v_health -> 'holdout' ->> 'n');
    ELSIF (v_health -> 'cusum' ->> 'alarm')::BOOLEAN THEN
        v_why := format(
            'monitor (%s): the CUSUM passed %s on %s after %s sessions (k %s): they stopped '
            'behaving like the holdout (mean %s, sd %s)',
            v_health ->> 'version', round((v_health -> 'cusum' ->> 'h')::NUMERIC, 3),
            v_health -> 'cusum' ->> 'alarm_at', v_health -> 'cusum' ->> 'sessions',
            round((v_health -> 'cusum' ->> 'k')::NUMERIC, 4),
            round((v_health -> 'holdout' ->> 'mean')::NUMERIC, 6),
            round((v_health -> 'holdout' ->> 'sd')::NUMERIC, 6));
    END IF;
    IF v_why IS NULL THEN
        RETURN NULL;
    END IF;
    UPDATE edge_deployments SET stage = 'retired', retired_reason = v_why
     WHERE id = NEW.deployment_id AND stage <> 'retired';
    IF FOUND THEN
        INSERT INTO edge_monitor_events (deployment_id, kind, reason, health)
        VALUES (NEW.deployment_id, 'retired', v_why, v_health);
    END IF;
    RETURN NULL;
END;
$$;

-- The promotion guard, re-created from 029 with 030's rules added (marked).
CREATE OR REPLACE FUNCTION edge_deployments_promotion_guard()
RETURNS TRIGGER AS $$
DECLARE
    v_class    TEXT;
    v_verdict  TEXT;
    v_selected JSONB;
    v_finished TIMESTAMPTZ;
    v_health   JSONB;
BEGIN
    -- `accounts.account_class`, matching the FK
    -- (edge_deployments.account_id -> accounts.account_id) and unchanged from 018.
    IF NEW.execution = 'auto' THEN
        SELECT account_class INTO v_class
          FROM accounts WHERE account_id = NEW.account_id;
        IF v_class = 'prop' THEN
            RAISE EXCEPTION
                'execution=auto is refused on prop account % (class=%): the '
                'firm prohibits automated decisions, so a human places every '
                'order on this account', NEW.account_id, v_class
                USING ERRCODE = 'check_violation';
        END IF;
    END IF;

    IF TG_OP = 'INSERT' THEN
        -- New deployments start at the bottom. Inserting straight at 'live'
        -- would walk around every rule below, which is the obvious attack on
        -- any state machine whose transitions are guarded but whose entry
        -- point is not.
        IF NEW.stage <> 'backtest' THEN
            RAISE EXCEPTION
                'a new deployment must start at stage=backtest (got %); the '
                'ladder is climbed, not entered partway up', NEW.stage
                USING ERRCODE = 'check_violation';
        END IF;
        -- 029: it trades exactly what its search selected, once that search
        -- has finished.
        SELECT selected_params, finished_at INTO v_selected, v_finished
          FROM edge_experiments WHERE id = NEW.experiment_id;
        IF NOT FOUND THEN
            -- Said here, before NOT NULL or the foreign key word it worse.
            RAISE EXCEPTION
                'experiment % is not present: a deployment deploys a search',
                NEW.experiment_id
                USING ERRCODE = 'check_violation';
        END IF;
        IF v_finished IS NULL THEN
            RAISE EXCEPTION
                'experiment % hasn''t finished: a running search has no verdict to deploy',
                NEW.experiment_id
                USING ERRCODE = 'check_violation';
        END IF;
        IF v_selected IS NULL THEN
            RAISE EXCEPTION
                'experiment % has no selected parameters on record (searched '
                'before 029, or written by hand): nothing to deploy',
                NEW.experiment_id
                USING ERRCODE = 'check_violation';
        END IF;
        IF NEW.params_snapshot IS DISTINCT FROM v_selected THEN
            RAISE EXCEPTION
                'params_snapshot % is not what experiment % selected (%): a '
                'deployment trades exactly what was searched',
                NEW.params_snapshot, NEW.experiment_id, v_selected
                USING ERRCODE = 'check_violation';
        END IF;
        -- 029: no deployment is born with evidence.
        IF NEW.forward_sessions_observed <> 0 THEN
            RAISE EXCEPTION
                'a new deployment observes 0 forward sessions (got %): sessions '
                'are counted from the forward ledger, edge_forward_sessions',
                NEW.forward_sessions_observed
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    -- ── 029: what is deployed is frozen from creation ───────────────────────
    -- The object traded and the object tested must be provably the same; a
    -- different one is a new deployment.
    IF NEW.params_snapshot IS DISTINCT FROM OLD.params_snapshot
       OR NEW.experiment_id IS DISTINCT FROM OLD.experiment_id
       OR NEW.account_id   IS DISTINCT FROM OLD.account_id
       OR NEW.symbol       IS DISTINCT FROM OLD.symbol THEN
        RAISE EXCEPTION
            'deployment % is frozen: its experiment, account, symbol and '
            'params_snapshot cannot change; create a new deployment', OLD.id
            USING ERRCODE = 'check_violation';
    END IF;

    -- ── 029: the observed count is the forward ledger's ─────────────────────
    -- Not a number anyone can write: a change must leave it equal to the
    -- ledger's count of forward rows for this deployment (030: forward rows).
    IF NEW.forward_sessions_observed IS DISTINCT FROM OLD.forward_sessions_observed
       AND NEW.forward_sessions_observed IS DISTINCT FROM
           (SELECT count(*)::INTEGER FROM edge_forward_sessions
             WHERE deployment_id = NEW.id AND stage = 'forward') THEN
        RAISE EXCEPTION
            'forward_sessions_observed is counted from edge_forward_sessions '
            '(% forward rows for deployment %), not set: got %',
            (SELECT count(*) FROM edge_forward_sessions
              WHERE deployment_id = NEW.id AND stage = 'forward'),
            NEW.id, NEW.forward_sessions_observed
            USING ERRCODE = 'check_violation';
    END IF;

    -- ── 029: `since` is stamped here, on every stage change ─────────────────
    -- It is when the deployment entered its stage. A same-stage update can't
    -- move it: moving the forward start earlier would admit older sessions.
    IF NEW.stage IS DISTINCT FROM OLD.stage THEN
        NEW.since := now();
    ELSIF NEW.since IS DISTINCT FROM OLD.since THEN
        RAISE EXCEPTION
            'since is stamped when the stage changes (% -> % refused)', OLD.since, NEW.since
            USING ERRCODE = 'check_violation';
    END IF;

    -- ── THE FREEZE, now evaluated on EVERY update ───────────────────────────
    -- Hoisted out of the same-stage branch (defect 1). Once a deployment has
    -- left backtest its declared requirement is immutable, whether or not the
    -- same statement also moves the stage. Retirement is exempt: abandoning a
    -- test must never be harder than completing it.
    IF OLD.stage <> 'backtest' AND NEW.stage <> 'retired'
       AND NEW.forward_sessions_required IS DISTINCT FROM OLD.forward_sessions_required THEN
        RAISE EXCEPTION
            'forward_sessions_required is frozen once a deployment leaves '
            'backtest (% -> %): it is declared in advance or it is not '
            'evidence', OLD.forward_sessions_required, NEW.forward_sessions_required
            USING ERRCODE = 'check_violation';
    END IF;

    IF NEW.stage = OLD.stage THEN
        RETURN NEW;
    END IF;

    -- Retirement is always available, needs no approval, and is the only
    -- transition anything automatic may perform. Demotion must be cheaper than
    -- promotion, or the system acquires a bias toward staying deployed.
    IF NEW.stage = 'retired' THEN
        RETURN NEW;
    END IF;

    -- ...and it is terminal. Re-deploying is a new decision and deserves a new
    -- row with its own evidence, not a quiet un-retirement of the old one.
    IF OLD.stage = 'retired' THEN
        RAISE EXCEPTION
            'retired is terminal; create a new deployment rather than '
            'un-retiring this one'
            USING ERRCODE = 'check_violation';
    END IF;

    IF OLD.stage = 'backtest' AND NEW.stage = 'forward' THEN
        SELECT verdict INTO v_verdict
          FROM edge_experiments WHERE id = NEW.experiment_id;
        IF v_verdict IS DISTINCT FROM 'USABLE' THEN
            RAISE EXCEPTION
                'backtest->forward requires the experiment verdict to be '
                'USABLE (experiment % is %)', NEW.experiment_id,
                COALESCE(v_verdict, '<missing>')
                USING ERRCODE = 'check_violation';
        END IF;
        IF NEW.forward_sessions_required IS NULL THEN
            RAISE EXCEPTION
                'backtest->forward requires forward_sessions_required to be '
                'declared in advance'
                USING ERRCODE = 'check_violation';
        END IF;
        -- 030: a deployment the monitor can't judge doesn't start forward.
        -- (OLD's row is what health reads: the update isn't applied yet, and
        -- what it reads — experiment, requirement — is the same in NEW.)
        v_health := edge_deployment_health(NEW.id);
        IF NOT COALESCE((v_health ->> 'applies')::BOOLEAN, false)
           AND (v_health ->> 'why') NOT LIKE 'a low-frequency edge%' THEN
            RAISE EXCEPTION
                'backtest->forward refused: %; forward sessions would have nothing '
                'to be judged against', v_health ->> 'why'
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.stage = 'forward' AND NEW.stage = 'live' THEN
        -- Fail closed on an unknown requirement (defect 3). `observed < NULL`
        -- is NULL, not TRUE, so without this the exception never fires and a
        -- NULL requirement is a complete bypass.
        IF OLD.forward_sessions_required IS NULL THEN
            RAISE EXCEPTION
                'forward->live refused: forward_sessions_required is NULL, so '
                'there is no declared requirement to have met'
                USING ERRCODE = 'check_violation';
        END IF;
        -- Measure against the requirement declared IN ADVANCE (defect 2).
        IF NEW.forward_sessions_observed IS NULL
           OR NEW.forward_sessions_observed < OLD.forward_sessions_required THEN
            RAISE EXCEPTION
                'forward->live requires % forward session(s), only % observed',
                OLD.forward_sessions_required,
                COALESCE(NEW.forward_sessions_observed::TEXT, 'NULL')
                USING ERRCODE = 'check_violation';
        END IF;
        -- 030: and the forward sessions must look like the backtest's.
        v_health := edge_deployment_health(NEW.id);
        IF COALESCE((v_health ->> 'applies')::BOOLEAN, false) THEN
            IF v_health -> 'band' ->> 'verdict' IS DISTINCT FROM 'pass' THEN
                RAISE EXCEPTION
                    'forward->live requires the forward band to have passed (it is %: '
                    'mean % against a floor of %)', v_health -> 'band' ->> 'verdict',
                    v_health -> 'band' ->> 'mean', v_health -> 'band' ->> 'floor'
                    USING ERRCODE = 'check_violation';
            END IF;
        ELSIF (v_health ->> 'why') NOT LIKE 'a low-frequency edge%' THEN
            RAISE EXCEPTION
                'forward->live refused: %', v_health ->> 'why'
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    -- Everything else — skipping a rung, or climbing back down to anything but
    -- retired — is refused. There is no legitimate backtest->live.
    RAISE EXCEPTION
        'illegal stage transition % -> %; the ladder is backtest -> forward -> '
        'live, and any stage may go to retired', OLD.stage, NEW.stage
        USING ERRCODE = 'check_violation';
END;
$$ LANGUAGE plpgsql;

COMMENT ON TABLE edge_forward_sessions IS
    'The forward ledger (EDGE_PROCESS §5, §7): one row per session a runner replayed, '
    'append-only, recorded as it happens, through forward and live; '
    'edge_deployments.forward_sessions_observed counts its forward rows, and '
    'edge_deployment_health() judges them (030).';
