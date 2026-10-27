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
-- + 017/018/020/024 (the edge ladder).
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
