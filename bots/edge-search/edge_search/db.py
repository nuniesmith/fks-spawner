"""The research database, through the scoped fks_backtest role (fks 025):
read the hypothesis and how many trials its edge has had, write the
experiment. The guards are the database's (fks 017/024): an experiment that
finished before its hypothesis was registered is refused there too.
"""

from __future__ import annotations

import json

import psycopg

from .search import Hypothesis, SearchError


def load_hypothesis(conn: psycopg.Connection, hypothesis_id: int) -> Hypothesis:
    row = conn.execute(
        "SELECT id, edge_id, idea, symbols, dataset, holdout_share, params, baseline, metric, "
        "       costs, registered_at, status "
        "FROM edge_hypotheses WHERE id = %s",
        (hypothesis_id,),
    ).fetchone()
    if row is None:
        raise SearchError(f"no hypothesis {hypothesis_id}")
    if row[11] != "registered":
        raise SearchError(f"hypothesis {hypothesis_id} was withdrawn; it isn't searched")
    return Hypothesis(
        id=row[0], edge_id=row[1], idea=row[2], symbols=list(row[3]), dataset=row[4],
        holdout_share=float(row[5]), params=list(row[6]), baseline=row[7], metric=row[8],
        costs=dict(row[9]), registered_at=row[10],
    )


def prior_trials(conn: psycopg.Connection, edge_id: str) -> int:
    """Every trial already run for this edge, whatever its hypothesis: the N
    the deflated Sharpe corrects for counts them all."""
    (n,) = conn.execute(
        "SELECT COALESCE(sum(x.n_trials), 0) FROM edge_experiments x "
        "JOIN edge_hypotheses h ON h.id = x.hypothesis_id WHERE h.edge_id = %s",
        (edge_id,),
    ).fetchone()
    return int(n)


def write_experiment(conn: psycopg.Connection, row: dict) -> int:
    cols = list(row)
    values = [json.dumps(v) if isinstance(v, dict) else v for v in row.values()]
    (new_id,) = conn.execute(
        f"INSERT INTO edge_experiments ({', '.join(cols)}) "
        f"VALUES ({', '.join(['%s'] * len(cols))}) RETURNING id",
        values,
    ).fetchone()
    conn.commit()
    return int(new_id)
