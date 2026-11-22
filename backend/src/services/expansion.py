"""Segmented, resumable expansion of menu recipes.

One database segment resolves a frontier of already-expanded paths into the
next path layer.  The planner keeps every segment at no more than
``SEGMENT_PATHS`` returned paths, so a single menu line that is larger than a
segment is split across several segments instead of failing the batch.

Each completed segment is stored in ``recipe_expansion_segments`` keyed by
``(run_id, seq)``.  A retry therefore resumes after the last durable segment
and never counts a path twice.  Terminal rows are aggregated only after every
segment has finished, which lets a cycle found late in a menu line invalidate
materials already produced for that line earlier.
"""

import hashlib
import json
import logging
from decimal import Decimal

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from src.models.tables import expansion_segments
from src.utils.encoding import canonical
from src.utils.time import utc_time

from .recipe_sql import EXPAND_SEGMENT

# At most this many paths are returned by one database segment.
SEGMENT_PATHS = 2000


class ExpansionInterrupted(Exception):
    """Raised when the run lease was lost; work stays checkpointed for a retry."""


def _outdegree(edges):
    counts = {}
    for edge in edges:
        counts[edge["parent_id"]] = counts.get(edge["parent_id"], 0) + 1
    return counts


def _initial_frontier(menu, outdegree):
    frontier, terminals = [], []
    for line_id, item in enumerate(menu, start=1):
        row = {
            "line_id": line_id,
            "node_id": item["recipe_id"],
            "depth": 0,
            "path_ids": str(item["recipe_id"]),
            "quantity": str(item["servings"]),
        }
        # A menu recipe without component rows is an unfinished recipe, exactly
        # as the one-shot recursive query classified its anchor row.
        if outdegree.get(item["recipe_id"], 0):
            frontier.append(row)
        else:
            terminals.append({**row, "kind": "missing"})
    return frontier, terminals


def _chunks(frontier, outdegree):
    """Greedily group frontier rows so each segment returns at most 2000 paths.

    The number of rows a frontier row produces equals its outdegree, so the
    sum of outdegrees is an exact bound on the segment result size.
    """
    chunk, size = [], 0
    for row in frontier:
        width = outdegree.get(row["node_id"], 0)
        if chunk and size + width > SEGMENT_PATHS:
            yield chunk
            chunk, size = [], 0
        chunk.append(row)
        size += width
        if size >= SEGMENT_PATHS:
            yield chunk
            chunk, size = [], 0
    if chunk:
        yield chunk


def _normalise(rows):
    return [
        {
            "line_id": int(row["line_id"]),
            "node_id": int(row["node_id"]),
            "depth": int(row["depth"]),
            "path_ids": str(row["path_ids"]),
            "quantity": format(Decimal(str(row["quantity"])), "f"),
            "kind": row["kind"],
        }
        for row in rows
    ]


def _run_segment(frontier, edges, recipe_count, max_depth, engine):
    with engine.connect() as conn:
        rows = conn.execute(EXPAND_SEGMENT, {
            "frontier": json.dumps(frontier),
            "edges": json.dumps(edges),
            "recipe_count": recipe_count,
            "max_depth": max_depth,
        }).mappings().all()
    result = _normalise(rows)
    if len(result) > SEGMENT_PATHS:
        # The planner guarantees this; a violation would break the per-segment
        # contract rather than silently produce an oversized segment.
        raise RuntimeError(f"展开段超过 {SEGMENT_PATHS} 条路径。")
    return result


def _digest(menu, edges, recipe_count, max_depth):
    return hashlib.sha256(canonical({
        "menu": menu, "edges": edges, "recipe_count": recipe_count,
        "max_depth": max_depth, "segment_paths": SEGMENT_PATHS,
    })).hexdigest()


def _load(engine, run_id, digest):
    """Return the set of durable segment numbers for this run and input."""
    with engine.begin() as conn:
        rows = conn.execute(select(expansion_segments.c.seq, expansion_segments.c.batch_digest)
                            .where(expansion_segments.c.run_id == run_id)).all()
        valid = {seq for seq, value in rows if value == digest}
        if len(valid) != len(rows):
            # A changed input or expansion plan must never mix checkpoints.
            conn.execute(delete(expansion_segments).where(expansion_segments.c.run_id == run_id))
            return set()
    return valid


def _stored_rows(engine, run_id, seq):
    with engine.connect() as conn:
        rows = conn.execute(select(expansion_segments.c.rows).where(
            expansion_segments.c.run_id == run_id, expansion_segments.c.seq == seq)).scalar_one()
    return _normalise(rows)


def _store(engine, run_id, seq, digest, rows):
    try:
        with engine.begin() as conn:
            conn.execute(expansion_segments.insert().values(
                run_id=run_id, seq=seq, batch_digest=digest, row_count=len(rows),
                rows=rows, created_at=utc_time()))
    except IntegrityError:
        # Another worker stored the same deterministic segment first.
        logging.info("Expansion segment %s/%s already checkpointed", run_id, seq)


def expand(menu, edges, recipe_count, engine, *, max_depth, run_id=None, digest=None,
           should_stop=None):
    """Expand the batch and return terminal rows only.

    ``run_id`` enables durable per-segment checkpoints.  ``should_stop`` is
    polled before each segment that still has to run, allowing a worker that
    lost its lease to stop without corrupting the checkpoint stream.
    """
    outdegree = _outdegree(edges)
    frontier, terminals = _initial_frontier(menu, outdegree)
    if run_id is not None:
        digest = digest or _digest(menu, edges, recipe_count, max_depth)
        stored = _load(engine, run_id, digest)
    else:
        digest, stored = None, set()
    seq = 0
    while frontier:
        next_frontier = []
        for chunk in _chunks(frontier, outdegree):
            seq += 1
            if seq in stored:
                rows = _stored_rows(engine, run_id, seq)
            else:
                if should_stop is not None and not should_stop():
                    raise ExpansionInterrupted()
                rows = _run_segment(frontier=chunk, edges=edges, recipe_count=recipe_count,
                                    max_depth=max_depth, engine=engine)
                if run_id is not None:
                    _store(engine, run_id, seq, digest, rows)
            terminals.extend(row for row in rows if row["kind"] is not None)
            next_frontier.extend(row for row in rows if row["kind"] is None)
        frontier = next_frontier
    return terminals
