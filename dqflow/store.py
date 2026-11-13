"""SQLite 持久化与判定引擎。

设计要点:
- 每次记录处理一个即时事务( BEGIN IMMEDIATE .. COMMIT ), 判定、业务状态与
  进度在同一事务内原子生效, 中断后可凭库内快照从 processed 继续。
- decisions 不可变; parcels 保存每包裹最高上报修订的首次上报值与最近一次
  有效判定; current_records 是当前合格记录, (carrier, tracking_no) 唯一。
- 运行接收时把输入字节快照与规范化规则一并落库, 恢复不再依赖原文件。
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any

from .errors import DQError
from .validation import (
    canonical_rules,
    is_identifier,
    parse_bounded_int,
    parse_csv_batch,
    parse_fixes,
    request_identity,
    valid_parcel_id,
    valid_tracking_no,
)

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS rule_versions (
        version TEXT PRIMARY KEY,
        canonical TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        status TEXT NOT NULL,
        identity_hash TEXT NOT NULL,
        rule_version TEXT NOT NULL,
        rules_json TEXT NOT NULL,
        received_seq INTEGER NOT NULL,
        total INTEGER NOT NULL,
        processed INTEGER NOT NULL DEFAULT 0,
        payload_kind TEXT NOT NULL,
        payload BLOB NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS parcels (
        parcel_id TEXT PRIMARY KEY,
        highest_revision INTEGER NOT NULL,
        first_carrier TEXT NOT NULL,
        first_tracking_no TEXT NOT NULL,
        first_weight_g TEXT NOT NULL,
        first_run_id TEXT NOT NULL,
        first_row INTEGER NOT NULL,
        latest_valid_run_id TEXT,
        latest_valid_row INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS current_records (
        parcel_id TEXT PRIMARY KEY,
        revision INTEGER NOT NULL,
        carrier TEXT NOT NULL,
        tracking_no TEXT NOT NULL,
        weight_g TEXT NOT NULL,
        loaded_run_id TEXT NOT NULL,
        loaded_row INTEGER NOT NULL
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_current_tracking
        ON current_records(carrier, tracking_no)
    """,
    """
    CREATE TABLE IF NOT EXISTS decisions (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        row INTEGER NOT NULL,
        parcel_id TEXT,
        revision TEXT,
        carrier TEXT,
        tracking_no TEXT,
        weight_g TEXT,
        status TEXT NOT NULL,
        reasons TEXT NOT NULL DEFAULT '[]',
        origin_run_id TEXT,
        origin_row INTEGER,
        parent_run_id TEXT,
        parent_row INTEGER,
        blocked_run_id TEXT,
        blocked_row INTEGER,
        rule_version TEXT NOT NULL,
        UNIQUE(run_id, row)
    )
    """,
]

SIX_COUNTS = (
    "loaded",
    "rejected",
    "invalid_identity",
    "stale",
    "duplicate",
    "conflict",
)


def connect(db_path: str, *, writable: bool = True) -> sqlite3.Connection:
    """打开数据库并确保 schema 就绪。

    writable=False 用于查询命令: 不设 busy_timeout、不主动加锁, 纯 SELECT
    在 WAL 下可与写事务并发, 读不到锁错误; 写连接设置 busy_timeout 以容忍
    与其他写事务之间短暂的锁等待(调用方保证同一时刻只有一个写命令)。
    """
    try:
        conn = sqlite3.connect(
            ":memory:" if (not writable and not os.path.exists(db_path)) else db_path
        )
    except sqlite3.Error as exc:
        raise DQError("storage", f"cannot open database {db_path}: {exc}") from exc
    conn.row_factory = sqlite3.Row
    # 自动提交模式, 事务边界全部显式管理。
    conn.isolation_level = None
    in_memory = not writable and not os.path.exists(db_path)
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        if writable:
            # WAL 让写事务与只读查询并发, 各自看到一致的已提交状态。
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
        if writable or in_memory:
            for statement in SCHEMA_STATEMENTS:
                conn.execute(statement)
    except sqlite3.Error as exc:
        conn.close()
        raise DQError("storage", f"cannot initialize database: {exc}") from exc
    return conn


def _ref(run_id: str | None, row: int | None) -> dict[str, Any] | None:
    if run_id is None:
        return None
    return {"run_id": run_id, "row": row}


def _rules_of(run: sqlite3.Row) -> dict[str, Any]:
    return json.loads(run["rules_json"])


# --------------------------------------------------------------------------- #
# 接收
# --------------------------------------------------------------------------- #


def receive_ingest(
    conn: sqlite3.Connection,
    run_id: str,
    csv_bytes: bytes,
    rules: dict[str, Any],
) -> sqlite3.Row:
    """结构校验已在库外完成; 此处完成运行身份持久化(接收事务)。"""
    records = parse_csv_batch(csv_bytes)
    identity = request_identity("ingest", csv_bytes, rules)
    return _receive_run(
        conn,
        run_id=run_id,
        kind="ingest",
        identity=identity,
        rules=rules,
        total=len(records),
        payload_kind="csv",
        payload=csv_bytes,
        extra_check=None,
    )


def receive_replay(
    conn: sqlite3.Connection,
    run_id: str,
    fix_bytes: bytes,
    rules: dict[str, Any],
    fixes: list[dict[str, Any]],
) -> sqlite3.Row:
    identity = request_identity("replay", fix_bytes, rules)

    def extra_check(tx: sqlite3.Connection) -> None:
        _validate_replay_targets(tx, fixes)

    return _receive_run(
        conn,
        run_id=run_id,
        kind="replay",
        identity=identity,
        rules=rules,
        total=len(fixes),
        payload_kind="fixes",
        payload=fix_bytes,
        extra_check=extra_check,
    )


def _validate_replay_targets(
    conn: sqlite3.Connection, fixes: list[dict[str, Any]]
) -> None:
    seen: set[tuple[str, int]] = set()
    for index, fix in enumerate(fixes, start=1):
        key = (fix["run_id"], fix["row"])
        if key in seen:
            raise DQError(
                "conflict",
                f"fix item {index} repeats target {fix['run_id']}#{fix['row']}",
            )
        seen.add(key)
        target = conn.execute(
            "SELECT * FROM decisions WHERE run_id=? AND row=?",
            (fix["run_id"], fix["row"]),
        ).fetchone()
        if target is None:
            raise DQError(
                "conflict",
                f"fix item {index} references unknown decision "
                f"{fix['run_id']}#{fix['row']}",
            )
        if target["status"] != "rejected":
            raise DQError(
                "conflict",
                f"fix item {index} target status is {target['status']}, "
                "only rejected decisions can be replayed",
            )
        parcel = conn.execute(
            "SELECT * FROM parcels WHERE parcel_id=?",
            (target["parcel_id"],),
        ).fetchone()
        if parcel is None:
            raise DQError(
                "conflict",
                f"fix item {index} target has no parcel state",
            )
        if (
            parcel["latest_valid_run_id"] != fix["run_id"]
            or parcel["latest_valid_row"] != fix["row"]
        ):
            raise DQError(
                "conflict",
                f"fix item {index} target is not the latest valid decision "
                f"of parcel {target['parcel_id']}",
            )


def _receive_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    kind: str,
    identity: str,
    rules: dict[str, Any],
    total: int,
    payload_kind: str,
    payload: bytes,
    extra_check,
) -> sqlite3.Row:
    if not is_identifier(run_id):
        raise DQError("argument", "invalid run id")
    rules_json = canonical_rules(rules)
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT * FROM runs WHERE run_id=?", (run_id,)
        ).fetchone()
        if existing is not None:
            # 已有 ID 的匹配优先于其他一切重新检查。
            if existing["identity_hash"] != identity:
                raise DQError(
                    "conflict",
                    f"run id {run_id} already exists with a different request",
                )
            conn.execute("COMMIT")
            return existing

        registered = conn.execute(
            "SELECT canonical FROM rule_versions WHERE version=?",
            (rules["version"],),
        ).fetchone()
        if registered is not None and registered["canonical"] != rules_json:
            raise DQError(
                "conflict",
                f"rule version {rules['version']} is bound to a different "
                "configuration",
            )
        if registered is None:
            conn.execute(
                "INSERT INTO rule_versions(version, canonical) VALUES (?, ?)",
                (rules["version"], rules_json),
            )

        open_run = conn.execute(
            "SELECT run_id FROM runs WHERE status='open' LIMIT 1"
        ).fetchone()
        if open_run is not None:
            raise DQError(
                "conflict",
                f"unfinished run {open_run['run_id']} exists; "
                "wait for it to complete",
            )

        if extra_check is not None:
            extra_check(conn)

        counter_row = conn.execute(
            "SELECT value FROM meta WHERE key='run_seq'"
        ).fetchone()
        if counter_row is None:
            next_seq = 1
            conn.execute(
                "INSERT INTO meta(key, value) VALUES ('run_seq', '1')"
            )
        else:
            next_seq = int(counter_row["value"]) + 1
            conn.execute(
                "UPDATE meta SET value=? WHERE key='run_seq'", (str(next_seq),)
            )

        conn.execute(
            """
            INSERT INTO runs(
                run_id, kind, status, identity_hash, rule_version, rules_json,
                received_seq, total, processed, payload_kind, payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            """,
            (
                run_id,
                kind,
                "done" if total == 0 else "open",
                identity,
                rules["version"],
                rules_json,
                next_seq,
                total,
                payload_kind,
                sqlite3.Binary(payload),
            ),
        )
        conn.execute("COMMIT")
    except DQError:
        conn.execute("ROLLBACK")
        raise
    except sqlite3.Error as exc:
        conn.execute("ROLLBACK")
        raise DQError("storage", f"database failure during reception: {exc}") from exc

    return conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()


# --------------------------------------------------------------------------- #
# 逐记录处理
# --------------------------------------------------------------------------- #


def _field_reasons(
    rules: dict[str, Any], carrier: str, tracking: str, weight_s: str
) -> list[str]:
    reasons: list[str] = []
    if carrier not in rules["carriers"]:
        reasons.append("carrier_not_allowed")
    if not valid_tracking_no(tracking):
        reasons.append("invalid_tracking_no")
    weight = parse_bounded_int(weight_s)
    if weight is None:
        reasons.append("invalid_weight_g")
    elif weight > rules["max_weight_g"]:
        reasons.append("weight_over_limit")
    return reasons


def _occupancy(
    conn: sqlite3.Connection, carrier: str, tracking: str, parcel_id: str
):
    return conn.execute(
        """
        SELECT parcel_id, loaded_run_id, loaded_row
          FROM current_records
         WHERE carrier=? AND tracking_no=? AND parcel_id<>?
        """,
        (carrier, tracking, parcel_id),
    ).fetchone()


def _insert_decision(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    row: int,
    fields: list[str] | None,
    status: str,
    reasons: list[str],
    origin: tuple[str, int] | None,
    parent: tuple[str, int] | None,
    blocked: tuple[str, int] | None,
    rule_version: str,
) -> None:
    if fields is None:
        parcel_id = revision = carrier = tracking = weight_s = None
    else:
        parcel_id, revision, carrier, tracking, weight_s = fields
    conn.execute(
        """
        INSERT INTO decisions(
            run_id, row, parcel_id, revision, carrier, tracking_no, weight_g,
            status, reasons,
            origin_run_id, origin_row,
            parent_run_id, parent_row,
            blocked_run_id, blocked_row,
            rule_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            row,
            parcel_id,
            revision,
            carrier,
            tracking,
            weight_s,
            status,
            json.dumps(reasons, ensure_ascii=False),
            origin[0] if origin else None,
            origin[1] if origin else None,
            parent[0] if parent else None,
            parent[1] if parent else None,
            blocked[0] if blocked else None,
            blocked[1] if blocked else None,
            rule_version,
        ),
    )


def _apply_loaded(
    conn: sqlite3.Connection,
    *,
    parcel_id: str,
    revision: int,
    carrier: str,
    tracking: str,
    weight_s: str,
    ref: tuple[str, int],
) -> None:
    """替换当前合格记录; 删除旧行即释放旧运单占用。"""
    conn.execute(
        "DELETE FROM current_records WHERE parcel_id=?", (parcel_id,)
    )
    conn.execute(
        """
        INSERT INTO current_records(
            parcel_id, revision, carrier, tracking_no, weight_g,
            loaded_run_id, loaded_row
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (parcel_id, revision, carrier, tracking, weight_s, ref[0], ref[1]),
    )


def _process_ingest_row(
    conn: sqlite3.Connection, run: sqlite3.Row, row_num: int, fields: list[str]
) -> None:
    run_id = run["run_id"]
    rules = _rules_of(run)
    rule_version = run["rule_version"]
    parcel_id, revision_s, carrier, tracking, weight_s = fields
    ref = (run_id, row_num)

    # 1) 身份字段, 错误按规定顺序收集。
    id_ok = valid_parcel_id(parcel_id)
    revision = parse_bounded_int(revision_s)
    if not id_ok or revision is None:
        reasons: list[str] = []
        if not id_ok:
            reasons.append("invalid_parcel_id")
        if revision is None:
            reasons.append("invalid_revision")
        _insert_decision(
            conn,
            run_id=run_id,
            row=row_num,
            fields=fields,
            status="invalid_identity",
            reasons=reasons,
            origin=None,
            parent=None,
            blocked=None,
            rule_version=rule_version,
        )
        return

    parcel = conn.execute(
        "SELECT * FROM parcels WHERE parcel_id=?", (parcel_id,)
    ).fetchone()

    # 2) 低于最高修订 -> stale; 三态判断不改动任何业务状态。
    if parcel is not None and revision < parcel["highest_revision"]:
        _insert_decision(
            conn,
            run_id=run_id,
            row=row_num,
            fields=fields,
            status="stale",
            reasons=[],
            origin=None,
            parent=None,
            blocked=None,
            rule_version=rule_version,
        )
        return

    # 3) 等于最高修订 -> 与首次上报原值逐字符串比较。
    if parcel is not None and revision == parcel["highest_revision"]:
        same = (
            carrier == parcel["first_carrier"]
            and tracking == parcel["first_tracking_no"]
            and weight_s == parcel["first_weight_g"]
        )
        _insert_decision(
            conn,
            run_id=run_id,
            row=row_num,
            fields=fields,
            status="duplicate" if same else "conflict",
            reasons=[],
            origin=(parcel["first_run_id"], parcel["first_row"]),
            parent=None,
            blocked=None,
            rule_version=rule_version,
        )
        return

    # 4) 新的最高上报修订: 记录首次上报原值。
    if parcel is None:
        conn.execute(
            """
            INSERT INTO parcels(
                parcel_id, highest_revision,
                first_carrier, first_tracking_no, first_weight_g,
                first_run_id, first_row,
                latest_valid_run_id, latest_valid_row
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                parcel_id,
                revision,
                carrier,
                tracking,
                weight_s,
                run_id,
                row_num,
                run_id,
                row_num,
            ),
        )
    else:
        conn.execute(
            """
            UPDATE parcels
               SET highest_revision=?,
                   first_carrier=?, first_tracking_no=?, first_weight_g=?,
                   first_run_id=?, first_row=?,
                   latest_valid_run_id=?, latest_valid_row=?
             WHERE parcel_id=?
            """,
            (
                revision,
                carrier,
                tracking,
                weight_s,
                run_id,
                row_num,
                run_id,
                row_num,
                parcel_id,
            ),
        )

    reasons = _field_reasons(rules, carrier, tracking, weight_s)
    blocked = None
    if not reasons:
        occupant = _occupancy(conn, carrier, tracking, parcel_id)
        if occupant is not None:
            reasons.append("tracking_in_use")
            blocked = (occupant["loaded_run_id"], occupant["loaded_row"])

    if reasons:
        _insert_decision(
            conn,
            run_id=run_id,
            row=row_num,
            fields=fields,
            status="rejected",
            reasons=reasons,
            origin=ref,
            parent=None,
            blocked=blocked,
            rule_version=rule_version,
        )
        return

    _insert_decision(
        conn,
        run_id=run_id,
        row=row_num,
        fields=fields,
        status="loaded",
        reasons=[],
        origin=ref,
        parent=None,
        blocked=None,
        rule_version=rule_version,
    )
    _apply_loaded(
        conn,
        parcel_id=parcel_id,
        revision=revision,
        carrier=carrier,
        tracking=tracking,
        weight_s=weight_s,
        ref=ref,
    )


def _process_replay_row(
    conn: sqlite3.Connection,
    run: sqlite3.Row,
    row_num: int,
    fix: dict[str, Any],
) -> None:
    run_id = run["run_id"]
    rules = _rules_of(run)
    rule_version = run["rule_version"]

    target = conn.execute(
        "SELECT * FROM decisions WHERE run_id=? AND row=?",
        (fix["run_id"], fix["row"]),
    ).fetchone()
    # 接收时已整单校验; 防御性处理。
    if target is None or target["status"] != "rejected":
        raise DQError(
            "conflict",
            f"replay target {fix['run_id']}#{fix['row']} is no longer valid",
        )
    parcel_id = target["parcel_id"]
    revision_s = target["revision"]
    revision = int(revision_s)
    fields = [
        parcel_id,
        revision_s,
        fix["carrier"],
        fix["tracking_no"],
        fix["weight_g"],
    ]
    ref = (run_id, row_num)

    parcel = conn.execute(
        "SELECT * FROM parcels WHERE parcel_id=?", (parcel_id,)
    ).fetchone()
    origin = (parcel["first_run_id"], parcel["first_row"])

    reasons = _field_reasons(
        rules, fix["carrier"], fix["tracking_no"], fix["weight_g"]
    )
    blocked = None
    if not reasons:
        occupant = _occupancy(
            conn, fix["carrier"], fix["tracking_no"], parcel_id
        )
        if occupant is not None:
            reasons.append("tracking_in_use")
            blocked = (occupant["loaded_run_id"], occupant["loaded_row"])

    if reasons:
        _insert_decision(
            conn,
            run_id=run_id,
            row=row_num,
            fields=fields,
            status="rejected",
            reasons=reasons,
            origin=origin,
            parent=(fix["run_id"], fix["row"]),
            blocked=blocked,
            rule_version=rule_version,
        )
    else:
        _insert_decision(
            conn,
            run_id=run_id,
            row=row_num,
            fields=fields,
            status="loaded",
            reasons=[],
            origin=origin,
            parent=(fix["run_id"], fix["row"]),
            blocked=None,
            rule_version=rule_version,
        )
        _apply_loaded(
            conn,
            parcel_id=parcel_id,
            revision=revision,
            carrier=fix["carrier"],
            tracking=fix["tracking_no"],
            weight_s=fix["weight_g"],
            ref=ref,
        )

    conn.execute(
        """
        UPDATE parcels
           SET latest_valid_run_id=?, latest_valid_row=?
         WHERE parcel_id=?
        """,
        (run_id, row_num, parcel_id),
    )


def _commit_row(
    conn: sqlite3.Connection, run: sqlite3.Row, row_num: int, work, *work_args
) -> None:
    try:
        conn.execute("BEGIN IMMEDIATE")
        work(conn, run, row_num, *work_args)
        conn.execute(
            "UPDATE runs SET processed=? WHERE run_id=?",
            (row_num, run["run_id"]),
        )
        if row_num >= run["total"]:
            conn.execute(
                "UPDATE runs SET status='done' WHERE run_id=?",
                (run["run_id"],),
            )
        conn.execute("COMMIT")
    except DQError:
        conn.execute("ROLLBACK")
        raise
    except sqlite3.Error as exc:
        conn.execute("ROLLBACK")
        raise DQError("storage", f"database failure while processing row: {exc}") from exc


def process_pending(conn: sqlite3.Connection, run: sqlite3.Row) -> sqlite3.Row:
    """从库内快照恢复并提交尚未处理的连续后缀。"""
    # 测试钩子: 仅在显式设置 DQFLOW_ROW_DELAY 时于每条记录提交后休眠,
    # 便于中断验收命中进行中的前缀; 默认无任何额外开销。
    delay_s = float(os.environ.get("DQFLOW_ROW_DELAY", "0") or 0)
    run_id = run["run_id"]
    if run["status"] == "done":
        return run

    if run["kind"] == "ingest":
        records = parse_csv_batch(bytes(run["payload"]))
        for row_num in range(run["processed"] + 1, run["total"] + 1):
            current = conn.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
            fields = records[row_num - 1]
            _commit_row(conn, current, row_num, _process_ingest_row, fields)
            if delay_s:
                time.sleep(delay_s)
    else:
        fixes = parse_fixes(bytes(run["payload"]))
        for row_num in range(run["processed"] + 1, run["total"] + 1):
            current = conn.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
            _commit_row(
                conn, current, row_num, _process_replay_row, fixes[row_num - 1]
            )
            if delay_s:
                time.sleep(delay_s)

    return conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()


def get_run(conn: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM runs WHERE run_id=?", (run_id,)
    ).fetchone()


# --------------------------------------------------------------------------- #
# 查询与输出
# --------------------------------------------------------------------------- #


def build_summary(conn: sqlite3.Connection, run: sqlite3.Row) -> dict[str, Any]:
    counts = {name: 0 for name in SIX_COUNTS}
    rows = conn.execute(
        "SELECT status, COUNT(*) AS n FROM decisions WHERE run_id=? GROUP BY status",
        (run["run_id"],),
    ).fetchall()
    for row in rows:
        counts[row["status"]] = row["n"]
    return {
        "run_id": run["run_id"],
        "kind": run["kind"],
        "status": run["status"],
        "rules": _rules_of(run),
        "total": run["total"],
        "processed": run["processed"],
        "counts": counts,
    }


def _decision_output(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "ref": _ref(row["run_id"], row["row"]),
        "status": row["status"],
        "rule_version": row["rule_version"],
        "input": [
            row["parcel_id"],
            row["revision"],
            row["carrier"],
            row["tracking_no"],
            row["weight_g"],
        ],
        "reasons": json.loads(row["reasons"]),
        "origin": _ref(row["origin_run_id"], row["origin_row"]),
        "parent": _ref(row["parent_run_id"], row["parent_row"]),
        "blocked_by": _ref(row["blocked_run_id"], row["blocked_row"]),
    }


def report_decisions(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM decisions WHERE run_id=? ORDER BY row", (run_id,)
    ).fetchall()
    return [_decision_output(row) for row in rows]


def history(conn: sqlite3.Connection, parcel_id: str) -> list[dict[str, Any]]:
    if not valid_parcel_id(parcel_id):
        raise DQError("argument", f"invalid parcel id: {parcel_id}")
    rows = conn.execute(
        """
        SELECT d.* FROM decisions d
         JOIN runs r ON r.run_id = d.run_id
        WHERE d.parcel_id=?
        ORDER BY r.received_seq, d.row
        """,
        (parcel_id,),
    ).fetchall()
    return [_decision_output(row) for row in rows]


def current_records(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM current_records ORDER BY parcel_id"
    ).fetchall()
    result = []
    for row in rows:
        result.append(
            {
                "parcel_id": row["parcel_id"],
                "revision": row["revision"],
                "carrier": row["carrier"],
                "tracking_no": row["tracking_no"],
                "weight_g": int(row["weight_g"]),
                "decision": _ref(row["loaded_run_id"], row["loaded_row"]),
            }
        )
    return result
