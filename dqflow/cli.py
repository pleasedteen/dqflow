"""命令行入口: python -m dqflow --db DB <command> ..."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from .errors import DQError
from .store import (
    build_summary,
    connect,
    current_records,
    get_run,
    history,
    process_pending,
    receive_ingest,
    receive_replay,
    report_decisions,
)
from .validation import (
    is_identifier,
    parse_csv_batch,
    parse_fixes,
    parse_rules,
    read_file_bytes,
)

EXIT_BY_CATEGORY = {
    "argument": 2,
    "input": 2,
    "conflict": 2,
    "not_found": 2,
    "storage": 1,
}


class _DQArgumentParser(argparse.ArgumentParser):
    """把 argparse 的用法错误转成统一 DQError(argument)。"""

    def error(self, message: str) -> None:  # pragma: no cover - 薄封装
        raise DQError("argument", message)


def build_parser() -> argparse.ArgumentParser:
    parser = _DQArgumentParser(prog="dqflow", add_help=True)
    parser.add_argument("--db", required=True, help="SQLite 数据库路径")
    sub = parser.add_subparsers(dest="command")

    ingest = sub.add_parser("ingest")
    ingest.add_argument("--run", required=True)
    ingest.add_argument("--csv", required=True)
    ingest.add_argument("--rules", required=True)

    replay = sub.add_parser("replay")
    replay.add_argument("--run", required=True)
    replay.add_argument("--fixes", required=True)
    replay.add_argument("--rules", required=True)

    resume = sub.add_parser("resume")
    resume.add_argument("--run", required=True)

    report = sub.add_parser("report")
    report.add_argument("--run", required=True)
    report.add_argument("--summary", action="store_true")

    sub.add_parser("current")

    hist = sub.add_parser("history")
    hist.add_argument("--parcel-id", required=True, dest="parcel_id")

    return parser


def _require_run_id(run_id: str) -> None:
    if not is_identifier(run_id):
        raise DQError("argument", f"invalid run id: {run_id}")


def _load_run(conn, run_id: str):
    _require_run_id(run_id)
    run = get_run(conn, run_id)
    if run is None:
        raise DQError("not_found", f"unknown run: {run_id}")
    return run


def dispatch(args: argparse.Namespace):
    command = args.command
    if command is None:
        raise DQError("argument", "a command is required")

    if command == "ingest":
        _require_run_id(args.run)
        # 所有结构性校验在触碰数据库之前完成。
        csv_bytes = read_file_bytes(args.csv)
        parse_csv_batch(csv_bytes)
        rules = parse_rules(read_file_bytes(args.rules))
        conn = connect(args.db)
        run = receive_ingest(conn, args.run, csv_bytes, rules)
        run = process_pending(conn, run)
        return build_summary(conn, run)

    if command == "replay":
        _require_run_id(args.run)
        fix_bytes = read_file_bytes(args.fixes)
        fixes = parse_fixes(fix_bytes)
        rules = parse_rules(read_file_bytes(args.rules))
        conn = connect(args.db)
        run = receive_replay(conn, args.run, fix_bytes, rules, fixes)
        run = process_pending(conn, run)
        return build_summary(conn, run)

    if command == "resume":
        conn = connect(args.db)
        run = _load_run(conn, args.run)
        run = process_pending(conn, run)
        return build_summary(conn, run)

    if command == "report":
        conn = connect(args.db, writable=False)
        run = _load_run(conn, args.run)
        summary = build_summary(conn, run)
        if not args.summary:
            summary["decisions"] = report_decisions(conn, run["run_id"])
        return summary

    if command == "current":
        conn = connect(args.db, writable=False)
        return current_records(conn)

    if command == "history":
        conn = connect(args.db, writable=False)
        return history(conn, args.parcel_id)

    raise DQError("argument", f"unknown command: {command}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        result = dispatch(args)
    except DQError as exc:
        payload = {"error": exc.category, "message": exc.message}
        sys.stderr.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return EXIT_BY_CATEGORY.get(exc.category, 2)
    sys.stdout.write(json.dumps(result, ensure_ascii=False) + "\n")
    return 0
