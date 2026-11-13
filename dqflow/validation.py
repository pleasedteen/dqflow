"""输入文件解析与格式校验。

包括运行/规则标识符、CSV 批次、规则 JSON、修正单 JSON 的严格解析。
所有结构性问题在打开数据库、产生任何业务状态之前抛出 DQError(input)。
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from typing import Any

from .errors import DQError

# 运行 ID 与规则版本: 首字符字母数字, 后续 0..63 个允许字符, 总长 1..64
ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
# 包裹业务键: 总长 1..40
PARCEL_ID_PATTERN = re.compile(r"[A-Z0-9][A-Z0-9-]{0,39}\Z")
# 运单号: 6..32
TRACKING_PATTERN = re.compile(r"[A-Z0-9]{6,32}\Z")
# 承运商: 1..16
CARRIER_PATTERN = re.compile(r"[A-Z][A-Z0-9]{0,15}\Z")

INT_MIN = 1
INT_MAX = 2_147_483_647

CSV_HEADER = ["parcel_id", "revision", "carrier", "tracking_no", "weight_g"]
FIELD_COUNT = 5


def is_identifier(value: str) -> bool:
    # fullmatch: 第一个字符必须是字母数字, 整体长度 1..64。
    return bool(ID_PATTERN.fullmatch(value))


def valid_parcel_id(value: str) -> bool:
    return bool(PARCEL_ID_PATTERN.fullmatch(value))


def valid_tracking_no(value: str) -> bool:
    return bool(TRACKING_PATTERN.fullmatch(value))


def valid_carrier_code(value: str) -> bool:
    return bool(CARRIER_PATTERN.fullmatch(value))


def parse_bounded_int(value: str) -> int | None:
    """按上游整数格式解析: 十进制、无符号、无前导零，范围 1..2147483647。

    格式或范围不符返回 None。
    """
    if not value or not value.isascii() or not value.isdigit():
        return None
    if len(value) > 1 and value[0] == "0":
        return None
    number = int(value)
    if number < INT_MIN or number > INT_MAX:
        return None
    return number


def read_file_bytes(path: str) -> bytes:
    try:
        with open(path, "rb") as handle:
            return handle.read()
    except OSError as exc:
        raise DQError("storage", f"cannot read file {path}: {exc}") from exc


def parse_csv_batch(data: bytes) -> list[list[str]]:
    """解析整批 CSV，返回逻辑记录(每条 5 个原字符串)。

    无 BOM、UTF-8、Excel 方言、严格解析；表头/编码/解析/列数错误整批拒绝。
    """
    if data.startswith(b"\xef\xbb\xbf"):
        raise DQError("input", "CSV must not contain a UTF-8 BOM")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DQError("input", f"CSV is not valid UTF-8: {exc}") from exc

    try:
        reader = csv.reader(io.StringIO(text), dialect="excel", strict=True)
        rows = list(reader)
    except csv.Error as exc:
        raise DQError("input", f"CSV parse error: {exc}") from exc

    if not rows:
        raise DQError("input", "CSV is empty: header row is required")
    header, *records = rows
    if header != CSV_HEADER:
        raise DQError(
            "input",
            "CSV header must be exactly: " + ",".join(CSV_HEADER),
        )
    for index, row in enumerate(records, start=1):
        if len(row) != FIELD_COUNT:
            raise DQError(
                "input",
                f"CSV record {index} has {len(row)} fields, expected {FIELD_COUNT}",
            )
    return records


class _RejectingHook:
    """object_pairs_hook: 拒绝重复对象成员，原样保留 (key, value) 对。"""

    def __call__(self, pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        seen: set[str] = set()
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in seen:
                raise DQError("input", f"duplicate JSON object member: {key}")
            seen.add(key)
            result[key] = value
        return result


def _load_json_object_bytes(data: bytes, what: str) -> Any:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DQError("input", f"{what} is not valid UTF-8: {exc}") from exc
    try:
        return json.loads(text, object_pairs_hook=_RejectingHook())
    except DQError:
        raise
    except json.JSONDecodeError as exc:
        raise DQError("input", f"{what} is not valid JSON: {exc}") from exc


def parse_rules(data: bytes) -> dict[str, Any]:
    """严格解析规则文件并返回规范化规则对象。

    返回 {"version": str, "carriers": sorted list[str], "max_weight_g": int}。
    禁止重复成员、额外字段与错误类型。
    """
    obj = _load_json_object_bytes(data, "rules file")
    if not isinstance(obj, dict):
        raise DQError("input", "rules file must contain a JSON object")

    expected = {"version", "carriers", "max_weight_g"}
    keys = set(obj)
    missing = expected - keys
    if missing:
        raise DQError("input", f"rules file missing fields: {sorted(missing)}")
    extra = keys - expected
    if extra:
        raise DQError("input", f"rules file has extra fields: {sorted(extra)}")

    version = obj["version"]
    if not isinstance(version, str) or not is_identifier(version):
        raise DQError("input", "rules.version must match the run identifier pattern")

    carriers = obj["carriers"]
    if not isinstance(carriers, list) or not carriers:
        raise DQError("input", "rules.carriers must be a non-empty JSON array")
    normalized: list[str] = []
    seen: set[str] = set()
    for item in carriers:
        if not isinstance(item, str) or not valid_carrier_code(item):
            raise DQError(
                "input",
                "each carrier must match [A-Z][A-Z0-9]{0,15}",
            )
        if item in seen:
            raise DQError("input", f"duplicate carrier in rules: {item}")
        seen.add(item)
        normalized.append(item)
    normalized.sort()

    max_weight = obj["max_weight_g"]
    # bool 是 int 的子类，规则要求整数类型，显式拒绝。
    if isinstance(max_weight, bool) or not isinstance(max_weight, int):
        raise DQError("input", "rules.max_weight_g must be a JSON integer")
    if max_weight < INT_MIN or max_weight > INT_MAX:
        raise DQError("input", "rules.max_weight_g out of allowed integer range")

    return {
        "version": version,
        "carriers": normalized,
        "max_weight_g": max_weight,
    }


def canonical_rules(rules: dict[str, Any]) -> str:
    """等价配置的稳定序列化: 排序成员、承运商排序，无多余空白。"""
    return json.dumps(
        rules,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def parse_fixes(data: bytes) -> list[dict[str, Any]]:
    """解析修正单 JSON 数组并做逐字段结构校验。

    业务资格(引用存在、可回放等)由存储层在接收事务中校验。
    """
    value = _load_json_object_bytes(data, "fixes file")
    if not isinstance(value, list):
        raise DQError("input", "fixes file must contain a JSON array")

    fixes: list[dict[str, Any]] = []
    required = {"run_id", "row", "carrier", "tracking_no", "weight_g"}
    for index, item in enumerate(value, start=1):
        if not isinstance(item, dict):
            raise DQError("input", f"fix item {index} must be a JSON object")
        keys = set(item)
        missing = required - keys
        if missing:
            raise DQError(
                "input", f"fix item {index} missing fields: {sorted(missing)}"
            )
        extra = keys - required
        if extra:
            raise DQError(
                "input", f"fix item {index} has extra fields: {sorted(extra)}"
            )
        run_id = item["run_id"]
        if not isinstance(run_id, str) or not is_identifier(run_id):
            raise DQError("input", f"fix item {index} has invalid run_id")
        row = item["row"]
        if isinstance(row, bool) or not isinstance(row, int) or row < 1:
            raise DQError("input", f"fix item {index} row must be a positive integer")
        for name in ("carrier", "tracking_no", "weight_g"):
            if not isinstance(item[name], str):
                raise DQError(
                    "input", f"fix item {index} field {name} must be a string"
                )
        fixes.append(
            {
                "run_id": run_id,
                "row": row,
                "carrier": item["carrier"],
                "tracking_no": item["tracking_no"],
                "weight_g": item["weight_g"],
            }
        )
    return fixes


def request_identity(kind: str, payload: bytes, rules: dict[str, Any]) -> str:
    """运行身份: 命令类型 + 输入字节 + 规范化规则；文件路径不参与。"""
    digest = hashlib.sha256()
    digest.update(kind.encode("ascii"))
    digest.update(b"\0")
    digest.update(payload)
    digest.update(b"\0")
    digest.update(canonical_rules(rules).encode("utf-8"))
    return digest.hexdigest()
