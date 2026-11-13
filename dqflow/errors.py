"""统一的命令失败类型。"""

from __future__ import annotations


class DQError(Exception):
    """业务可预期错误。

    category 对应 REQUIREMENTS 中的错误分类，决定退出码:
    argument / input / conflict / not_found -> 2
    storage -> 1
    """

    def __init__(self, category: str, message: str) -> None:
        super().__init__(message)
        self.category = category
        self.message = message
