"""生成一份可直接导入的示例备份清单，便于网页一键演示。

结构（盘 1 MiB = 256 块）：
    base                 块0=A 块1=B 块2=9 块200=Z
    └─ snap-1            块1=b（覆盖） 块2=全零（显式改写，覆盖 base 的 9）
       ├─ snap-2         块0=C
       │  └─ snap-4      块5=E
       └─ snap-3         块3=D

snap-1 是非根且有子层，适合演示“撤除”；snap-2 亦有一个子层。
"""

from __future__ import annotations

import base64

BLOCK_SIZE = 4096


def _fill(byte: bytes) -> str:
    """用单字节填满 4 KiB 并 base64 编码。"""
    return base64.b64encode(byte * BLOCK_SIZE).decode("ascii")


def build_sample_manifest() -> dict:
    return {
        "disk_size": 1024 * 1024,
        "layers": [
            {"id": "base", "parent": None, "blocks": {
                "0": _fill(b"A"), "1": _fill(b"B"), "2": _fill(b"\x39"),
                "200": _fill(b"Z")}},
            {"id": "snap-1", "parent": "base", "blocks": {
                "1": _fill(b"b"), "2": _fill(b"\x00")}},
            {"id": "snap-2", "parent": "snap-1", "blocks": {
                "0": _fill(b"C")}},
            {"id": "snap-4", "parent": "snap-2", "blocks": {
                "5": _fill(b"E")}},
            {"id": "snap-3", "parent": "snap-1", "blocks": {
                "3": _fill(b"D")}},
        ],
    }


# --------------------------------------------------------------------- #
# 坏块演示场景
#
# 拓扑（盘 64 KiB = 16 块；先以合法清单导入，再由演示接口篡改层文件制造静置损坏）：
#
#   base   块0=01 块1=02(随后篡成短块) 块2=09 块7=07
#   └─ mid  块1=12(覆盖 base 坏块1) 块2=显式全零(随后篡坏) 块4=14(随后篡成非法 base64)
#      ├─ left  块0=1A 块2=显式全零(覆盖 mid 坏块2) 块4=1F(覆盖 mid 坏块4)
#      │  └─ leaf 块5=1C
#      └─ right 块2=显式全零(覆盖 mid 坏块2) 块3=1B（不覆盖 mid 坏块4）
#
# 预期：
#   - 下载 base：需要自己的坏块1 -> 拒绝（来源 base，块1）
#   - 下载 mid：需要自己的坏块2/4 -> 拒绝（来源 mid）
#   - 下载 right：需要 mid 的坏块4 且无覆盖 -> 拒绝（来源 mid，块4）
#   - 下载 left/leaf：所有坏块都被下层自有块覆盖（含显式全零）-> 正常
#   - 撤除 mid：right 会继承坏块4 -> 整次拒绝，快照树/层文件原样不动
#   - 撤除 left：left 自身完好，leaf 合并其好块 -> 允许，leaf 镜像逐字节一致
# --------------------------------------------------------------------- #
CORRUPT_DEMO_DISK_SIZE = 64 * 1024

# 损坏计划：层 id -> {块号: "short"（解码后不足 4 KiB）或 "badbase64"（非法 Base64）}
CORRUPTION_PLAN = {
    "base": {1: "short"},
    "mid": {2: "short", 4: "badbase64"},
}


def build_corrupt_demo_manifest() -> dict:
    """坏块演示用的【合法】清单（损坏由导入后的注入步骤制造）。"""
    return {
        "disk_size": CORRUPT_DEMO_DISK_SIZE,
        "layers": [
            {"id": "base", "parent": None, "blocks": {
                "0": _fill(b"\x01"), "1": _fill(b"\x02"),
                "2": _fill(b"\x09"), "7": _fill(b"\x07")}},
            {"id": "mid", "parent": "base", "blocks": {
                "1": _fill(b"\x12"), "2": _fill(b"\x00"), "4": _fill(b"\x14")}},
            {"id": "left", "parent": "mid", "blocks": {
                "0": _fill(b"\x1a"), "2": _fill(b"\x00"), "4": _fill(b"\x1f")}},
            {"id": "leaf", "parent": "left", "blocks": {
                "5": _fill(b"\x1c")}},
            {"id": "right", "parent": "mid", "blocks": {
                "2": _fill(b"\x00"), "3": _fill(b"\x1b")}},
        ],
    }


def corrupt_payload(kind: str) -> str:
    """生成写入层文件的坏块载荷。"""
    if kind == "short":
        # 合法 Base64，但解码后只有 5 字节，不是 4 KiB
        return base64.b64encode(b"short").decode("ascii")
    if kind == "badbase64":
        # 含 Base64 字母表外字符，严格解码必失败
        return "@@@not-valid-base64@@@"
    raise ValueError(f"未知损坏类型：{kind}")
