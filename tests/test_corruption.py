"""坏盘块（Base64 非法或解码后不是 4 KiB）处理测试。

损坏只能在合法导入之后通过篡改卷上层文件制造（导入接口本身会严格拒绝坏块），
模拟生产中的静置损坏。场景拓扑见 app/sample.py 的坏块演示说明。
"""

from __future__ import annotations

import base64
import json

import pytest

from app.sample import (
    CORRUPTION_PLAN,
    build_corrupt_demo_manifest,
    corrupt_payload,
)
from app.store import BLOCK_SIZE, BadBlockError, Store, StoreError
from tests.conftest import blk, files_on_disk, make_manifest, manifest_files

SHORT_B64 = base64.b64encode(b"short").decode("ascii")
BAD_B64 = "@@@not-valid-base64@@@"


@pytest.fixture
def corrupted(store):
    """导入合法清单后注入坏块：base 块1 短块；mid 块2 短块、块4 非法 Base64。"""
    store.import_manifest(build_corrupt_demo_manifest())
    plan = {
        lid: {int(idx): kind for idx, kind in blocks.items()}
        for lid, blocks in CORRUPTION_PLAN.items()
    }
    payloads = {"short": corrupt_payload("short"),
                "badbase64": corrupt_payload("badbase64")}
    store.inject_corruption_for_demo(plan, payloads)
    return store


def _tamper(store: Store, layer_id: str, idx: int, value: str) -> None:
    """直接改写某个已导入层文件的单个块（绕过导入校验，模拟静置损坏）。"""
    name = store._manifest["layers"][layer_id]["file"]
    path = store.layers_dir / name
    raw = json.loads(path.read_text("utf-8"))
    raw["blocks"][str(idx)] = value
    path.write_text(json.dumps(raw), encoding="utf-8")
    store._block_cache.pop(name, None)


def _layer_files(store: Store) -> dict:
    return {lid: info["file"] for lid, info in store._manifest["layers"].items()}


# --------------------------------------------------------------------- #
# state：坏块可被发现并展示来源快照与块号
# --------------------------------------------------------------------- #
class TestStateCorruption:
    def test_state_lists_corrupt_blocks(self, corrupted):
        by_id = {l["id"]: l for l in corrupted.state()["layers"]}
        assert by_id["base"]["corrupt_blocks"] == [1]
        assert by_id["mid"]["corrupt_blocks"] == [2, 4]
        assert by_id["left"]["corrupt_blocks"] == []
        assert by_id["leaf"]["corrupt_blocks"] == []
        assert by_id["right"]["corrupt_blocks"] == []

    def test_state_marks_remove_blocked_with_source_and_index(self, corrupted):
        by_id = {l["id"]: l for l in corrupted.state()["layers"]}
        mid = by_id["mid"]
        assert mid["can_remove"] is False
        assert mid["remove_blocked_reason"] is not None
        assert "mid" in mid["remove_blocked_reason"]
        assert "4" in mid["remove_blocked_reason"]  # 块号
        # left 自身完好、leaf 覆盖了 left 的全部块 -> 仍可撤除
        assert by_id["left"]["can_remove"] is True
        assert by_id["left"]["remove_blocked_reason"] is None

    def test_state_detects_tampering_after_process_lived(self, imported):
        # 先正常读一次建立缓存，再篡改文件；缓存必须经 mtime 签名失效
        imported.resolve_image("leaf")
        _tamper(imported, "mid", 1, SHORT_B64)
        by_id = {l["id"]: l for l in imported.state()["layers"]}
        assert by_id["mid"]["corrupt_blocks"] == [1]


# --------------------------------------------------------------------- #
# 下载：需要读坏块即拒绝，禁止祖先补齐；被下层覆盖（含显式全零）不影响
# --------------------------------------------------------------------- #
class TestDownloadCorruption:
    def test_download_layer_with_own_bad_block_rejected(self, corrupted):
        with pytest.raises(BadBlockError) as ei:
            corrupted.resolve_image("base")
        assert ei.value.layer_id == "base"
        assert ei.value.block_index == 1
        assert "base" in str(ei.value) and "块 1" in str(ei.value)

    def test_download_mid_rejected_with_source_and_index(self, corrupted):
        with pytest.raises(BadBlockError) as ei:
            corrupted.resolve_image("mid")
        assert ei.value.layer_id == "mid"
        assert ei.value.block_index in (2, 4)
        assert "mid" in str(ei.value) and f"块 {ei.value.block_index}" in str(ei.value)

    def test_bad_block_in_parent_when_child_overrides_is_fine(self, corrupted):
        # base 块1 坏；mid 在块1 有自有好块，left/leaf 经 mid 覆盖该坏块 -> 读块1 不受影响
        for lid in ["left", "leaf"]:
            img = corrupted.resolve_image(lid)
            assert img[BLOCK_SIZE:2 * BLOCK_SIZE] == bytes([0x12]) * BLOCK_SIZE
        # mid 自身还持有坏块2/4，下载 mid 仍须拒绝
        with pytest.raises(BadBlockError):
            corrupted.resolve_image("mid")

    def test_explicit_zero_block_counts_as_coverage(self, store):
        # 只有 mid 块2 损坏；left/right 都在块2 上写了显式全零 -> 两个子层下载均成功，
        # 块2 解析为全零，证明显式全零块算覆盖、不会去读父层坏块。
        store.import_manifest(make_manifest(layers=[
            {"id": "base", "parent": None, "blocks": {"0": blk(1), "2": blk(9)}},
            {"id": "mid", "parent": "base", "blocks": {"2": blk(0)}},
            {"id": "left", "parent": "mid", "blocks": {"2": blk(0)}},
            {"id": "right", "parent": "mid", "blocks": {"2": blk(0), "3": blk(4)}},
        ]))
        _tamper(store, "mid", 2, SHORT_B64)
        for lid in ("left", "right"):
            img = store.resolve_image(lid)
            assert img[2 * BLOCK_SIZE:3 * BLOCK_SIZE] == bytes(BLOCK_SIZE)
        # mid 自身在块2 上没有下层覆盖 -> 拒绝
        with pytest.raises(BadBlockError) as ei:
            store.resolve_image("mid")
        assert ei.value.block_index == 2

    def test_demo_manifest_zero_coverage_of_bad_block(self, corrupted):
        # 演示清单里 mid 块2 坏，left/leaf 以显式全零覆盖 -> 块2 解析为全零
        for lid in ("left", "leaf"):
            img = corrupted.resolve_image(lid)
            assert img[2 * BLOCK_SIZE:3 * BLOCK_SIZE] == bytes(BLOCK_SIZE)
        with pytest.raises(BadBlockError):
            corrupted.resolve_image("right")  # right 卡在未覆盖的坏块4

    def test_uncovered_inherited_bad_block_rejected_no_ancestor_fill(self, corrupted):
        # right 没有块4：mid 块4 坏，且 base 没有块4 -> 必须拒绝，不能输出零块短镜像
        with pytest.raises(BadBlockError) as ei:
            corrupted.resolve_image("right")
        assert ei.value.layer_id == "mid"
        assert ei.value.block_index == 4

    def test_covered_restore_point_downloads_full_size_image(self, corrupted):
        # left/leaf 覆盖了 mid 的坏块2、4 -> 完整 64 KiB 镜像可下载
        for lid in ["left", "leaf"]:
            img = corrupted.resolve_image(lid)
            assert len(img) == 64 * 1024
        leaf = corrupted.resolve_image("leaf")
        assert leaf[4 * BLOCK_SIZE:5 * BLOCK_SIZE] == bytes([0x1F]) * BLOCK_SIZE
        assert leaf[5 * BLOCK_SIZE:6 * BLOCK_SIZE] == bytes([0x1C]) * BLOCK_SIZE

    def test_bad_base64_and_short_block_both_detected(self, store):
        store.import_manifest(make_manifest(layers=[
            {"id": "r", "parent": None,
             "blocks": {"0": blk(1), "1": blk(2), "2": blk(3)}}]))
        _tamper(store, "r", 1, SHORT_B64)   # 短块
        _tamper(store, "r", 2, BAD_B64)     # 非法 Base64
        with pytest.raises(BadBlockError):
            store.resolve_image("r")
        by_id = {l["id"]: l for l in store.state()["layers"]}
        assert by_id["r"]["corrupt_blocks"] == [1, 2]

    def test_good_image_is_never_short(self, imported):
        # 回归：正常镜像长度严格等于盘大小
        assert len(imported.resolve_image("leaf")) == 64 * 1024


# --------------------------------------------------------------------- #
# 撤除：坏块继承前置检查；拒绝时树与层文件原样不动
# --------------------------------------------------------------------- #
class TestRemoveCorruption:
    def test_remove_refused_when_child_would_inherit_bad_block(self, corrupted):
        before_files = _layer_files(corrupted)
        before = {l["id"]: dict(l) for l in corrupted.state()["layers"]}
        with pytest.raises(BadBlockError) as ei:
            corrupted.remove("mid")  # right 会继承 mid 的坏块4
        assert ei.value.layer_id == "mid"
        assert ei.value.block_index == 4
        # 快照树原样
        after = {l["id"]: l for l in corrupted.state()["layers"]}
        assert set(after) == set(before)
        for lid in before:
            assert after[lid]["parent"] == before[lid]["parent"]
        # 层文件一个都没动（无新增/删除、无 journal 残留）
        assert _layer_files(corrupted) == before_files
        assert files_on_disk(corrupted) == manifest_files(corrupted)
        assert not (corrupted.root / "journal.json").exists()

    def test_remove_refused_when_direct_child_itself_corrupt(self, store):
        store.import_manifest(make_manifest())
        _tamper(store, "left", 0, SHORT_B64)  # 直接子层 left 自身含坏块
        with pytest.raises(BadBlockError) as ei:
            store.remove("mid")
        assert ei.value.layer_id == "left"
        assert files_on_disk(store) == manifest_files(store)

    def test_remove_allowed_when_all_children_cover_bad_blocks(self, store):
        # mid 块1 坏；left/right 都自有块1（right 用显式全零覆盖）-> 可撤除
        store.import_manifest(make_manifest(layers=[
            {"id": "base", "parent": None, "blocks": {"0": blk(1)}},
            {"id": "mid", "parent": "base", "blocks": {"1": blk(2)}},
            {"id": "left", "parent": "mid", "blocks": {"1": blk(0x1A)}},
            {"id": "right", "parent": "mid", "blocks": {"1": blk(0)}},
        ]))
        _tamper(store, "mid", 1, SHORT_B64)
        before_left = store.resolve_image("left")
        before_right = store.resolve_image("right")
        state = store.remove("mid")
        by_id = {l["id"]: l for l in state["layers"]}
        assert by_id["left"]["parent"] == "base"
        assert by_id["right"]["parent"] == "base"
        # 保留点镜像逐字节一致
        assert store.resolve_image("left") == before_left
        assert store.resolve_image("right") == before_right
        # 合并出的新子层不含坏块
        for cid in ("left", "right"):
            f = store._manifest["layers"][cid]["file"]
            blocks = json.loads((store.layers_dir / f).read_text())["blocks"]
            for value in blocks.values():
                assert len(base64.b64decode(value)) == BLOCK_SIZE

    def test_remove_unaffected_layer_with_corruption_elsewhere(self, corrupted):
        # 撤除 left：left 自身完好，leaf 合并其好块；其他分支的坏块不参与
        before_leaf = corrupted.resolve_image("leaf")
        before_files = set(files_on_disk(corrupted))
        state = corrupted.remove("left")
        by_id = {l["id"]: l for l in state["layers"]}
        assert "left" not in by_id
        assert by_id["leaf"]["parent"] == "mid"
        assert corrupted.resolve_image("leaf") == before_leaf  # 逐字节一致
        # 含坏块的 mid/base 文件未被改动（文件名仍在清单中）
        assert corrupted._manifest["layers"]["mid"]["file"] in files_on_disk(corrupted)
        assert len(files_on_disk(corrupted)) == len(before_files) - 1

    def test_remove_corrupt_target_keeps_tree_across_reboot(self, corrupted, tmp_path):
        with pytest.raises(BadBlockError):
            corrupted.remove("mid")
        rebooted = Store(tmp_path)
        assert {l["id"] for l in rebooted.state()["layers"]} == {
            "base", "mid", "left", "leaf", "right"}
        # 重启后坏块仍被识别，拒绝依旧生效
        with pytest.raises(BadBlockError):
            rebooted.remove("mid")

    def test_badblock_is_store_error(self, corrupted):
        # 保证 API 层按 400 处理而非 500
        with pytest.raises(StoreError):
            corrupted.resolve_image("right")
