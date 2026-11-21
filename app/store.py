"""快照链存储引擎。

磁盘布局（DATA_DIR）：
    manifest.json    当前清单（原子替换更新），记录 disk_size 与每层 {parent, file, blocks}
    journal.json     撤除操作的两阶段日志，仅在操作进行中存在
    layers/*.layer   每层一个文件，JSON 存稀疏块 {"blocks": {"块索引": "base64(4KiB)"}}

撤除（remove）顺序 —— 提交点之前旧链始终完整可用：
    1. 为每个直接子层写出合并后的新层文件（uuid 命名，tmp+rename+fsync）
    2. 写 journal.json（记录旧/新 epoch、新文件、待删旧文件、完整新清单）
    3. 原子替换 manifest.json                      —— 提交点
    4. 删除旧层文件与 journal（收尾，失败可留给启动恢复清理）

启动恢复（保证重启后只呈现一套完整可恢复的链）：
    journal 有效且 manifest 已是新 epoch -> 前滚：补删旧文件
    journal 有效且 manifest 仍是旧 epoch -> 回滚：删除新文件，旧链原样保留
    journal 缺失或损坏                   -> 切换未开始，清理孤儿/临时文件即可
"""

from __future__ import annotations

import base64
import binascii
import json
import os
import re
import threading
import uuid
from pathlib import Path

BLOCK_SIZE = 4096                      # 4 KiB 盘块
MAX_DISK_SIZE = 16 * 1024 * 1024       # 单盘至多 16 MiB

MANIFEST_NAME = "manifest.json"
JOURNAL_NAME = "journal.json"
LAYERS_DIR = "layers"

_LAYER_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")


class StoreError(Exception):
    """可预期的用户侧错误（校验失败、状态冲突），对应 HTTP 400。"""


class NotFoundError(StoreError):
    """引用了不存在的快照，对应 HTTP 404。"""


class CorruptStoreError(Exception):
    """存储内部不一致，需要人工介入，对应 HTTP 500。"""


class BadBlockError(StoreError):
    """层文件中某个盘块损坏（Base64 非法或解码后不是 4 KiB）。

    携带来源快照 id 与块号，便于维护页定位；下载/撤除遇到坏块时按 400 拒绝，
    绝不用祖先块或零块补齐。
    """

    def __init__(self, layer_id: str | None, block_index: int,
                 filename: str, reason: str):
        self.layer_id = layer_id
        self.block_index = block_index
        self.filename = filename
        self.reason = reason
        source = f"来源快照 {layer_id}" if layer_id else f"层文件 {filename}"
        super().__init__(
            f"{source} 的块 {block_index} 损坏（{reason}），操作已拒绝；"
            "不会用祖先块或零块补齐，请人工修复该层文件"
        )


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _atomic_write(path: Path, data: bytes) -> None:
    """写临时文件 -> fsync -> rename -> fsync 目录。失败时目标文件保持原样。"""
    tmp = path.with_name(f"{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
    _fsync_dir(path.parent)


class Store:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.layers_dir = self.root / LAYERS_DIR
        self.layers_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.recovery_events: list[str] = []
        # 层文件坏块/内容缓存：文件名 -> (mtime_ns, size, 坏块索引列表, 好块字典)
        # 带 mtime/大小签名：层文件在进程存活期间被外部篡改也能被重新识别
        self._block_cache: dict[str, tuple[int, int, list[int], dict[int, bytes]]] = {}
        self._recover()
        self._manifest = self._load_manifest()

    # ------------------------------------------------------------------ #
    # 启动恢复
    # ------------------------------------------------------------------ #
    def _recover(self) -> None:
        journal_path = self.root / JOURNAL_NAME
        journal = None
        if journal_path.exists():
            try:
                journal = json.loads(journal_path.read_text("utf-8"))
                if not isinstance(journal, dict) or journal.get("op") != "remove":
                    raise ValueError("journal 结构不完整")
            except Exception:
                # 日志写在清单切换之前；日志损坏说明崩溃发生在切换开始之前，
                # 旧清单必然完好，按“无操作在途”处理并清掉残迹。
                journal = None
                journal_path.unlink(missing_ok=True)
                self.recovery_events.append(
                    "发现损坏的 journal（崩溃发生在切换前），已按无在途操作清理"
                )

        if journal is not None:
            manifest = self._load_manifest()
            epoch = manifest.get("epoch") if manifest else None
            if epoch == journal.get("new_epoch"):
                # 清单已切换但收尾未完成 -> 前滚
                for name in journal.get("obsolete_files", []):
                    self._delete_layer_file(name)
                journal_path.unlink(missing_ok=True)
                self.recovery_events.append(
                    f"检测到已切换未收尾的撤除（目标 {journal.get('target')}），"
                    "已前滚并清理旧层文件"
                )
            elif epoch == journal.get("old_epoch"):
                # 清单尚未切换 -> 回滚，旧链原样保留
                for name in journal.get("new_files", []):
                    self._delete_layer_file(name)
                journal_path.unlink(missing_ok=True)
                self.recovery_events.append(
                    f"检测到未完成的撤除（目标 {journal.get('target')}），"
                    "已回滚，保留旧链"
                )
            else:
                raise CorruptStoreError(
                    "journal 与 manifest 的 epoch 均不匹配，存储状态需要人工检查"
                )

        # 清理根目录残留临时文件与 layers/ 下未被引用的文件（含导入中断的残骸）
        for tmp in self.root.glob("*.tmp-*"):
            tmp.unlink(missing_ok=True)
        self._gc_orphans()

    def _gc_orphans(self) -> None:
        referenced = set()
        manifest = self._load_manifest()
        if manifest:
            referenced = {info["file"] for info in manifest["layers"].values()}
        removed = 0
        for f in self.layers_dir.iterdir():
            if f.is_file() and f.name not in referenced:
                f.unlink(missing_ok=True)
                removed += 1
        if removed:
            self.recovery_events.append(f"清理了 {removed} 个未被引用的层文件")

    def _load_manifest(self) -> dict | None:
        path = self.root / MANIFEST_NAME
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text("utf-8"))
        except Exception as exc:
            raise CorruptStoreError(f"manifest.json 损坏：{exc}") from exc
        if not isinstance(data, dict) or "layers" not in data or "epoch" not in data:
            raise CorruptStoreError("manifest.json 结构不完整")
        return data

    # ------------------------------------------------------------------ #
    # 层文件读写
    # ------------------------------------------------------------------ #
    def _write_layer_file(self, blocks: dict[int, bytes]) -> str:
        """把稀疏块写成一个新层文件（uuid 命名，原子落盘），返回文件名。"""
        name = f"{uuid.uuid4().hex}.layer"
        payload = json.dumps(
            {"blocks": {str(i): base64.b64encode(b).decode("ascii")
                        for i, b in sorted(blocks.items())}}
        ).encode("utf-8")
        _atomic_write(self.layers_dir / name, payload)
        # 写入的内容由本进程生成、必然合法，直接建立缓存避免重新读盘解码
        stat = (self.layers_dir / name).stat()
        self._block_cache[name] = (stat.st_mtime_ns, stat.st_size, [], dict(blocks))
        return name

    def _invalidate_cache(self) -> None:
        """manifest 切换（导入/撤除提交）后，旧文件缓存全部作废。"""
        self._block_cache.clear()

    @staticmethod
    def _decode_block(raw_value, layer_id, idx: int, filename: str) -> bytes:
        """严格解码一个盘块：Base64 必须合法且解码后正好 4 KiB，否则判坏块。"""
        if not isinstance(raw_value, str):
            raise BadBlockError(layer_id, idx, filename, "数据不是 Base64 字符串")
        try:
            data = base64.b64decode(raw_value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise BadBlockError(layer_id, idx, filename, f"Base64 无法解码：{exc}") from exc
        if len(data) != BLOCK_SIZE:
            raise BadBlockError(
                layer_id, idx, filename,
                f"解码后为 {len(data)} 字节，必须为 {BLOCK_SIZE} 字节（4 KiB）",
            )
        return data

    def _scan_layer_file(self, name: str,
                         layer_id: str | None = None) -> tuple[list[int], dict[int, bytes]]:
        """读一个层文件并逐块严格校验，带缓存。

        返回 (坏块索引列表, 好块字典)：坏块不进好块字典，也不抛异常，
        供 state 展示与撤除前置检查使用；层文件整体不可读时抛 CorruptStoreError。
        """
        path = self.layers_dir / name
        try:
            sig = path.stat()
            signature = (sig.st_mtime_ns, sig.st_size)
        except FileNotFoundError as exc:
            raise CorruptStoreError(f"层文件丢失：{name}") from exc
        cached = self._block_cache.get(name)
        if cached is not None and (cached[0], cached[1]) == signature:
            return cached[2], cached[3]
        try:
            raw = json.loads(path.read_text("utf-8"))
        except FileNotFoundError as exc:
            raise CorruptStoreError(f"层文件丢失：{name}") from exc
        except Exception as exc:
            raise CorruptStoreError(f"层文件损坏：{name}: {exc}") from exc
        if not isinstance(raw, dict) or not isinstance(raw.get("blocks"), dict):
            raise CorruptStoreError(f"层文件结构不完整：{name}")
        good: dict[int, bytes] = {}
        bad: list[int] = []
        for key, value in raw["blocks"].items():
            try:
                idx = int(key)
            except (TypeError, ValueError):
                # 非数字索引无法对应块号，属于文件级损坏
                raise CorruptStoreError(f"层文件 {name} 含非法块索引：{key!r}")
            try:
                good[idx] = self._decode_block(value, layer_id, idx, name)
            except BadBlockError:
                bad.append(idx)
        bad.sort()
        self._block_cache[name] = (signature[0], signature[1], bad, good)
        return bad, good

    def _read_layer_file(self, name: str, layer_id: str | None = None) -> dict[int, bytes]:
        """严格读取层文件：存在坏块即抛 BadBlockError，绝不静默产出短块。"""
        bad, good = self._scan_layer_file(name, layer_id)
        if bad:
            raise BadBlockError(layer_id, bad[0], name,
                                "Base64 非法或解码后不是 4 KiB")
        return dict(good)

    def _delete_layer_file(self, name: str) -> None:
        try:
            (self.layers_dir / name).unlink(missing_ok=True)
        except OSError:
            pass  # 留待下次启动时的孤儿清理
        self._block_cache.pop(name, None)

    # ------------------------------------------------------------------ #
    # 导入
    # ------------------------------------------------------------------ #
    def inject_corruption_for_demo(self, plan: dict[str, dict[int, str]],
                                   payloads: dict[str, str]) -> None:
        """【仅用于维护页演示】在合法导入后篡改卷上的层文件，制造静置损坏。

        plan: {层 id: {块号: 损坏类型}}；payloads: {损坏类型: 坏块字符串}。
        直接重写该层文件（演示场景可接受就地改写），缓存经 mtime/大小签名自动失效。
        """
        with self._lock:
            manifest = self._require_manifest()
            for lid, block_plan in plan.items():
                if lid not in manifest["layers"]:
                    raise NotFoundError(f"快照 {lid} 不存在")
                name = manifest["layers"][lid]["file"]
                path = self.layers_dir / name
                raw = json.loads(path.read_text("utf-8"))
                for idx_str, kind in block_plan.items():
                    raw["blocks"][str(idx_str)] = payloads[kind]
                _atomic_write(path, json.dumps(raw).encode("utf-8"))
                self._block_cache.pop(name, None)

    def import_manifest(self, payload: dict) -> dict:
        """校验并整体替换当前存储。新清单原子切换，切换前旧链不受影响。"""
        disk_size, layers = self._validate_import(payload)
        with self._lock:
            files: dict[str, str] = {}
            try:
                for lid, layer in layers.items():
                    files[lid] = self._write_layer_file(layer["blocks"])
            except OSError as exc:
                for name in files.values():
                    self._delete_layer_file(name)
                raise StoreError(f"写入层文件失败，旧数据未受影响：{exc}") from exc

            new_manifest = {
                "epoch": uuid.uuid4().hex,
                "disk_size": disk_size,
                "layers": {
                    lid: {
                        "parent": layers[lid]["parent"],
                        "file": files[lid],
                        "blocks": len(layers[lid]["blocks"]),
                    }
                    for lid in layers
                },
            }
            try:
                _atomic_write(
                    self.root / MANIFEST_NAME,
                    json.dumps(new_manifest, ensure_ascii=False, indent=1).encode("utf-8"),
                )
            except OSError as exc:
                for name in files.values():
                    self._delete_layer_file(name)
                raise StoreError(f"切换清单失败，旧数据未受影响：{exc}") from exc
            old = self._manifest
            self._manifest = new_manifest
            if old:
                for info in old["layers"].values():
                    self._delete_layer_file(info["file"])
            state = self.state()
            state["message"] = f"导入成功：{len(layers)} 层，盘大小 {disk_size} 字节"
            return state

    @staticmethod
    def _validate_import(payload) -> tuple[int, dict]:
        if not isinstance(payload, dict):
            raise StoreError("清单必须是 JSON 对象")
        disk_size = payload.get("disk_size")
        if not isinstance(disk_size, int) or isinstance(disk_size, bool):
            raise StoreError("disk_size 必须是整数（字节）")
        if disk_size <= 0 or disk_size > MAX_DISK_SIZE:
            raise StoreError(f"disk_size 必须在 1..{MAX_DISK_SIZE}（16 MiB）字节之间")
        if disk_size % BLOCK_SIZE != 0:
            raise StoreError(f"disk_size 必须是 {BLOCK_SIZE} 的整数倍")
        max_blocks = disk_size // BLOCK_SIZE

        raw_layers = payload.get("layers")
        if not isinstance(raw_layers, list) or not raw_layers:
            raise StoreError("layers 必须是非空数组")

        layers: dict[str, dict] = {}
        for item in raw_layers:
            if not isinstance(item, dict):
                raise StoreError("layers 数组元素必须是对象")
            lid = item.get("id")
            if not isinstance(lid, str) or not _LAYER_ID_RE.fullmatch(lid):
                raise StoreError(f"非法层 id：{lid!r}（允许字母数字及 _.-，1-64 字符）")
            if lid in layers:
                raise StoreError(f"层 id 重复：{lid}")
            parent = item.get("parent")
            if parent is not None and not isinstance(parent, str):
                raise StoreError(f"层 {lid} 的 parent 必须是字符串或 null")
            raw_blocks = item.get("blocks")
            if not isinstance(raw_blocks, dict):
                raise StoreError(f"层 {lid} 的 blocks 必须是对象（索引 -> base64）")
            blocks: dict[int, bytes] = {}
            for key, value in raw_blocks.items():
                if not isinstance(key, str) or not key.isdecimal() or str(int(key)) != key:
                    raise StoreError(f"层 {lid} 的块索引非法：{key!r}")
                idx = int(key)
                if idx >= max_blocks:
                    raise StoreError(
                        f"层 {lid} 的块索引 {idx} 超出范围（盘共 {max_blocks} 块）"
                    )
                if not isinstance(value, str):
                    raise StoreError(f"层 {lid} 块 {idx} 的数据必须是 base64 字符串")
                try:
                    data = base64.b64decode(value, validate=True)
                except (binascii.Error, ValueError) as exc:
                    raise StoreError(f"层 {lid} 块 {idx} 的 base64 无法解码") from exc
                if len(data) != BLOCK_SIZE:
                    raise StoreError(
                        f"层 {lid} 块 {idx} 解码后为 {len(data)} 字节，"
                        f"必须为 {BLOCK_SIZE} 字节"
                    )
                blocks[idx] = data
            layers[lid] = {"parent": parent, "blocks": blocks}

        for lid, layer in layers.items():
            parent = layer["parent"]
            if parent is not None and parent not in layers:
                raise StoreError(f"层 {lid} 的父层 {parent} 不存在")
        for lid in layers:  # 环检测
            seen = {lid}
            cur = lid
            while layers[cur]["parent"] is not None:
                cur = layers[cur]["parent"]
                if cur in seen:
                    raise StoreError(f"检测到父子关系循环（经过层 {cur}）")
                seen.add(cur)
        return disk_size, layers

    # ------------------------------------------------------------------ #
    # 撤除（删除中间快照，只重写直接子层）
    # ------------------------------------------------------------------ #
    def remove(self, layer_id: str) -> dict:
        with self._lock:
            plan = self._prepare_remove(layer_id)
            try:
                self._commit_remove(plan)
            except OSError as exc:
                self._rollback_remove(plan)
                raise StoreError(f"切换清单失败，已回滚，旧链仍可用：{exc}") from exc
            self._finalize_remove(plan)
            state = self.state()
            state["message"] = (
                f"已撤除 {layer_id}，重写直接子层：{', '.join(plan['children'])}"
            )
            return state

    def _check_remove_corruption(self, layers: dict, layer_id: str,
                                 info: dict, children: list[str]
                                 ) -> tuple[dict[int, bytes], dict[str, dict[int, bytes]]]:
        """撤除前的坏块检查（只读，不写任何文件）。

        拒绝规则：
          1. 任一直接子层自身含坏块 -> 拒绝（无法保证合并后镜像逐字节一致）；
          2. 被撤除层含坏块，且任一直接子层在该块号上没有自有块
             （显式全零块也算自有、算覆盖）-> 该子层将被迫继承坏块，整次拒绝。
        通过时返回（被撤除层好块, 各直接子层好块）供合并使用。
        """
        target_bad, target_good = self._scan_layer_file(info["file"], layer_id)
        child_blocks: dict[str, dict[int, bytes]] = {}
        for cid in children:
            bad, good = self._scan_layer_file(layers[cid]["file"], cid)
            if bad:
                raise BadBlockError(
                    cid, bad[0], layers[cid]["file"],
                    "直接子层自身含坏块，撤除无法保证保留点镜像逐字节一致",
                )
            child_blocks[cid] = good
        for idx in target_bad:
            for cid in children:
                if idx not in child_blocks[cid]:
                    raise BadBlockError(
                        layer_id, idx, info["file"],
                        f"撤除该快照会要求直接子层 {cid} 继承此坏块",
                    )
        return target_good, child_blocks

    def _prepare_remove(self, layer_id: str) -> dict:
        """第 1-2 步：坏块前置检查 -> 合并写出新子层文件 + 写 journal。旧链完全不动。"""
        manifest = self._require_manifest()
        layers = manifest["layers"]
        if layer_id not in layers:
            raise NotFoundError(f"快照 {layer_id} 不存在")
        info = layers[layer_id]
        if info["parent"] is None:
            raise StoreError("基盘（根层）不能撤除")
        children = sorted(
            lid for lid, x in layers.items() if x["parent"] == layer_id
        )
        if not children:
            raise StoreError(f"快照 {layer_id} 没有子层，不属于可撤除对象")

        # 任何写盘动作之前先做坏块检查：拒绝时快照树与层文件保持原样
        target_good, child_blocks = self._check_remove_corruption(
            layers, layer_id, info, children
        )
        new_files: dict[str, str] = {}
        merged_counts: dict[str, int] = {}
        try:
            for cid in children:
                # 子层已有的块（含全零块）保持子层版本；缺块才从被撤除层的好块继承。
                # 能走到这里说明被撤除层的每个坏块都被子层自有块覆盖，坏块不会进入合并结果。
                merged = {**target_good, **child_blocks[cid]}
                new_files[cid] = self._write_layer_file(merged)
                merged_counts[cid] = len(merged)
        except OSError as exc:
            for name in new_files.values():
                self._delete_layer_file(name)
            raise StoreError(f"写入新子层文件失败，旧链未受影响：{exc}") from exc

        new_layers = {
            lid: dict(meta) for lid, meta in layers.items() if lid != layer_id
        }
        for cid in children:
            new_layers[cid]["parent"] = info["parent"]
            new_layers[cid]["file"] = new_files[cid]
            new_layers[cid]["blocks"] = merged_counts[cid]
        new_manifest = {
            "epoch": uuid.uuid4().hex,
            "disk_size": manifest["disk_size"],
            "layers": new_layers,
        }
        plan = {
            "op": "remove",
            "target": layer_id,
            "children": children,
            "old_epoch": manifest["epoch"],
            "new_epoch": new_manifest["epoch"],
            "new_files": list(new_files.values()),
            "obsolete_files": [info["file"]] + [layers[c]["file"] for c in children],
            "new_manifest": new_manifest,
        }
        try:
            _atomic_write(
                self.root / JOURNAL_NAME,
                json.dumps(plan, ensure_ascii=False).encode("utf-8"),
            )
        except OSError as exc:
            for name in new_files.values():
                self._delete_layer_file(name)
            raise StoreError(f"写入 journal 失败，旧链未受影响：{exc}") from exc
        return plan

    def _commit_remove(self, plan: dict) -> None:
        """第 3 步：原子替换 manifest —— 唯一的提交点。"""
        _atomic_write(
            self.root / MANIFEST_NAME,
            json.dumps(plan["new_manifest"], ensure_ascii=False, indent=1).encode("utf-8"),
        )
        self._manifest = plan["new_manifest"]

    def _finalize_remove(self, plan: dict) -> None:
        """第 4 步：收尾清理。失败不影响一致性，留给启动恢复。"""
        for name in plan["obsolete_files"]:
            self._delete_layer_file(name)
        try:
            (self.root / JOURNAL_NAME).unlink(missing_ok=True)
        except OSError:
            pass

    def _rollback_remove(self, plan: dict) -> None:
        for name in plan["new_files"]:
            self._delete_layer_file(name)
        try:
            (self.root / JOURNAL_NAME).unlink(missing_ok=True)
        except OSError:
            pass

    # ------------------------------------------------------------------ #
    # 查询 / 下载
    # ------------------------------------------------------------------ #
    def _require_manifest(self) -> dict:
        if self._manifest is None:
            raise StoreError("尚未导入备份清单")
        return self._manifest

    def resolve_image(self, layer_id: str) -> bytes:
        """沿父链按稀疏继承还原整盘原始字节（缺块为全零）。

        解析规则（自目标层向根层）：
          - 块号上离目标最近的自有块生效（显式全零块也算自有、算覆盖）；
          - 只有恢复点链上确实需要某个坏块、且没有任何下层用自有块覆盖它时，
            才抛 BadBlockError 拒绝下载；绝不用更早祖先的好块或零块补齐；
          - 与恢复点无关的坏块（已被下层覆盖，或在其他分支上）不影响下载。
        """
        with self._lock:
            manifest = self._require_manifest()
            layers = manifest["layers"]
            if layer_id not in layers:
                raise NotFoundError(f"快照 {layer_id} 不存在")
            chain: list[str] = []
            cur = layer_id
            while cur is not None:
                chain.append(cur)
                cur = layers[cur]["parent"]

            resolved: dict[int, bytes] = {}
            bad_pending: dict[int, tuple[str, str]] = {}
            # 目标层 -> 根：先出现（离目标近）的层对每个块号拥有决定权
            for lid in chain:
                bad, good = self._scan_layer_file(layers[lid]["file"], lid)
                for idx, data in good.items():
                    if idx not in resolved and idx not in bad_pending:
                        resolved[idx] = data
                for idx in bad:
                    if idx not in resolved and idx not in bad_pending:
                        bad_pending[idx] = (lid, layers[lid]["file"])
            if bad_pending:
                idx = sorted(bad_pending)[0]
                lid, fname = bad_pending[idx]
                raise BadBlockError(
                    lid, idx, fname,
                    f"恢复点 {layer_id} 需要读取该坏块且无下层自有块覆盖",
                )
            buf = bytearray(manifest["disk_size"])
            for idx, data in resolved.items():
                buf[idx * BLOCK_SIZE:(idx + 1) * BLOCK_SIZE] = data
            return bytes(buf)

    def state(self) -> dict:
        with self._lock:
            base = {
                "block_size": BLOCK_SIZE,
                "max_disk_size": MAX_DISK_SIZE,
                "recovery_events": list(self.recovery_events),
            }
            if self._manifest is None:
                return {**base, "initialized": False, "disk_size": None,
                        "epoch": None, "layers": []}
            layers = self._manifest["layers"]
            children = {lid: [] for lid in layers}
            for lid, meta in layers.items():
                if meta["parent"] is not None:
                    children[meta["parent"]].append(lid)
            # 逐文件扫描坏块（容错：单个层文件整体不可读不应让维护页打不开）
            corrupt_map: dict[str, list[int]] = {}
            unreadable: dict[str, str] = {}
            for lid, meta in layers.items():
                try:
                    bad, _ = self._scan_layer_file(meta["file"], lid)
                except CorruptStoreError as exc:
                    bad = []
                    unreadable[lid] = str(exc)
                corrupt_map[lid] = bad
            listing = []
            for lid, meta in sorted(layers.items()):
                entry = {
                    "id": lid,
                    "parent": meta["parent"],
                    "children": sorted(children[lid]),
                    "block_count": meta["blocks"],
                    "corrupt_blocks": corrupt_map[lid],
                    "unreadable": unreadable.get(lid),
                    "can_remove": meta["parent"] is not None and bool(children[lid]),
                    "remove_blocked_reason": None,
                }
                if entry["can_remove"]:
                    reason = self._remove_block_reason(lid, meta, children[lid],
                                                        layers, corrupt_map, unreadable)
                    entry["remove_blocked_reason"] = reason
                    if reason is not None:
                        entry["can_remove"] = False
                listing.append(entry)
            return {
                **base,
                "initialized": True,
                "disk_size": self._manifest["disk_size"],
                "epoch": self._manifest["epoch"],
                "layers": listing,
            }

    def _remove_block_reason(self, layer_id: str, info: dict, child_ids: list[str],
                             layers: dict, corrupt_map: dict[str, list[int]],
                             unreadable: dict[str, str]) -> str | None:
        """计算坏块导致的撤除阻断原因（供维护页置灰按钮与展示来源/块号）。"""
        if layer_id in unreadable:
            return unreadable[layer_id]
        target_bad = corrupt_map[layer_id]
        child_good: dict[str, set[int]] = {}
        for cid in child_ids:
            if cid in unreadable:
                return unreadable[cid]
            if corrupt_map[cid]:
                idx = corrupt_map[cid][0]
                return (f"直接子层 {cid} 的块 {idx} 损坏，撤除无法保证保留点镜像一致")
            child_good[cid] = set(self._scan_layer_file(layers[cid]["file"], cid)[1])
        for idx in target_bad:
            for cid in child_ids:
                if idx not in child_good[cid]:
                    return (f"来源快照 {layer_id} 的块 {idx} 损坏，"
                            f"直接子层 {cid} 未覆盖该块，撤除会令其继承坏块")
        return None
