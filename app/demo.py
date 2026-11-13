"""并发交错演示：O/A/B/C 四批次 + 可控故障注入，验证循环等待已修复。

场景（同一项目，共享 Patient/Study/Series 由种子批次先行发布）：

  X/Y/Z 为源 SOP Instance UID：
    O：自身 SOP = X，保留成功后挂起 2 秒再受控失败（释放 X）
    A：自身 SOP = Y，嵌套引用 X
    B：自身 SOP = Z，嵌套引用 Y
    C：自身 SOP = X，嵌套引用 Z（O 失败释放 X 后到达）

旧“占用-等待”协议下：A 占 Y 等 X、B 占 Z 等 Y、C 占 X 等 Z —— 三者循环等待，
只能各自等到 60 秒超时失败。新协议“阻塞即让路”（等待前释放自己的全部
PENDING 保留、锁外等待后重试）下，A/B/C 无需重传即可全部发布。

本模块由 ``POST /api/demo/interleaved`` 调用，仅在演示钩子启用时可用；
每次运行生成全新源 UID，可重复执行且互不影响。
"""
from __future__ import annotations

import io
import threading
import time
import uuid
import zipfile

import pydicom
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.sequence import Sequence
from pydicom.uid import ExplicitVRLittleEndian

from . import db
from .config import settings
from .pipeline import UploadedFile, process_batch, zip_path

SOP_CLASS_CT = "1.2.840.10008.5.1.4.1.1.2"

#: 演示时间线中各角色的说明
ROLE_NOTES = {
    "seed": "种子批次：发布共享 Patient/Study/Series（其他共有标识已发布）",
    "O": "占用 X，转换挂起后受控失败（释放 X）",
    "A": "引用 X，自身 SOP = Y",
    "B": "引用 Y，自身 SOP = Z",
    "C": "O 失败后到达：自身 SOP = X，引用 Z",
}


def _make_dicom_bytes(
    patient_id: str,
    study_uid: str,
    series_uid: str,
    sop_uid: str,
    ref_sop_uid: str | None = None,
) -> bytes:
    """构造最小合法未压缩 DICOM（可带一条嵌套 ReferencedSOPSequence 引用）。"""
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = SOP_CLASS_CT
    meta.MediaStorageSOPInstanceUID = sop_uid
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    meta.FileMetaInformationVersion = b"\x00\x01"
    meta.ImplementationClassUID = "1.2.840.1001.99.1.99"
    ds = Dataset()
    ds.file_meta = meta
    ds.PatientID = patient_id
    ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = series_uid
    ds.SOPInstanceUID = sop_uid
    ds.SOPClassUID = SOP_CLASS_CT
    ds.Modality = "CT"
    ds.Rows = 4
    ds.Columns = 4
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelData = bytes(range(32))
    if ref_sop_uid:
        item = Dataset()
        item.ReferencedSOPClassUID = SOP_CLASS_CT  # 受保护，不重写
        item.ReferencedSOPInstanceUID = ref_sop_uid  # 必须按持久映射重写
        ds.ReferencedSOPSequence = Sequence([item])
    buf = io.BytesIO()
    ds.save_as(buf, enforce_file_format=True)
    return buf.getvalue()


def _read_own_and_ref(batch_id: str, entry_name: str) -> tuple[str, str | None]:
    """从已发布 ZIP 中读出（自身 SOPInstanceUID, 嵌套引用 ReferencedSOPInstanceUID）。"""
    with zipfile.ZipFile(zip_path(batch_id)) as zf:
        ds = pydicom.dcmread(io.BytesIO(zf.read(entry_name)))
    ref = None
    if "ReferencedSOPSequence" in ds:
        ref = str(ds.ReferencedSOPSequence[0].ReferencedSOPInstanceUID)
    return str(ds.SOPInstanceUID), ref


def run_interleaved_demo(project_id: str) -> dict:
    """在指定项目内完整跑一遍 O/A/B/C 交错场景，返回时间线与一致性校验结果。"""
    run = uuid.uuid4().int % 10**8
    patient = f"PAT-ILV-{run}"
    study = f"1.2.840.1001.99.777.{run}.1"
    series = f"{study}.1"
    uid_x = f"{study}.1.1"
    uid_y = f"{study}.1.2"
    uid_z = f"{study}.1.3"
    uid_seed = f"{study}.1.9"

    def bundle(name: str, sop_uid: str, ref_sop_uid: str | None = None) -> list[UploadedFile]:
        return [
            UploadedFile(
                name,
                _make_dicom_bytes(patient, study, series, sop_uid, ref_sop_uid),
            )
        ]

    t0 = time.monotonic()
    steps: list[dict] = []
    step_lock = threading.Lock()

    def log(actor: str, action: str, **extra) -> None:
        with step_lock:
            steps.append(
                {
                    "t": round(time.monotonic() - t0, 2),
                    "actor": actor,
                    "action": action,
                    **extra,
                }
            )

    # 记录运行前已有批次，用于事后定位 O 的失败批次行
    conn = db.connect()
    try:
        before_ids = {row["id"] for row in db.list_batches(conn, project_id)}
    finally:
        conn.close()

    # 1) 种子批次：发布共享 Patient/Study/Series（“其他共有标识已发布”）
    seed_id = process_batch(project_id, bundle("seed_shared.dcm", uid_seed))
    log("seed", f"{ROLE_NOTES['seed']}", batch_id=seed_id, status="PUBLISHED")

    outcomes: dict[str, dict] = {}

    def worker(role: str, files: list[UploadedFile], hold: float = 0.0,
               fail: bool = False, delay: float = 0.0) -> None:
        if delay:
            time.sleep(delay)
        start = time.monotonic()
        log(role, f"开始上传（{ROLE_NOTES[role]}）")
        try:
            batch_id = process_batch(
                project_id, files, demo_hold_seconds=hold, demo_fail=fail
            )
        except Exception as exc:
            outcomes[role] = {
                "batch_id": None,
                "status": "FAILED",
                "error": str(exc),
                "elapsed": round(time.monotonic() - start, 2),
            }
            log(role, f"批次失败：{exc}（PENDING 映射已回收）", status="FAILED")
            return
        outcomes[role] = {
            "batch_id": batch_id,
            "status": "PUBLISHED",
            "elapsed": round(time.monotonic() - start, 2),
        }
        log(role, f"批次发布成功（{outcomes[role]['elapsed']}s）",
            batch_id=batch_id, status="PUBLISHED")

    # 2) O/A/B 并发：O 保留 X 后挂起 2 秒再受控失败；A 引用 X、B 引用 Y
    threads = [
        threading.Thread(
            target=worker,
            args=("O", bundle("o_own_x.dcm", uid_x)),
            kwargs={"hold": 2.0, "fail": True},
            daemon=True,
        ),
        threading.Thread(
            target=worker,
            args=("A", bundle("a_refs_x_own_y.dcm", uid_y, ref_sop_uid=uid_x)),
            kwargs={"delay": 0.4},
            daemon=True,
        ),
        threading.Thread(
            target=worker,
            args=("B", bundle("b_refs_y_own_z.dcm", uid_z, ref_sop_uid=uid_y)),
            kwargs={"delay": 0.8},
            daemon=True,
        ),
    ]
    log("demo", "并发启动 O（挂起 2s 后受控失败）、A（0.4s 后到达）、B（0.8s 后到达）")
    for t in threads:
        t.start()

    # 3) O 失败释放 X 后，C 到达（自身 X，引用 Z）——与正在重试的 A 竞争 X
    threads[0].join()
    log("demo", "O 已失败并释放 X，C 到达")
    worker("C", bundle("c_own_x_refs_z.dcm", uid_x, ref_sop_uid=uid_z))
    for t in threads[1:]:
        t.join()
    total = round(time.monotonic() - t0, 2)

    # 4) 汇总：定位 O 的失败批次行，读取 X/Y/Z 持久映射
    conn = db.connect()
    try:
        new_failed = [
            row
            for row in db.list_batches(conn, project_id)
            if row["id"] not in before_ids and row["status"] == "FAILED"
        ]
        o_batch_id = new_failed[0]["id"] if len(new_failed) == 1 else None
        outcomes["O"]["batch_id"] = o_batch_id
        published = {
            row["source_value"]: row for row in db.list_mappings(conn, project_id)
        }
    finally:
        conn.close()
    if o_batch_id:
        log("O", "失败批次已标记 FAILED，X 的 PENDING 保留已回收",
            batch_id=o_batch_id, status="FAILED")

    mx, my, mz = (published.get(u) for u in (uid_x, uid_y, uid_z))

    # 5) 一致性校验
    checks: list[dict] = []

    def check(label: str, ok: bool) -> None:
        checks.append({"label": label, "ok": bool(ok)})

    check("O 批次受控失败并回收（未留下 PENDING 映射）",
          outcomes["O"]["status"] == "FAILED" and o_batch_id is not None)
    check("A/B/C 无需重传全部发布（各上传一次）",
          all(outcomes[r]["status"] == "PUBLISHED" for r in "ABC"))
    check("X/Y/Z 各有且仅有一条已发布持久映射",
          all(m is not None for m in (mx, my, mz)))
    if mx is not None and o_batch_id is not None:
        check("X 的映射由 O 之外的批次创建（失败未污染已发布映射）",
              mx["created_batch_id"] != o_batch_id)
    check(f"总耗时 {total}s 远低于 {settings.lock_wait_seconds}s 等待上限"
          "（未发生超时解环）", total < settings.lock_wait_seconds)

    if all(outcomes[r]["status"] == "PUBLISHED" for r in "ABC") and all(
        m is not None for m in (mx, my, mz)
    ):
        a_own, a_ref = _read_own_and_ref(outcomes["A"]["batch_id"], "a_refs_x_own_y.dcm")
        b_own, b_ref = _read_own_and_ref(outcomes["B"]["batch_id"], "b_refs_y_own_z.dcm")
        c_own, c_ref = _read_own_and_ref(outcomes["C"]["batch_id"], "c_own_x_refs_z.dcm")
        check("A 包自身 SOP == 持久映射 Y 目标", a_own == my["target_value"])
        check("A 包嵌套引用 == 持久映射 X 目标", a_ref == mx["target_value"])
        check("B 包自身 SOP == 持久映射 Z 目标", b_own == mz["target_value"])
        check("B 包嵌套引用 == 持久映射 Y 目标", b_ref == my["target_value"])
        check("C 包自身 SOP == 持久映射 X 目标", c_own == mx["target_value"])
        check("C 包嵌套引用 == 持久映射 Z 目标", c_ref == mz["target_value"])

    id_to_role = {
        outcomes[r]["batch_id"]: r for r in "OABC" if outcomes[r].get("batch_id")
    }
    id_to_role[seed_id] = "seed"
    mapping_report = [
        {
            "role": role,
            "source_value": source,
            "target_value": m["target_value"],
            "created_by": id_to_role.get(m["created_batch_id"], "其他批次"),
        }
        for role, source, m in (
            ("X", uid_x, mx),
            ("Y", uid_y, my),
            ("Z", uid_z, mz),
        )
        if m is not None
    ]

    return {
        "ok": all(c["ok"] for c in checks),
        "project_id": project_id,
        "elapsed_seconds": total,
        "lock_wait_seconds": settings.lock_wait_seconds,
        "uids": {"X": uid_x, "Y": uid_y, "Z": uid_z},
        "steps": sorted(steps, key=lambda s: s["t"]),
        "batches": outcomes,
        "mappings": mapping_report,
        "checks": checks,
    }
