"""端到端演示/验收脚本：跨批次引用、同源并发、项目隔离、并发交错解环。

以隔离模式（python -I）运行，临时数据目录与临时端口启动完整 FastAPI 应用，
通过 HTTP 上传 scripts/generate_samples.py 生成的样例并断言：

  1. 跨批次：batch_b 引用 batch_a 的 Study/Series/SOP，映射与第一批完全一致；
  2. 前向引用：batch_a 中引用的“尚未入库” SOP 也已预分配映射；
  3. 同源并发：两个并发批次包含相同源标识，得到相同目标，且都发布成功；
  4. 项目隔离：另一项目的相同源标识得到不同目标；
  5. 压缩文件与坏文件整批拒绝，无部分批次、无映射污染；
  6. 下载的 ZIP 中 SOP Class UID 与像素字节不变，嵌套引用已替换；
  7. 并发交错 + 可控故障：O 占 X 后转换受控失败，A 引用 X/自身 Y、B 引用 Y/自身 Z、
     C（O 失败后到达）自身 X/引用 Z —— A/B/C 无需重传全部发布，无 60 秒超时解环。

用法：
  python -I scripts/demo.py [--samples data/samples] [--keep]
"""
from __future__ import annotations

import argparse
import io
import os
import sys
import tempfile
import threading
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# 隔离模式（-I）下把项目根加入 sys.path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 演示需要可控故障注入钩子，必须在导入 app 模块（读取配置）前设置
os.environ.setdefault("DICOM_REMAP_DEMO_HOOKS", "true")

import httpx  # noqa: E402
import pydicom  # noqa: E402
import uvicorn  # noqa: E402

from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402

SAMPLE_SOURCE_UIDS = {
    "patient": "PAT-DEMO-0001",
    "study": "1.2.840.1001.99.1.1",
    "series_a1": "1.2.840.1001.99.1.1.1",
    "sop_a1": "1.2.840.1001.99.1.1.1.1",
    "sop_a2": "1.2.840.1001.99.1.1.2.1",
    "sop_future": "1.2.840.1001.99.99.0.0.1",
}
SOP_CLASS_CT = "1.2.840.10008.5.1.4.1.1.2"
SOP_CLASS_SR = "1.2.840.10008.5.1.4.1.1.88.11"

# 并发交错演示样例的固定源 UID（与 scripts/generate_samples.py 一致）
ILV_UIDS = {
    "X": "1.2.840.1001.99.7.1.1.1",
    "Y": "1.2.840.1001.99.7.1.1.2",
    "Z": "1.2.840.1001.99.7.1.1.3",
}


class ServerThread(threading.Thread):
    def __init__(self, port: int):
        super().__init__(daemon=True)
        self.config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")

    def run(self):
        server = uvicorn.Server(self.config)
        server.run()


def _upload(client: httpx.Client, base: str, project_id: str, files: list[Path],
            demo_hold_seconds: float | None = None, demo_fail: bool = False):
    data = {"project_id": project_id}
    if demo_hold_seconds is not None:
        data["demo_hold_seconds"] = str(demo_hold_seconds)
    if demo_fail:
        data["demo_fail"] = "true"
    file_payload = [("files", (p.name, p.read_bytes(), "application/dicom")) for p in files]
    return client.post(f"{base}/api/batches", data=data, files=file_payload)


def _mapping_index(client: httpx.Client, base: str, project_id: str) -> dict[tuple[str, str], str]:
    rows = client.get(f"{base}/api/mappings", params={"project_id": project_id}).json()
    return {(r["id_kind"], r["source_value"]): r["target_value"] for r in rows}


def _download_zip(client: httpx.Client, base: str, batch_id: str) -> dict[str, pydicom.Dataset]:
    resp = client.get(f"{base}/api/batches/{batch_id}/download")
    resp.raise_for_status()
    out: dict[str, pydicom.Dataset] = {}
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for name in zf.namelist():
            out[name] = pydicom.dcmread(io.BytesIO(zf.read(name)))
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", default=str(ROOT / "data" / "samples"))
    parser.add_argument("--keep", action="store_true", help="保留临时数据目录（默认删除）")
    args = parser.parse_args()
    samples = Path(args.samples)
    if not samples.exists():
        print(f"样例目录不存在：{samples}，请先运行 python scripts/generate_samples.py {samples}")
        return 2

    tmp = Path(tempfile.mkdtemp(prefix="dicom-demo-"))
    os.environ["DICOM_REMAP_DATA"] = str(tmp)
    # 各模块持有的是同一个 settings 引用；frozen dataclass 用 object.__setattr__
    # 绕过冻结，db_path/staging_dir/published_dir 均为 property，会立即反映新目录。
    object.__setattr__(settings, "data_dir", tmp)

    port = 18080
    ServerThread(port).start()
    base = f"http://127.0.0.1:{port}"

    for _ in range(50):
        try:
            if httpx.get(f"{base}/api/health", timeout=1).status_code == 200:
                break
        except httpx.TransportError:
            time.sleep(0.1)

    failures: list[str] = []

    def check(cond: bool, label: str):
        print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
        if not cond:
            failures.append(label)

    with httpx.Client(timeout=30) as client:
        print("== 准备：项目 ==")
        projects = client.get(f"{base}/api/projects").json()
        proj_a, proj_b = projects[0]["id"], projects[1]["id"]
        print(f"  项目 A={projects[0]['name']}  项目 B={projects[1]['name']}")

        print("== 场景 1：第一批入库（含嵌套引用与前向引用） ==")
        r = _upload(client, base, proj_a, sorted((samples / "batch_a").glob("*.dcm")))
        check(r.status_code == 201, f"batch_a 上传成功 (HTTP {r.status_code}, {r.text if r.status_code != 201 else ''})")
        batch_a_id = r.json()["id"] if r.status_code == 201 else None

        mapping_a = _mapping_index(client, base, proj_a)
        for kind, key in (
            ("patient_id", SAMPLE_SOURCE_UIDS["patient"]),
            ("study_uid", SAMPLE_SOURCE_UIDS["study"]),
            ("series_uid", SAMPLE_SOURCE_UIDS["series_a1"]),
            ("sop_uid", SAMPLE_SOURCE_UIDS["sop_a1"]),
            ("sop_uid", SAMPLE_SOURCE_UIDS["sop_a2"]),
            ("sop_uid", SAMPLE_SOURCE_UIDS["sop_future"]),
        ):
            check((kind, key) in mapping_a, f"预分配映射存在: {kind} <- {key}")

        zipped = _download_zip(client, base, batch_a_id)
        ds_a2 = zipped["a2_referencing.dcm"]
        target_a1 = mapping_a[("sop_uid", SAMPLE_SOURCE_UIDS["sop_a1"])]
        target_study = mapping_a[("study_uid", SAMPLE_SOURCE_UIDS["study"])]
        target_patient = mapping_a[("patient_id", SAMPLE_SOURCE_UIDS["patient"])]
        target_future = mapping_a[("sop_uid", SAMPLE_SOURCE_UIDS["sop_future"])]

        check(str(ds_a2.ReferencedStudySequence[0].StudyInstanceUID) == target_study,
              "嵌套 ReferencedStudySequence 的 Study UID 已替换")
        check(str(ds_a2.ReferencedStudySequence[0].ReferencedSOPSequence[0].ReferencedSOPInstanceUID)
              == target_future, "深层嵌套的前向 SOP 引用已替换为预分配映射")
        check(str(ds_a2.ReferencedSOPInstanceUID) == target_a1,
              "顶层 ReferencedSOPInstanceUID 同批 SOP 引用已替换")
        check(str(ds_a2.ReferencedPatientSequence[0].PatientID) == target_patient,
              "ReferencedPatientSequence 中 PatientID 已替换")
        check(str(ds_a2.ReferencedSOPClassUID) == SOP_CLASS_CT, "SOP Class UID 保持不变")
        check(str(ds_a2.ReferencedStudySequence[0].ReferencedSOPSequence[0].ReferencedSOPClassUID)
              == SOP_CLASS_SR, "嵌套 Referenced SOP Class UID 保持不变")
        check(str(ds_a2.file_meta.MediaStorageSOPClassUID) == SOP_CLASS_CT,
              "Media Storage SOP Class UID 保持不变")
        check(str(ds_a2.file_meta.MediaStorageSOPInstanceUID)
              == mapping_a[("sop_uid", SAMPLE_SOURCE_UIDS["sop_a2"])],
              "file_meta 中 Media Storage SOP Instance UID 已同步替换")
        check(bytes(ds_a2.PixelData) == bytes(range(32)), "像素字节逐字节不变")
        check(str(ds_a2.file_meta.TransferSyntaxUID) in (
            "1.2.840.10008.1.2", "1.2.840.10008.1.2.1"), "传输语法仍为未压缩")

        print("== 场景 2：跨批次引用（第二批引用第一批标识） ==")
        r = _upload(client, base, proj_a, sorted((samples / "batch_b").glob("*.dcm")))
        check(r.status_code == 201, f"batch_b 上传成功 (HTTP {r.status_code})")
        batch_b_id = r.json()["id"]
        mapping_a2 = _mapping_index(client, base, proj_a)
        check(mapping_a2[("sop_uid", SAMPLE_SOURCE_UIDS["sop_a1"])] == target_a1,
              "跨批次复用：SOP A1 目标映射与第一批完全一致")
        check(mapping_a2[("study_uid", SAMPLE_SOURCE_UIDS["study"])] == target_study,
              "跨批次复用：Study 目标映射与第一批完全一致")
        zipped_b = _download_zip(client, base, batch_b_id)
        ds_b = zipped_b["b1_cross_batch.dcm"]
        check(str(ds_b.ReferencedSeriesSequence[0].SeriesInstanceUID)
              == mapping_a2[("series_uid", SAMPLE_SOURCE_UIDS["series_a1"])],
              "跨批次 Series 引用替换为持久映射")
        check(str(ds_b.ReferencedSeriesSequence[0].ReferencedSOPSequence[0].ReferencedSOPInstanceUID)
              == target_a1, "跨批次 SOP 引用替换为持久映射")
        check(bytes(ds_b.PixelData) == bytes(range(32)), "第二批像素字节不变")

        print("== 场景 3：同源并发（两个并发批次含相同源标识） ==")
        same_files = sorted((samples / "batch_a").glob("*.dcm"))
        with ThreadPoolExecutor(max_workers=2) as pool:
            futs = [
                pool.submit(_upload, client, base, proj_a, same_files),
                pool.submit(_upload, client, base, proj_a, same_files),
            ]
            results = [f.result() for f in futs]
        statuses = sorted(r.status_code for r in results)
        check(statuses == [201, 201], f"两个并发批次均发布成功 {statuses}")
        if statuses == [201, 201]:
            ids = {r.json()["id"] for r in results}
            check(len(ids) == 2, "并发产生两个独立批次")
            mapping_after = _mapping_index(client, base, proj_a)
            check(mapping_after[("sop_uid", SAMPLE_SOURCE_UIDS["sop_a1"])] == target_a1,
                  "并发后同一源 SOP 仍只对应同一目标（无分叉映射）")
            check(len(mapping_after) == len(mapping_a2),
                  "并发批次未产生重复/分叉映射行")

        print("== 场景 4：项目隔离 ==")
        r = _upload(client, base, proj_b, sorted((samples / "batch_a").glob("*.dcm")))
        check(r.status_code == 201, f"项目 B 批次上传成功 (HTTP {r.status_code})")
        mapping_b = _mapping_index(client, base, proj_b)
        check(mapping_b[("sop_uid", SAMPLE_SOURCE_UIDS["sop_a1"])] != target_a1,
              "不同项目相同源 SOP 得到不同目标")
        check(mapping_b[("patient_id", SAMPLE_SOURCE_UIDS["patient"])] != target_patient,
              "不同项目相同 PatientID 得到不同目标")

        print("== 场景 5：坏文件/压缩文件整批拒绝 ==")
        r_bad = _upload(client, base, proj_a, sorted((samples / "bad").glob("*.dcm")))
        check(r_bad.status_code == 400, f"坏文件被拒绝 (HTTP {r_bad.status_code})")
        r_zip = _upload(client, base, proj_a, sorted((samples / "compressed").glob("*.dcm")))
        check(r_zip.status_code == 400, f"压缩 DICOM 被拒绝 (HTTP {r_zip.status_code})")
        # 混合批次：一份好文件 + 一份坏文件，必须整体失败
        mixed = [samples / "batch_b" / "b1_cross_batch.dcm", samples / "bad" / "broken.dcm"]
        r_mix = _upload(client, base, proj_a, mixed)
        check(r_mix.status_code == 400, "好坏混合批次整体拒绝")
        batches = client.get(f"{base}/api/batches", params={"project_id": proj_a}).json()
        check(all(b["status"] != "PENDING" for b in batches), "不存在遗留 PENDING 批次")
        # 解析/限额阶段失败按设计不产生批次行（无部分批次）；转换/发布阶段失败才留 FAILED 审计
        check(all(b["status"] in ("PUBLISHED", "FAILED") for b in batches),
              "所有批次均处于终态（PUBLISHED/FAILED）")
        mapping_clean = _mapping_index(client, base, proj_a)
        check(len(mapping_clean) == len(mapping_after),
              "失败尝试未污染已发布映射表")

        print("== 场景 6：限额 ==")
        big = samples / "batch_b" / "b1_cross_batch.dcm"
        many = [big] * 21
        r_many = _upload(client, base, proj_a, many)
        check(r_many.status_code in (400, 413), f"超过 20 份被拒绝 (HTTP {r_many.status_code})")

        print("== 场景 7：并发交错 + 可控故障（循环等待修复验证） ==")
        # 全新项目，保证 X/Y/Z 无任何既有映射；共享标识由种子批次先行发布
        ilv_proj = client.post(
            f"{base}/api/projects",
            data={"name": f"交错演示-{uuid.uuid4().hex[:8]}"},
        ).json()["id"]
        ilv = samples / "interleaved"
        r = _upload(client, base, ilv_proj, [ilv / "seed_shared.dcm"])
        check(r.status_code == 201, "种子批次发布共享 Patient/Study/Series")
        shared_before = _mapping_index(client, base, ilv_proj)

        t0 = time.monotonic()
        with ThreadPoolExecutor(max_workers=4) as pool:
            f_o = pool.submit(_upload, client, base, ilv_proj,
                              [ilv / "o_own_x.dcm"], 2.0, True)
            time.sleep(0.4)  # 确保 O 先保留 X
            f_a = pool.submit(_upload, client, base, ilv_proj,
                              [ilv / "a_refs_x_own_y.dcm"])
            time.sleep(0.4)
            f_b = pool.submit(_upload, client, base, ilv_proj,
                              [ilv / "b_refs_y_own_z.dcm"])
            r_o = f_o.result()   # O 受控失败并释放 X
            # O 失败后 C 到达：自身 X、引用 Z，与重试中的 A 竞争 X
            f_c = pool.submit(_upload, client, base, ilv_proj,
                              [ilv / "c_own_x_refs_z.dcm"])
            r_a, r_b, r_c = f_a.result(), f_b.result(), f_c.result()
        elapsed = time.monotonic() - t0

        check(r_o.status_code == 500, f"O 转换阶段受控失败 (HTTP {r_o.status_code})")
        check(r_a.status_code == 201 and r_b.status_code == 201 and r_c.status_code == 201,
              f"A/B/C 无需重传全部发布 (A={r_a.status_code}, B={r_b.status_code}, C={r_c.status_code})")
        check(elapsed < 30, f"交错场景总耗时 {elapsed:.1f}s，远低于 60 秒等待上限（未发生超时解环）")

        ilv_map = _mapping_index(client, base, ilv_proj)
        check(all(ilv_map.get(k) == v for k, v in shared_before.items()),
              "O 失败未改变任何已发布映射")
        tx = ilv_map.get(("sop_uid", ILV_UIDS["X"]))
        ty = ilv_map.get(("sop_uid", ILV_UIDS["Y"]))
        tz = ilv_map.get(("sop_uid", ILV_UIDS["Z"]))
        check(all((tx, ty, tz)), "X/Y/Z 各有且仅有一条已发布持久映射")

        batches = client.get(f"{base}/api/batches", params={"project_id": ilv_proj}).json()
        check(any(b["status"] == "FAILED" for b in batches), "O 批次已标记 FAILED")
        check(all(b["status"] != "PENDING" for b in batches), "无遗留 PENDING 批次")

        if r_a.status_code == r_b.status_code == r_c.status_code == 201 and all((tx, ty, tz)):
            ds_a = _download_zip(client, base, r_a.json()["id"])["a_refs_x_own_y.dcm"]
            check(str(ds_a.SOPInstanceUID) == ty, "A 包自身 SOP == 持久映射 Y 目标")
            check(str(ds_a.ReferencedSOPSequence[0].ReferencedSOPInstanceUID) == tx,
                  "A 包嵌套引用 == 持久映射 X 目标")
            ds_b = _download_zip(client, base, r_b.json()["id"])["b_refs_y_own_z.dcm"]
            check(str(ds_b.SOPInstanceUID) == tz, "B 包自身 SOP == 持久映射 Z 目标")
            check(str(ds_b.ReferencedSOPSequence[0].ReferencedSOPInstanceUID) == ty,
                  "B 包嵌套引用 == 持久映射 Y 目标")
            ds_c = _download_zip(client, base, r_c.json()["id"])["c_own_x_refs_z.dcm"]
            check(str(ds_c.SOPInstanceUID) == tx, "C 包自身 SOP == 持久映射 X 目标")
            check(str(ds_c.ReferencedSOPSequence[0].ReferencedSOPInstanceUID) == tz,
                  "C 包嵌套引用 == 持久映射 Z 目标")

    print()
    if failures:
        print(f"演示结束：{len(failures)} 项失败")
        if not args.keep:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)
        else:
            print(f"临时数据保留在 {tmp}")
        return 1
    print(f"全部断言通过。临时数据目录：{tmp}（{'已保留' if args.keep else '退出后可手动删除'}）")
    if not args.keep:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
