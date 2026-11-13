"""循环等待修复的回归测试：O/A/B/C 交错上传 + 可控故障注入。

对应场景（同一项目，共享 Patient/Study/Series 已发布）：
  O 占 X 后转换受控失败；A 引用 X、自身 Y；B 引用 Y、自身 Z；
  O 失败释放 X 后 C 到达，自身 X、引用 Z。
修复前（占用-等待）：A 占 Y 等 X、B 占 Z 等 Y、C 占 X 等 Z —— 循环等待至 60s 超时；
修复后（阻塞即让路）：A/B/C 无需重传全部发布，且远快于 60 秒等待上限。
"""
from __future__ import annotations

import io
import threading
import time
import uuid
import zipfile

import pydicom
import pytest

from tests.conftest import upload

SOP_CLASS_CT = "1.2.840.10008.5.1.4.1.1.2"


@pytest.fixture()
def demo_hooks(client):
    """启用演示钩子（受控挂起/失败注入），测试结束还原。"""
    from app.config import settings

    object.__setattr__(settings, "demo_hooks_enabled", True)
    try:
        yield
    finally:
        object.__setattr__(settings, "demo_hooks_enabled", False)


def _upload_hooked(client, project_id, paths, hold=0.0, fail=False):
    files = [("files", (p.name, p.read_bytes(), "application/dicom")) for p in paths]
    data = {"project_id": project_id}
    if hold:
        data["demo_hold_seconds"] = str(hold)
    if fail:
        data["demo_fail"] = "true"
    return client.post("/api/batches", data=data, files=files)


def _mapping_index(client, project_id):
    rows = client.get("/api/mappings", params={"project_id": project_id}).json()
    return {(r["id_kind"], r["source_value"]): r for r in rows}


def _zip_dataset(client, batch_id, name):
    dl = client.get(f"/api/batches/{batch_id}/download")
    assert dl.status_code == 200
    with zipfile.ZipFile(io.BytesIO(dl.content)) as zf:
        return pydicom.dcmread(io.BytesIO(zf.read(name)))


def _nested_ref_sop(ds):
    return str(
        ds.ReferencedStudySequence[0].ReferencedSOPSequence[0].ReferencedSOPInstanceUID
    )


def _ilv_files(sample_dicom, tmp_path):
    """构造 O/A/B/C 交错场景的五份文件，返回 (files, uids)。"""
    unique = uuid.uuid4().int % 10**10
    patient = f"PAT-ILV-{unique}"
    study = f"1.2.840.1001.99.88.{unique}.1"
    series = f"{study}.1"
    uids = {
        "X": f"{study}.1.1",
        "Y": f"{study}.1.2",
        "Z": f"{study}.1.3",
        "seed": f"{study}.1.9",
    }

    def make(name, sop_uid, ref_sop=None):
        refs = {"ref_sop": (ref_sop, SOP_CLASS_CT)} if ref_sop else None
        return sample_dicom(
            tmp_path / name,
            patient_id=patient,
            study_uid=study,
            series_uid=series,
            sop_uid=sop_uid,
            refs=refs,
        )

    files = {
        "seed": make("seed.dcm", uids["seed"]),
        "O": make("o.dcm", uids["X"]),
        "A": make("a.dcm", uids["Y"], ref_sop=uids["X"]),
        "B": make("b.dcm", uids["Z"], ref_sop=uids["Y"]),
        "C": make("c.dcm", uids["X"], ref_sop=uids["Z"]),
    }
    return files, uids


def test_interleaved_circular_wait_all_publish(
    client, project_id, sample_dicom, tmp_path, demo_hooks
):
    """O/A/B/C 交错：修复前三者循环等待至 60s 超时，修复后全部发布。"""
    files, uids = _ilv_files(sample_dicom, tmp_path)
    assert upload(client, project_id, [files["seed"]]).status_code == 201
    shared_before = _mapping_index(client, project_id)
    batches_before = {
        b["id"] for b in client.get("/api/batches", params={"project_id": project_id}).json()
    }

    results = {}

    def run(role, hold=0.0, fail=False, delay=0.0):
        if delay:
            time.sleep(delay)
        results[role] = _upload_hooked(
            client, project_id, [files[role]], hold=hold, fail=fail
        )

    t0 = time.monotonic()
    threads = [
        threading.Thread(target=run, args=("O",), kwargs={"hold": 2.0, "fail": True}),
        threading.Thread(target=run, args=("A",), kwargs={"delay": 0.4}),
        threading.Thread(target=run, args=("B",), kwargs={"delay": 0.8}),
    ]
    for t in threads:
        t.start()
    threads[0].join()  # O 失败释放 X 后，C 到达（自身 X，引用 Z）
    run("C")
    for t in threads[1:]:
        t.join()
    elapsed = time.monotonic() - t0

    assert results["O"].status_code == 500
    assert results["A"].status_code == 201, results["A"].text
    assert results["B"].status_code == 201, results["B"].text
    assert results["C"].status_code == 201, results["C"].text
    # 关键：远未触及 60 秒等待上限（旧协议下三者各等满 60s 后 409）
    assert elapsed < 30

    mappings = _mapping_index(client, project_id)
    # O 的失败未改变任何已发布映射
    for key, row in shared_before.items():
        assert mappings[key]["target_value"] == row["target_value"]
    # X/Y/Z 各一条已发布持久映射
    mx = mappings[("sop_uid", uids["X"])]
    my = mappings[("sop_uid", uids["Y"])]
    mz = mappings[("sop_uid", uids["Z"])]
    assert mx["status"] == my["status"] == mz["status"] == "PUBLISHED"

    # 成功包的自身及嵌套引用 UID 与持久映射一致
    ds_a = _zip_dataset(client, results["A"].json()["id"], "a.dcm")
    assert str(ds_a.SOPInstanceUID) == my["target_value"]
    assert _nested_ref_sop(ds_a) == mx["target_value"]
    ds_b = _zip_dataset(client, results["B"].json()["id"], "b.dcm")
    assert str(ds_b.SOPInstanceUID) == mz["target_value"]
    assert _nested_ref_sop(ds_b) == my["target_value"]
    ds_c = _zip_dataset(client, results["C"].json()["id"], "c.dcm")
    assert str(ds_c.SOPInstanceUID) == mx["target_value"]
    assert _nested_ref_sop(ds_c) == mz["target_value"]

    # 批次终态：O FAILED、A/B/C PUBLISHED，无 PENDING 遗留
    batches = client.get("/api/batches", params={"project_id": project_id}).json()
    new_batches = [b for b in batches if b["id"] not in batches_before]
    assert len(new_batches) == 4  # O/A/B/C
    statuses = sorted(b["status"] for b in new_batches)
    assert statuses == ["FAILED", "PUBLISHED", "PUBLISHED", "PUBLISHED"]

    # 重启恢复后：已发布映射与 ZIP 保持不变
    from app.pipeline import recover_on_startup

    assert recover_on_startup() == []
    assert _mapping_index(client, project_id) == mappings
    dl = client.get(f"/api/batches/{results['A'].json()['id']}/download")
    assert dl.status_code == 200


def test_disjoint_batch_not_blocked_by_waiting_batch(
    client, project_id, sample_dicom, tmp_path, demo_hooks
):
    """等待中的批次不持写锁：无共享源标识的批次照常并行转换发布。"""
    unique = uuid.uuid4().int % 10**10
    patient = f"PAT-ILV-{unique}"
    study = f"1.2.840.1001.99.87.{unique}.1"
    series = f"{study}.1"
    uid_x = f"{study}.1.1"
    uid_y = f"{study}.1.2"

    seed = sample_dicom(tmp_path / "seed.dcm", patient_id=patient,
                        study_uid=study, series_uid=series, sop_uid=f"{study}.1.9")
    o = sample_dicom(tmp_path / "o.dcm", patient_id=patient,
                     study_uid=study, series_uid=series, sop_uid=uid_x)
    a = sample_dicom(tmp_path / "a.dcm", patient_id=patient,
                     study_uid=study, series_uid=series, sop_uid=uid_y,
                     refs={"ref_sop": (uid_x, SOP_CLASS_CT)})
    # 完全独立标识的批次
    d = sample_dicom(tmp_path / "d.dcm")

    assert upload(client, project_id, [seed]).status_code == 201

    results = {}
    done_at = {}

    def run(role, path, hold=0.0, delay=0.0):
        if delay:
            time.sleep(delay)
        results[role] = _upload_hooked(client, project_id, [path], hold=hold)
        done_at[role] = time.monotonic()

    threads = [
        threading.Thread(target=run, args=("O", o), kwargs={"hold": 3.0}),
        threading.Thread(target=run, args=("A", a), kwargs={"delay": 0.3}),
        threading.Thread(target=run, args=("D", d), kwargs={"delay": 0.6}),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results["O"].status_code == 201, results["O"].text
    assert results["A"].status_code == 201, results["A"].text
    assert results["D"].status_code == 201, results["D"].text
    # D 与 O/A 无共享源标识：O 仍挂起时 D 已完成发布（等待不持写锁、互不阻塞）
    assert done_at["D"] < done_at["O"]
    # A 必须等到 O 发布 X 之后才能完成
    assert done_at["A"] >= done_at["O"] - 1.0


def test_blocked_reservation_releases_own_pending_rows(client, project_id):
    """db 层直证“阻塞即让路”：等待中的批次不持有任何 PENDING 映射。"""
    from app import db

    uid_x = f"1.2.840.1001.99.77.{uuid.uuid4().int % 10**10}.1"
    uid_y = f"1.2.840.1001.99.77.{uuid.uuid4().int % 10**10}.2"
    holder_batch = str(uuid.uuid4())
    waiter_batch = str(uuid.uuid4())

    conn_holder = db.connect()
    held = db.reserve_mappings(conn_holder, project_id, holder_batch,
                               {"sop_uid": [uid_x]})
    assert uid_x in held["sop_uid"]

    result_box = {}

    def waiter():
        conn = db.connect()
        try:
            result_box["mapping"] = db.reserve_mappings(
                conn, project_id, waiter_batch, {"sop_uid": [uid_x, uid_y]}
            )
        finally:
            conn.close()

    t = threading.Thread(target=waiter)
    t.start()
    try:
        conn_check = db.connect()
        # 等 waiter 完成首轮保留尝试并进入阻塞等待
        deadline = time.monotonic() + 10
        while True:
            row = conn_check.execute(
                "SELECT status FROM batches WHERE id=?", (waiter_batch,)
            ).fetchone()
            if row is not None:
                break
            assert time.monotonic() < deadline, "waiter 未进入保留流程"
            time.sleep(0.05)
        time.sleep(0.5)  # 覆盖至少一个等待轮询周期
        # 关键断言：阻塞期间 waiter 不强占 Y（没有任何 PENDING 行）
        row_y = conn_check.execute(
            "SELECT * FROM id_mappings WHERE id_kind='sop_uid' AND source_value=?",
            (uid_y,),
        ).fetchone()
        assert row_y is None, "阻塞等待期间仍持有自己的 PENDING 映射（占映射等待）"
        conn_check.close()

        # holder 发布 X 后，waiter 应复用同一目标并自行保留 Y
        db.publish_batch(conn_holder, holder_batch, [], f"{holder_batch}.zip", 0)
        t.join(timeout=30)
    finally:
        if t.is_alive():  # pragma: no cover
            t.join(timeout=5)
        conn_holder.close()

    assert not t.is_alive()
    mapping = result_box["mapping"]
    assert mapping["sop_uid"][uid_x] == held["sop_uid"][uid_x]
    assert uid_y in mapping["sop_uid"]


def test_demo_hooks_disabled_by_default(client, project_id, sample_dicom, tmp_path):
    """未启用演示钩子时，demo_* 参数被忽略，批次正常发布。"""
    f = sample_dicom(tmp_path / "f.dcm")
    resp = _upload_hooked(client, project_id, [f], hold=1.0, fail=True)
    assert resp.status_code == 201


def test_interleaved_demo_endpoint(client, project_id, demo_hooks):
    """演示端点：完整跑通 O/A/B/C 交错场景并报告一致。"""
    resp = client.post("/api/demo/interleaved", data={"project_id": project_id})
    assert resp.status_code == 200, resp.text
    report = resp.json()
    assert report["ok"], report
    assert report["batches"]["O"]["status"] == "FAILED"
    assert report["batches"]["A"]["status"] == "PUBLISHED"
    assert report["batches"]["B"]["status"] == "PUBLISHED"
    assert report["batches"]["C"]["status"] == "PUBLISHED"
    assert report["elapsed_seconds"] < 30
    assert len(report["mappings"]) == 3
    # 演示生成的批次可正常下载
    batch_a = report["batches"]["A"]["batch_id"]
    assert client.get(f"/api/batches/{batch_a}/download").status_code == 200


def test_interleaved_demo_endpoint_requires_hooks(client, project_id):
    resp = client.post("/api/demo/interleaved", data={"project_id": project_id})
    assert resp.status_code == 404
