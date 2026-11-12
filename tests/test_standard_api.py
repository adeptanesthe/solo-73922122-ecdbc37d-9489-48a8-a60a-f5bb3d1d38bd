"""End-to-end tests for the standardisation HTTP API and the recompute window.

Uses the same real-socket uvicorn fixture as the live LIS tests: every HL7
message goes through MLLP, and mapping replacement runs the real background
recompute (with an adjustable per-row delay).
"""

from __future__ import annotations

import time

import httpx
import pytest

from app.hl7 import mllp_frame

from tests.test_lis import make_message, server  # noqa: F401  (fixture re-use)
from tests.test_lis import run_async, send_async

GLU_MAP = [
    {"source_item": "GLU", "source_unit": "mmol/L",
     "standard_item": "GLUCOSE", "standard_unit": "mmol/L",
     "coefficient": "1"},
    {"source_item": "CREA", "source_unit": "umol/L",
     "standard_item": "CREATININE", "standard_unit": "mg/dL",
     "coefficient": "0.0113"},
]


def _send(server, control_id, sample, items, status="F"):
    text = make_message(control_id, sample,
                        [(c, v, u, status) for c, v, u in items])
    ack = run_async(send_async(server.port, mllp_frame(text.encode())))
    assert "MSA|AA" in ack
    return text


def _wait_task(client, url, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        task = client.get(url).json()
        if task["state"] in ("completed", "failed", "interrupted"):
            return task
        time.sleep(0.02)
    raise AssertionError("task did not finish in time")


def test_seed_mapping_and_unavailable_reasons(server):
    _send(server, "M1", "S1", [
        ("GLU", "5.12", "mmol/L"), ("K", "4.1", "mmol/L")])
    with httpx.Client(timeout=10) as client:
        mapping = client.get(server.url("/api/v1/standard/mapping")).json()
        assert mapping["state"] == "active"
        assert mapping["entries"] == []
        data = client.get(
            server.url("/api/v1/standard/samples/S1")).json()
        assert data["mapping_version"] == mapping["version_id"]
        by_code = {i["code"]: i for i in data["items"]}
        assert by_code["GLU"]["reason"] == "no_mapping"
        assert by_code["GLU"]["available"] is False
        assert client.get(
            server.url("/api/v1/standard/samples/nope")).status_code == 404


def test_replace_mapping_recomputes_and_serves_exact_values(server):
    _send(server, "M1", "S1", [
        ("GLU", "5.12", "mmol/L"), ("CREA", "92", "umol/L")])
    with httpx.Client(timeout=20) as client:
        resp = client.put(server.url("/api/v1/standard/mapping"),
                          json={"entries": GLU_MAP, "note": "first real map"})
        assert resp.status_code == 202
        started = resp.json()
        assert started["state"] == "running"
        task = _wait_task(
            client, server.url(f"/api/v1/standard/tasks/{started['task_id']}"))
        assert task["state"] == "completed"
        assert task["processed_rows"] == 2

        latest = client.get(server.url(
            "/api/v1/standard/tasks/latest")).json()
        assert latest["task_id"] == started["task_id"]
        assert client.get(server.url("/api/v1/standard/tasks")).json()[
            "tasks"][0]["task_id"] == started["task_id"]

        mapping = client.get(server.url("/api/v1/standard/mapping")).json()
        assert mapping["version_id"] == started["version_id"]
        data = client.get(
            server.url("/api/v1/standard/samples/S1")).json()
        by_code = {i["code"]: i for i in data["items"]}
        assert by_code["GLU"]["standard_item"] == "GLUCOSE"
        assert by_code["GLU"]["standard_value"] == "5.12"
        assert by_code["CREA"]["standard_value"] == "1.0396"
        assert by_code["CREA"]["standard_unit"] == "mg/dL"
        assert all(i["available"] for i in data["items"])

        # Raw tables are untouched.
        raw = client.get(server.url("/api/v1/samples/S1")).json()
        assert {i["code"]: i["value"] for i in raw["items"]} == {
            "GLU": "5.12", "CREA": "92"}
        msg = client.get(server.url("/api/v1/messages/M1")).json()
        assert msg["ack_code"] == "AA"


def test_replacement_rejects_invalid_payloads(server):
    with httpx.Client(timeout=10) as client:
        # coefficient zero
        bad = [dict(GLU_MAP[0], coefficient="0")]
        assert client.put(server.url("/api/v1/standard/mapping"),
                          json={"entries": bad}).status_code == 422
        # duplicate standard item
        dup = GLU_MAP + [{
            "source_item": "K", "source_unit": "mmol/L",
            "standard_item": "GLUCOSE", "standard_unit": "mmol/L",
            "coefficient": "1"}]
        assert client.put(server.url("/api/v1/standard/mapping"),
                          json={"entries": dup}).status_code == 422
        # 21 rows
        many = [{"source_item": f"C{i}", "source_unit": "u",
                 "standard_item": f"S{i}", "standard_unit": "u",
                 "coefficient": "1"} for i in range(21)]
        assert client.put(server.url("/api/v1/standard/mapping"),
                          json={"entries": many}).status_code == 422
        # no task was started by the rejected calls
        assert client.get(server.url("/api/v1/standard/tasks/latest")
                          ).status_code == 404


def test_only_one_recompute_runs_and_mllp_still_accepts_during_it(server):
    # Enough rows for the per-row delay to create an observable recompute
    # window in which MLLP traffic keeps flowing.
    _send(server, "M0", "S0", [
        ("GLU", "7.10", "mmol/L"), ("CREA", "92", "umol/L")])
    for n in range(8):
        _send(server, f"MB{n}", f"SB{n}", [
            ("GLU", str(4 + n), "mmol/L"), ("CREA", "80", "umol/L")])
    with httpx.Client(timeout=40) as client:
        first = client.put(server.url("/api/v1/standard/mapping"), json={
            "entries": GLU_MAP, "delay_ms": 50}).json()
        # Wait until the background task is observably in progress.
        deadline = time.time() + 5
        while time.time() < deadline:
            running = client.get(server.url(
                f"/api/v1/standard/tasks/{first['task_id']}")).json()
            if running["state"] == "running" and running["processed_rows"] >= 0 \
                    and running.get("started_at"):
                break
            time.sleep(0.01)
        # Second replacement while the first is running -> 409.
        conflict = client.put(server.url("/api/v1/standard/mapping"), json={
            "entries": [{"source_item": "K", "source_unit": "mmol/L",
                         "standard_item": "POTASSIUM", "standard_unit": "mmol/L",
                         "coefficient": "1"}]})
        assert conflict.status_code == 409

        # MLLP keeps accepting F/C while the recompute is in flight.
        _send(server, "M2", "S0", [("GLU", "5.80", "mmol/L")], status="C")
        _send(server, "M3", "S2", [
            ("CREA", "88.4956", "umol/L"), ("K", "4.1", "mmol/L")])

        task = _wait_task(
            client, server.url(f"/api/v1/standard/tasks/{first['task_id']}"),
            timeout=40)
        assert task["state"] == "completed"

        s1 = {i["code"]: i for i in client.get(
            server.url("/api/v1/standard/samples/S0")).json()["items"]}
        # The correction made during the recompute window is the final value;
        # an earlier sweep must never have overwritten it.
        assert s1["GLU"]["source_value"] == "5.80"
        assert s1["GLU"]["standard_value"] == "5.80"
        assert s1["GLU"]["control_id"] == "M2"

        s2 = {i["code"]: i for i in client.get(
            server.url("/api/v1/standard/samples/S2")).json()["items"]}
        assert s2["CREA"]["standard_value"] == "1.00000028"
        assert s2["K"]["available"] is False
        assert s2["K"]["reason"] == "no_mapping"

        # A further replacement is allowed once the slot is free, and the old
        # version remains queryable until the new one completes.
        second = client.put(server.url("/api/v1/standard/mapping"), json={
            "entries": [{"source_item": "GLU", "source_unit": "mmol/L",
                         "standard_item": "GLU_SI", "standard_unit": "mmol/L",
                         "coefficient": "1"}]}).json()
        _wait_task(client, server.url(
            f"/api/v1/standard/tasks/{second['task_id']}"))
        assert client.get(server.url("/api/v1/standard/mapping")).json()[
            "version_id"] == second["version_id"]


def test_unit_mismatch_does_not_block_reception(server):
    _send(server, "M1", "S1", [
        ("GLU", "5.12", "mg/dL"),          # unit differs from mapping
        ("CREA", "92", "umol/L")])
    with httpx.Client(timeout=20) as client:
        started = client.put(server.url("/api/v1/standard/mapping"),
                             json={"entries": GLU_MAP}).json()
        _wait_task(client, server.url(
            f"/api/v1/standard/tasks/{started['task_id']}"))
        data = {i["code"]: i for i in client.get(
            server.url("/api/v1/standard/samples/S1")).json()["items"]}
        assert data["GLU"]["available"] is False
        assert data["GLU"]["reason"] == "unit_mismatch"
        assert data["CREA"]["available"] is True
        # The AA receipt and the raw message are still stored.
        assert client.get(server.url("/api/v1/messages/M1")).json()[
            "ack_code"] == "AA"


def test_mapping_replace_is_full_table_not_merge(server):
    _send(server, "M1", "S1", [
        ("GLU", "5.0", "mmol/L"), ("CREA", "92", "umol/L")])
    with httpx.Client(timeout=20) as client:
        first = client.put(server.url("/api/v1/standard/mapping"),
                           json={"entries": GLU_MAP}).json()
        _wait_task(client, server.url(
            f"/api/v1/standard/tasks/{first['task_id']}"))
        # Replacement containing only GLU: CREA must lose its mapping.
        second = client.put(server.url("/api/v1/standard/mapping"), json={
            "entries": [{"source_item": "GLU", "source_unit": "mmol/L",
                         "standard_item": "GLUCOSE", "standard_unit": "mmol/L",
                         "coefficient": "1"}]}).json()
        _wait_task(client, server.url(
            f"/api/v1/standard/tasks/{second['task_id']}"))
        data = {i["code"]: i for i in client.get(
            server.url("/api/v1/standard/samples/S1")).json()["items"]}
        assert data["GLU"]["available"] is True
        assert data["CREA"]["available"] is False
        assert data["CREA"]["reason"] == "no_mapping"


def test_task_404_and_health(server):
    with httpx.Client(timeout=10) as client:
        assert client.get(
            server.url("/api/v1/standard/tasks/NOPE")).status_code == 404
        health = client.get(server.url("/health")).json()
        assert health["status"] == "ok"
        assert health["recalc_running"] is False


@pytest.mark.parametrize("coefficient,expected", [
    # Decimal scale is preserved: 7.10 has two fractional places.
    ("1000", "7100.00"),
    ("0.001", "0.00710"),
])
def test_exact_decimal_coefficients_over_http(server, coefficient, expected):
    _send(server, "M1", "S1", [("GLU", "7.10", "mmol/L")])
    entries = [{"source_item": "GLU", "source_unit": "mmol/L",
                "standard_item": "GLU_X", "standard_unit": "x",
                "coefficient": coefficient}]
    with httpx.Client(timeout=20) as client:
        started = client.put(server.url("/api/v1/standard/mapping"),
                             json={"entries": entries}).json()
        _wait_task(client, server.url(
            f"/api/v1/standard/tasks/{started['task_id']}"))
        item = client.get(server.url(
            "/api/v1/standard/samples/S1")).json()["items"][0]
        assert item["standard_value"] == expected
        assert item["coefficient"] == coefficient
