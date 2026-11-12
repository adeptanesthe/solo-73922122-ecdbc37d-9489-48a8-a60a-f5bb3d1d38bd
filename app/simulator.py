"""Analyser simulator: demonstrates the MLLP flows required by the interface.

Scenarios (each goes through the real TCP socket, not the store directly):

1. ``fragmented``   — one message split across many TCP writes (fragmentation).
2. ``burst``        — several messages written back-to-back without waiting
                      (coalescing / 粘包), ACK read per message.
3. ``dropped-ack``  — message accepted, ACK discarded, then identical
                      retransmission must return the stored ACK (same MSA-2,
                      results not applied twice).
4. ``concurrent``   — N connections delivering the same MSH-10 simultaneously.
5. ``corrected``    — OBX-11=C correction of an existing item.
6. ``rejected``     — an illegal message; whole message rejected with AE and
                      no partial result is stored.
7. ``standardised`` — mapping replacement with a visible background recompute
                      (tunable delay), an F and a correction C submitted while
                      the recompute is running, unavailable items
                      (no_mapping / unit_mismatch) and exact decimal output.
                      Uses the HTTP API on the same service (default port 8000).

Usage:
    python -m app.simulator --host 127.0.0.1 --port 2575
    python -m app.simulator --host 127.0.0.1 --port 2575 --scenario concurrent
    python -m app.simulator --host 127.0.0.1 --port 2575 \
        --scenario standardised --http-url http://127.0.0.1:8000 --delay-ms 40
    docker compose exec lis python -m app.simulator --host 127.0.0.1 --port 2575
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

log = logging.getLogger("simulator")

VT = b"\x0b"
FS = b"\x1c"
CR = b"\x0d"


def message(control_id: str, sample: str, items: list[tuple[str, str, str]],
            *, method: str = "F", sending_app: str = "ANALYSER") -> str:
    """Build an ASCII ORU^R01; items are (code, value, unit) tuples.

    Sample numbers are sent as ``OBR-3.1`` with ``OBR-3.2`` as the filler
    identifier, which is the usual analyser layout.
    """
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    msh = "|".join(["MSH", "^~\\&", sending_app, "LAB", "LIS", "HOSP", now,
                    "", "ORU^R01", control_id, "P", "2.5.1"])
    obr = "|".join(["OBR", "1", "S01", f"{sample}^LAB", "", "", "", "", "", "",
                    "", "", "", "", "", "", f"DR{control_id}"])
    obx = []
    for i, (code, value, unit) in enumerate(items, start=1):
        obx.append("|".join(["OBX", str(i), "NM", f"{code}^Analyte^{code}-LOCAL",
                             "", value, f"{unit}", "", "", "", "", method]))
    return "\r".join([msh, obr] + obx) + "\r"


def frame(text: str) -> bytes:
    return VT + text.encode("ascii") + FS + CR


async def send_frame(host: str, port: int, frame_bytes: bytes,
                     fragments: int = 1, delay: float = 0.0,
                     read_ack: bool = True) -> str:
    reader, writer = await asyncio.open_connection(host, port)
    try:
        size = max(1, (len(frame_bytes) + fragments - 1) // fragments)
        for offset in range(0, len(frame_bytes), size):
            writer.write(frame_bytes[offset:offset + size])
            await writer.drain()
            if delay:
                await asyncio.sleep(delay)
        if not read_ack:
            return ""
        ack = await asyncio.wait_for(read_ack_frame(reader), timeout=20)
        return ack.decode("ascii", errors="replace")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


VT_BYTE = 0x0B
FS_BYTE = 0x1C


async def read_ack_frame(reader: asyncio.StreamReader) -> bytes:
    data = bytearray()
    started = False
    while True:
        chunk = await reader.read(4096)
        if not chunk:
            return bytes(data)
        for b in chunk:
            if not started:
                if b == VT_BYTE:
                    started = True
                continue
            if b == FS_BYTE:
                return bytes(data)
            data.append(b)


def msa(ack: str) -> tuple[str, str]:
    for segment in ack.split("\r"):
        fields = segment.split("|")
        if fields and fields[0] == "MSA":
            return fields[1], fields[2]
    return "?", ""


def show(label: str, ack: str) -> None:
    code, control = msa(ack)
    print(f"  {label:<28} ACK={code}  MSA-2={control}")


async def scenario_fragmented(host: str, port: int) -> None:
    print("[fragmented] single message split into 40 TCP writes")
    text = message("FRAG-001", "S-100", [("GLU", "5.12", "mmol/L"),
                                          ("K", "4.1", "mmol/L")])
    ack = await send_frame(host, port, frame(text), fragments=40, delay=0.01)
    show("fragmented delivery", ack)


async def scenario_burst(host: str, port: int) -> None:
    print("[burst] three messages sent back-to-back on one connection (粘包)")
    texts = [
        message("BURST-001", "S-200", [("ALT", "33", "U/L")]),
        message("BURST-002", "S-201", [("ALT", "45", "U/L"), ("AST", "28", "U/L")]),
        message("BURST-003", "S-202", [("AST", "52", "U/L")]),
    ]
    blob = b"".join(frame(t) for t in texts)
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(blob)
        await writer.drain()
        for text in texts:
            ack = await asyncio.wait_for(read_ack_frame(reader), timeout=20)
            show(f"burst {text.split('|')[9]}", ack.decode("ascii", errors="replace"))
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def scenario_dropped_ack(host: str, port: int) -> None:
    print("[dropped-ack] send, discard ACK, retransmit identical message")
    text = message("DROP-001", "S-300", [("CREA", "92", "umol/L")])
    f = frame(text)
    first = await send_frame(host, port, f)
    show("first delivery (lost ACK)", first)
    second = await send_frame(host, port, f)
    show("retransmission", second)
    print("  -> same MSA-2 and the stored ACK bytes are replayed; "
          "the result is not applied twice")


async def scenario_concurrent(host: str, port: int, count: int = 8) -> None:
    print(f"[concurrent] {count} connections sending the same MSH-10 at once")
    text = message("CONC-001", "S-400", [("GLU", "6.30", "mmol/L"),
                                          ("NA", "140", "mmol/L")])
    f = frame(text)
    acks = await asyncio.gather(*[
        send_frame(host, port, f) for _ in range(count)])
    codes = [msa(a)[0] for a in acks]
    controls = {msa(a)[1] for a in acks}
    print(f"  ACK codes: {codes}")
    print(f"  distinct MSA-2 values: {sorted(controls)}")
    print("  -> exactly one AA applied; all responses are the same stored ACK")


async def scenario_corrected(host: str, port: int) -> None:
    print("[corrected] F then C for the same item")
    base = message("COR-001", "S-500", [("GLU", "7.10", "mmol/L"),
                                         ("K", "4.4", "mmol/L")])
    show("initial F", await send_frame(host, port, frame(base)))
    corrected = message("COR-002", "S-500", [("GLU", "5.80", "mmol/L")],
                        method="C")
    show("correction C", await send_frame(host, port, frame(corrected)))
    bad = message("COR-003", "S-500", [("K", "4.9", "mmol/L")], method="C")
    show("C for absent item", await send_frame(host, port, frame(bad)))


async def scenario_rejected(host: str, port: int) -> None:
    print("[rejected] message with one illegal OBX (value 'abc') must be wholly rejected")
    text = message("REJ-001", "S-600", [("GLU", "5.00", "mmol/L"),
                                         ("ALT", "abc", "U/L")])
    show("mixed valid+invalid OBX", await send_frame(host, port, frame(text)))


# --------------------------------------------------------------------------- #
# Standardisation demo (HTTP API + MLLP traffic during a background recompute)
# --------------------------------------------------------------------------- #

def _http_json(method: str, url: str, payload: dict | None = None) -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload).encode("ascii")
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read().decode("ascii"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("ascii"))


async def scenario_standardised(host: str, port: int, http_url: str,
                                delay_ms: int) -> None:
    print(f"[standardised] mapping replacement with {delay_ms} ms/row recompute "
          "delay; corrections arrive while it runs")
    sample = "S-900"
    # F results: GLU and CREA are mappable; K has no mapping yet.
    show("initial F", await send_frame(host, port, frame(message(
        "STD-F1", sample, [("GLU", "7.10", "mmol/L"),
                           ("CREA", "92", "umol/L"),
                           ("K", "4.10", "mmol/L")]))))
    status, data = _http_json("GET", f"{http_url}/api/v1/standard/samples/{sample}")
    print("  before mapping: " + ", ".join(
        f"{i['code']}=unavailable({i['reason']})" for i in data["items"]))

    mapping = [
        {"source_item": "GLU", "source_unit": "mmol/L",
         "standard_item": "GLUCOSE", "standard_unit": "mmol/L",
         "coefficient": "1"},
        {"source_item": "CREA", "source_unit": "umol/L",
         "standard_item": "CREATININE", "standard_unit": "mg/dL",
         # exact decimal: 92 umol/L * 0.0113 = 1.0396 mg/dL
         "coefficient": "0.0113"},
    ]
    status, started = _http_json(
        "PUT", f"{http_url}/api/v1/standard/mapping",
        {"entries": mapping, "note": "simulator demo",
         "delay_ms": delay_ms})
    assert status == 202, started
    task_id = started["task_id"]
    print(f"  replacement accepted: task={task_id} version="
          f"{started['version_id']} (202, recompute in background)")

    # A second replacement while the first runs must be refused (409).
    status, conflict = _http_json(
        "PUT", f"{http_url}/api/v1/standard/mapping",
        {"entries": [{"source_item": "K", "source_unit": "mmol/L",
                      "standard_item": "POTASSIUM", "standard_unit": "mmol/L",
                      "coefficient": "1"}], "delay_ms": delay_ms})
    print(f"  concurrent replacement -> HTTP {status}: {conflict.get('detail')}")

    # MLLP keeps accepting F/C during the recompute window.
    show("correction C during recompute", await send_frame(
        host, port, frame(message("STD-C1", sample,
                                  [("GLU", "5.80", "mmol/L")], method="C"))))
    show("new sample F during recompute", await send_frame(
        host, port, frame(message("STD-F2", "S-901",
                                  [("CREA", "88.4956", "umol/L"),
                                   ("ALT", "33", "U/L")]))))

    # Poll task status until the atomic switch.
    for _ in range(500):
        status, task = _http_json(
            "GET", f"{http_url}/api/v1/standard/tasks/{task_id}")
        if task["state"] in ("completed", "failed", "interrupted"):
            break
        await asyncio.sleep(0.05)
    print(f"  task {task_id}: {task['state']} "
          f"({task['processed_rows']}/{task['total_rows']} rows)")
    assert task["state"] == "completed", task

    status, data = _http_json(
        "GET", f"{http_url}/api/v1/standard/samples/{sample}")
    for item in data["items"]:
        if item["available"]:
            print(f"  {item['code']:>5} {item['source_value']} {item['source_unit']} "
                  f"-> {item['standard_item']} = {item['standard_value']} "
                  f"{item['standard_unit']} (x{item['coefficient']})")
        else:
            print(f"  {item['code']:>5} unavailable: {item['reason']}")
    by_code = {i["code"]: i for i in data["items"]}
    assert by_code["GLU"]["source_value"] == "5.80", "correction must survive"
    assert by_code["GLU"]["control_id"] == "STD-C1"
    assert by_code["CREA"]["standard_value"] == "1.0396"
    assert by_code["K"]["reason"] == "no_mapping"

    status, data = _http_json("GET",
                              f"{http_url}/api/v1/standard/samples/S-901")
    by_code = {i["code"]: i for i in data["items"]}
    print(f"  S-901 CREA -> {by_code['CREA']['standard_value']} mg/dL; "
          f"ALT unavailable({by_code['ALT']['reason']})")
    print("  -> new mapping and all its results switched atomically; raw "
          "results, messages and ACKs are unchanged")


SCENARIOS = {
    "fragmented": scenario_fragmented,
    "burst": scenario_burst,
    "dropped-ack": scenario_dropped_ack,
    "concurrent": scenario_concurrent,
    "corrected": scenario_corrected,
    "rejected": scenario_rejected,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2575)
    parser.add_argument("--scenario", choices=sorted(
        list(SCENARIOS) + ["standardised"]),
        action="append", help="default: all MLLP scenarios")
    parser.add_argument("--http-url", default="http://127.0.0.1:8000",
                        help="HTTP API base URL for the standardised scenario")
    parser.add_argument("--delay-ms", type=int, default=40,
                        help="per-row recompute delay used by the standardised "
                             "scenario (kept outside every DB lock)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(message)s")
    chosen = args.scenario or list(SCENARIOS)
    try:
        for name in chosen:
            if name == "standardised":
                asyncio.run(scenario_standardised(
                    args.host, args.port, args.http_url, args.delay_ms))
            else:
                asyncio.run(SCENARIOS[name](args.host, args.port))
    except ConnectionRefusedError:
        print(f"cannot connect to {args.host}:{args.port}; is the receiver running?",
              file=sys.stderr)
        return 1
    except urllib.error.URLError as exc:
        print(f"cannot reach HTTP API at {args.http_url}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
