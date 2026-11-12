"""FastAPI application: MLLP TCP listener plus HTTP query/demo API."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field, field_validator

from .hl7 import (
    ERR_SEGMENT_SEQUENCE,
    HL7Error,
    MLLPError,
    build_ack,
    extract_envelope,
    mllp_frame,
    normalize_payload,
    parse_message,
)
from .recalc import RecalcManager
from .standard import MappingValidationError, validate_coefficient
from .store import ApplyError, MappingConflictError, Store

log = logging.getLogger("lis")

VT = b"\x0b"
FS = b"\x1c"
CR = b"\x0d"
VT_BYTE = 0x0B
FS_BYTE = 0x1C

MAX_MESSAGE_BYTES = 256 * 1024
READ_CHUNK = 4096


@dataclass
class Settings:
    db_path: str = os.environ.get("LIS_DB_PATH", "/data/lis.db")
    mllp_host: str = os.environ.get("LIS_MLLP_HOST", "0.0.0.0")
    mllp_port: int = int(os.environ.get("LIS_MLLP_PORT", "2575"))
    # Per-row artificial pause of a background recompute, used to make the
    # recompute window observable in demos (and to inject corrections in it).
    # Conversion sleeps outside every database lock, so ingestion is unaffected.
    recalc_delay_ms: int = int(os.environ.get("LIS_RECALC_DELAY_MS", "0"))

    @classmethod
    def from_env(cls) -> "Settings":
        return cls()


settings = Settings.from_env()
store = Store(settings.db_path)
recalc_manager = RecalcManager(store, default_delay_ms=settings.recalc_delay_ms)


# --------------------------------------------------------------------------- #
# MLLP server
# --------------------------------------------------------------------------- #

class MLLPServer:
    """Single-process asyncio MLLP listener.

    Each connection is handled independently, so interleaved/partial frames,
    multi-message bursts and concurrent connections are all supported.  Frames
    are delimited by VT ... FS+CR.  Anything before VT, after a frame and
    trailing CR/LF is skipped.  A connection sending more than one frame gets
    one ACK per frame, in order.
    """

    def __init__(self, host: str, port: int, store: Store):
        self.host = host
        self.port = port
        self.store = store
        self.server: Optional[asyncio.AbstractServer] = None
        self._conn_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self._handle_conn, self.host, self.port, limit=64 * 1024,
            reuse_address=True, backlog=256)
        log.info("MLLP listener bound on %s:%s", self.host, self.port)

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        for task in list(self._conn_tasks):
            task.cancel()
        if self._conn_tasks:
            await asyncio.gather(*self._conn_tasks, return_exceptions=True)

    async def _handle_conn(self, reader: asyncio.StreamReader,
                           writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._conn_tasks.add(task)
        peer = "?"
        try:
            peer = "%s:%s" % writer.get_extra_info("peername")[:2]
            in_frame = False
            payload = bytearray()
            while True:
                chunk = await reader.read(READ_CHUNK)
                if not chunk:
                    break
                for b in chunk:
                    if not in_frame:
                        if b == VT_BYTE:
                            in_frame = True
                            payload.clear()
                        # bytes outside a frame (e.g. stray CR) are ignored
                        continue
                    if b == FS_BYTE:
                        framed = bytes(payload)
                        ack = await self._handle_frame(framed, peer)
                        writer.write(mllp_frame(ack))
                        await writer.drain()
                        payload.clear()
                        in_frame = False
                        continue
                    if b == VT_BYTE:
                        # re-synchronise on a new start block
                        payload.clear()
                        continue
                    payload.append(b)
                    if len(payload) > MAX_MESSAGE_BYTES:
                        log.warning("%s oversized message; closing connection", peer)
                        payload.clear()
                        in_frame = False
                        raise ConnectionResetError("message too large")
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            pass
        except asyncio.CancelledError:
            pass
        except Exception:  # pragma: no cover - unexpected
            log.exception("MLLP connection error for %s", peer)
        finally:
            if task is not None:
                self._conn_tasks.discard(task)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _handle_frame(self, payload: bytes, peer: str) -> bytes:
        """Validate and apply one frame; return the ACK bytes to transmit."""
        try:
            data = normalize_payload(payload)
        except MLLPError as exc:
            log.warning("%s bad frame: %s", peer, exc)
            envelope = extract_envelope(payload)
            return build_ack(envelope, "AE", str(exc), ERR_SEGMENT_SEQUENCE)
        try:
            parsed = parse_message(data)
        except HL7Error as exc:
            log.info("%s rejected MSH-10=%r: %s", peer,
                     extract_envelope(data).control_id, exc.text)
            envelope = extract_envelope(data)
            return build_ack(envelope, "AE", exc.text, exc.code, exc.segment)
        try:
            # Small, bounded write transaction off the event loop so one slow
            # fsync cannot stall other connections.  The ACK is returned only
            # after COMMIT (see Store.apply); duplicates replay the stored ACK.
            result = await asyncio.to_thread(self.store.apply, parsed)
        except ApplyError as exc:
            envelope = extract_envelope(data)
            return build_ack(envelope, "AE", exc.text, exc.err_code, exc.segment)
        except Exception:
            log.exception("%s storage failure while applying %s", peer,
                          parsed.control_id)
            envelope = extract_envelope(data)
            return build_ack(envelope, "AE", "internal processing error", "207")
        if not result.applied:
            log.info("%s duplicate MSH-10=%s; replaying stored ACK", peer,
                     parsed.control_id)
        return result.ack


# --------------------------------------------------------------------------- #
# FastAPI wiring
# --------------------------------------------------------------------------- #

mllp = MLLPServer(settings.mllp_host, settings.mllp_port, store)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # init() also seeds an empty active mapping (first boot) and marks any
    # recompute task interrupted by a previous process shutdown.
    store.init()
    await mllp.start()
    app.state.started_at = time.monotonic()
    try:
        yield
    finally:
        await mllp.stop()


app = FastAPI(
    title="Analyser → LIS HL7 v2.5.1 ORU^R01 receiver with result standardisation",
    version="2.0.0",
    description=(
        "Single-process receiver for one analyser. MLLP on TCP "
        f"{settings.mllp_host}:{settings.mllp_port}; HTTP API on this port.\n\n"
        "Use **POST /api/v1/demo/messages** to push an HL7 message through the "
        "real MLLP socket with configurable fragmentation, then GET the result.\n\n"
        "The **standardisation** group manages whole-table mapping replacements "
        "(max 20 rows, exact Decimal multiplication), background recompute tasks "
        "and precomputed standardised result queries. Missing mappings or unit "
        "mismatches are stored as unavailable with a reason and never block "
        "message reception."
    ),
    lifespan=lifespan,
)


@app.get("/health", tags=["ops"])
def health() -> dict:
    return {"status": "ok", "mllp_port": settings.mllp_port,
            "db": settings.db_path,
            "recalc_running": recalc_manager.is_running()}

@app.get("/api/v1/samples", tags=["results"])
def list_samples(limit: int = 200) -> dict:
    return {"samples": store.samples(min(max(limit, 1), 1000))}


@app.get("/api/v1/samples/{sample_no}", tags=["results"])
def get_sample(sample_no: str) -> dict:
    if not sample_no or len(sample_no) > 64:
        raise HTTPException(status_code=400, detail="invalid sample number")
    data = store.sample_results(sample_no)
    if not data["items"] and not data["messages"]:
        raise HTTPException(status_code=404,
                            detail=f"no results for sample '{sample_no}'")
    return data


@app.get("/api/v1/messages/{control_id}", tags=["result-messages"])
def get_message(control_id: str) -> dict:
    message = store.message(control_id)
    if message is None:
        raise HTTPException(status_code=404,
                            detail=f"message '{control_id}' not found")
    return message


# --------------------------------------------------------------------------- #
# Standardisation API: mapping replacement, task status, standardised results
# --------------------------------------------------------------------------- #

MAX_MAPPING_ENTRIES = 20


class MappingEntry(BaseModel):
    source_item: str = Field(..., min_length=1, max_length=64,
                             description="Source/analyser observation code (OBX-3.1)")
    source_unit: str = Field("", max_length=32,
                             description="Expected source unit (OBX-6.1); a result "
                                         "with any other unit is marked unavailable")
    standard_item: str = Field(..., min_length=1, max_length=64,
                               description="Canonical warehouse item code; unique "
                                           "across the whole mapping")
    standard_unit: str = Field("", max_length=32,
                               description="Canonical unit for the warehouse")
    coefficient: str = Field(...,
                             description="Positive exact decimal multiplier string; "
                                         "standard = source * coefficient (Decimal)")

    @field_validator("source_item", "standard_item")
    @classmethod
    def _no_hl7_delimiters(cls, value: str) -> str:
        value = value.strip()
        if any(ch in value for ch in "|^~\\&") or any(
                ord(ch) < 32 or ord(ch) == 127 for ch in value):
            raise ValueError("item code must not contain HL7 delimiters or controls")
        return value

    @field_validator("coefficient")
    @classmethod
    def _positive_decimal(cls, value: str) -> str:
        try:
            return validate_coefficient(value)
        except MappingValidationError as exc:
            raise ValueError(str(exc)) from None

    @field_validator("source_unit", "standard_unit")
    @classmethod
    def _clean_unit(cls, value: str) -> str:
        value = value.strip()
        if any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
            raise ValueError("unit must not contain control characters")
        return value


class MappingReplaceRequest(BaseModel):
    entries: list[MappingEntry] = Field(
        ..., min_length=1, max_length=MAX_MAPPING_ENTRIES,
        description="Full replacement mapping (1..20 rows); source_item and "
                    "standard_item must each be unique")
    note: str = Field("", max_length=300, description="Free-form replacement note")
    delay_ms: Optional[int] = Field(
        None, ge=0, le=10000,
        description="Per-row recompute delay for this replacement; defaults to "
                    "LIS_RECALC_DELAY_MS. Applied outside every DB lock.")


def _check_entry_uniqueness(req: MappingReplaceRequest) -> list[dict]:
    entries = [e.model_dump() for e in req.entries]
    sources = [e["source_item"] for e in entries]
    standards = [e["standard_item"] for e in entries]
    if len(set(sources)) != len(sources):
        raise HTTPException(status_code=422,
                            detail="source_item values must be unique")
    if len(set(standards)) != len(standards):
        raise HTTPException(status_code=422,
                            detail="standard_item values must be unique")
    return entries


@app.get("/api/v1/standard/mapping", tags=["standardisation"])
def get_active_mapping() -> dict:
    """Return the mapping version currently used for stored standard results."""
    return store.current_mapping()


@app.put("/api/v1/standard/mapping", tags=["standardisation"],
         status_code=202)
def replace_mapping(req: MappingReplaceRequest) -> dict:
    """Replace the whole mapping (max 20 rows) and recompute in the background.

    The previous version stays active and keeps serving queries until the
    recompute finishes; the new mapping and all its standardised results then
    switch atomically.  Only one recompute runs at a time (409 while busy).
    """
    entries = _check_entry_uniqueness(req)
    try:
        return recalc_manager.start_replacement(
            entries, note=req.note, delay_ms=req.delay_ms)
    except MappingConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None


@app.get("/api/v1/standard/tasks", tags=["standardisation"])
def list_recalc_tasks(limit: int = 50) -> dict:
    return {"tasks": store.list_tasks(limit)}


@app.get("/api/v1/standard/tasks/latest", tags=["standardisation"])
def get_latest_task() -> dict:
    task = store.latest_task()
    if task is None:
        raise HTTPException(status_code=404, detail="no recompute task yet")
    return task


@app.get("/api/v1/standard/tasks/{task_id}", tags=["standardisation"])
def get_recalc_task(task_id: str) -> dict:
    task = store.task_status(task_id)
    if task is None:
        raise HTTPException(status_code=404,
                            detail=f"task '{task_id}' not found")
    return task


@app.get("/api/v1/standard/samples/{sample_no}", tags=["standardisation"])
def get_standard_sample(sample_no: str) -> dict:
    """Stored standardised results for a sample (no conversion at query time).

    Each item carries ``available`` plus, when unavailable, a ``reason`` of
    ``no_mapping`` or ``unit_mismatch``.  Unavailable items never affect the
    original message, its raw results or its receipt.
    """
    if not sample_no or len(sample_no) > 64:
        raise HTTPException(status_code=400, detail="invalid sample number")
    data = store.standard_sample_results(sample_no)
    if not data["items"]:
        raise HTTPException(
            status_code=404,
            detail=f"no standardised results for sample '{sample_no}'")
    return data


class DemoRequest(BaseModel):
    message: str = Field(..., description="HL7 message text (CR or LF line ends)")
    fragments: int = Field(1, ge=1, le=200,
                           description="Split the MLLP frame into this many TCP writes")
    delay_ms: int = Field(0, ge=0, le=5000, description="Delay between TCP writes")
    drop_ack: bool = Field(False,
                           description="Send the message, discard the first ACK "
                                       "as if it was lost, then resend it")


class DemoResponse(BaseModel):
    message: str = Field(..., description="Acknowledged message text")
    fragments: int
    ack_frames: list[str]
    ack_codes: list[str]
    stored_ack: Optional[str] = None
    note: Optional[str] = None


async def _send_fragments(host: str, port: int, frame: bytes, fragments: int,
                          delay_ms: float, read_ack: bool) -> Optional[bytes]:
    """Open one MLLP connection, write the frame in N pieces, read one ACK."""
    reader, writer = await asyncio.open_connection(host, port)
    try:
        size = max(1, (len(frame) + fragments - 1) // fragments)
        for offset in range(0, len(frame), size):
            writer.write(frame[offset:offset + size])
            await writer.drain()
            if delay_ms:
                await asyncio.sleep(delay_ms / 1000)
        if not read_ack:
            return None
        ack = await asyncio.wait_for(_read_frame(reader), timeout=15)
        return ack
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def _read_frame(reader: asyncio.StreamReader) -> bytes:
    while True:
        chunk = await reader.read(4096)
        if not chunk:
            return b""
        if FS in chunk:
            body = chunk.split(FS, 1)[0]
            if VT in body:
                body = body.split(VT, 1)[1]
            return body


def _ack_code(ack: bytes) -> str:
    text = ack.decode("ascii", errors="replace")
    for segment in text.split("\r"):
        parts = segment.split("|")
        if parts and parts[0] == "MSA" and len(parts) > 1:
            return parts[1]
    return "?"


@app.post("/api/v1/demo/messages", response_model=DemoResponse,
          tags=["demo"])
async def demo_message(req: DemoRequest) -> DemoResponse:
    """Send an HL7 message over the real MLLP socket (demo/test entry point).

    ``fragments`` splits the frame across multiple TCP writes to exercise
    reassembly; ``drop_ack`` transmits, discards the ACK, then retransmits the
    identical bytes to demonstrate stored-ACK replay.
    """
    body = req.message.replace("\r\n", "\n").replace("\n", "\r").strip("\r")
    if not body:
        raise HTTPException(status_code=422, detail="message must not be empty")
    try:
        encoded = body.encode("ascii", errors="strict")
    except UnicodeEncodeError:
        raise HTTPException(status_code=422, detail="message must be ASCII")
    frame = mllp_frame(encoded)
    envelope = extract_envelope(encoded)
    control_id = envelope.control_id

    ack_frames: list[str] = []
    note = None
    ack = await _send_fragments("127.0.0.1", settings.mllp_port, frame,
                                req.fragments, req.delay_ms, read_ack=True)
    ack_frames.append((ack or b"").decode("ascii", errors="replace"))
    if req.drop_ack:
        first_code = _ack_code(ack or b"")
        note = (f"First ACK ({first_code}) discarded as simulated transport loss; "
                "identical message retransmitted.")
        resent = await _send_fragments("127.0.0.1", settings.mllp_port, frame,
                                       req.fragments, req.delay_ms, read_ack=True)
        ack_frames.append((resent or b"").decode("ascii", errors="replace"))

    stored = store.message(control_id) if control_id else None
    return DemoResponse(
        message=body,
        fragments=req.fragments,
        ack_frames=[a for a in ack_frames if a],
        ack_codes=[_ack_code(a.encode("ascii", errors="ignore"))
                   for a in ack_frames if a],
        stored_ack=(stored["ack_message"] if stored else None),
        note=note,
    )


@app.post("/api/v1/demo/concurrent", tags=["demo"])
async def demo_concurrent(message: str, connections: int = 8) -> dict:
    """Send the same message over N connections at once (duplicate-ID race).

    Exactly one delivery applies the results; every connection must receive
    the same stored AA for the original MSH-10.
    """
    connections = max(2, min(connections, 32))
    body = message.replace("\r\n", "\n").replace("\n", "\r").strip("\r")
    if not body:
        raise HTTPException(status_code=422, detail="message must not be empty")
    try:
        encoded = body.encode("ascii", errors="strict")
    except UnicodeEncodeError:
        raise HTTPException(status_code=422, detail="message must be ASCII")
    frame = mllp_frame(encoded)
    control_id = extract_envelope(encoded).control_id

    async def deliver() -> str:
        ack = await _send_fragments("127.0.0.1", settings.mllp_port, frame,
                                    1, 0, read_ack=True)
        return (ack or b"").decode("ascii", errors="replace")

    acks = await asyncio.gather(*[deliver() for _ in range(connections)])
    stored = store.message(control_id) if control_id else None
    return {
        "control_id": control_id,
        "connections": connections,
        "ack_codes": [_ack_code(a.encode("ascii", errors="ignore")) for a in acks],
        "distinct_acks": len(set(acks)),
        "identical_acks": len(set(acks)) == 1,
        "stored_ack": stored["ack_message"] if stored else None,
    }


@app.get("/api/v1/demo/samples/{sample_no}", response_class=PlainTextResponse,
         tags=["demo"])
def demo_sample_text(sample_no: str) -> str:
    data = store.sample_results(sample_no)
    if not data["items"]:
        raise HTTPException(status_code=404, detail="no results")
    lines = [f"sample {sample_no}"]
    for item in data["items"]:
        lines.append(f"  {item['code']:>12} = {item['value']:<12} "
                     f"{item['unit']:<8} [{item['status']}] "
                     f"msg={item['control_id']} at {item['updated_at']}")
    return "\n".join(lines) + "\n"
