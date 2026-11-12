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
from pydantic import BaseModel, Field

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
from .store import ApplyError, Store

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

    @classmethod
    def from_env(cls) -> "Settings":
        return cls()


settings = Settings.from_env()
store = Store(settings.db_path)


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
    store.init()
    await mllp.start()
    app.state.started_at = time.monotonic()
    try:
        yield
    finally:
        await mllp.stop()


app = FastAPI(
    title="Analyser → LIS HL7 v2.5.1 ORU^R01 receiver",
    version="1.0.0",
    description=(
        "Single-process receiver for one analyser. MLLP on TCP "
        f"{settings.mllp_host}:{settings.mllp_port}; HTTP API on this port.\n\n"
        "Use **POST /api/v1/demo/messages** to push an HL7 message through the "
        "real MLLP socket with configurable fragmentation, then GET the result."
    ),
    lifespan=lifespan,
)


@app.get("/health", tags=["ops"])
def health() -> dict:
    return {"status": "ok", "mllp_port": settings.mllp_port,
            "db": settings.db_path}

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
