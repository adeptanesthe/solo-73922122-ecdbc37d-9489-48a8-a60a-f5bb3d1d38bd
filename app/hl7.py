"""HL7 v2.5.1 ORU^R01 parsing, validation and ACK building.

Supported subset (matching the analyser interface contract):

* ASCII encoding only (MSH-18 absent or ASCII); MLLP payload is normalised
  from ``\\r``, ``\\n`` or ``\\r\\n`` segment terminators to canonical ``\\r``.
* One MSH, exactly one OBR and 1..20 OBX segments with unique ``OBX-3.1``
  codes and numeric ``OBX-5`` values.
* ``OBX-11`` is restricted to ``F`` (add a new result) or ``C`` (correct an
  existing result).  Whether F/C is legal against the current database state
  is decided by :mod:`store`, inside the same transaction.
* ``OBR-3.1`` is the sample number, ``OBX-3.1`` the analysis code.

Any structural or value problem raises :class:`HL7Error`, which the caller
turns into an AE acknowledgement.  Parsing is side-effect free.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

# HL7 v2.5.1 table 0357 error codes used by this interface.
ERR_SEGMENT_SEQUENCE = "100"
ERR_FIELD_MISSING = "101"
ERR_DATA_TYPE = "102"
ERR_TABLE_VALUE = "103"
ERR_UNSUPPORTED_MESSAGE_TYPE = "200"
ERR_UNSUPPORTED_EVENT = "201"
ERR_UNSUPPORTED_VERSION = "203"
ERR_UNKNOWN_SEGMENT = "204"
ERR_DUPLICATE_KEY = "205"
ERR_APPLICATION_ERROR = "207"

MAX_OBX = 20

NUMERIC_RE = re.compile(r"\A[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\Z")
IDENT_RE = re.compile(r"\A[^\x00-\x1f\x7f|^~\\&]{1,64}\Z")
SAMPLE_RE = re.compile(r"\A[^\x00-\x1f\x7f|^~\\&]{1,64}\Z")
CODE_RE = re.compile(r"\A[^\x00-\x1f\x7f|^~\\&]{1,64}\Z")

VT = b"\x0b"
FS = b"\x1c"
CR = b"\x0d"
LF = b"\x0a"

OBX_STATUSES = {"F", "C"}
NUMERIC_VALUE_TYPES = {"NM", "SN", ""}


class HL7Error(Exception):
    """Message rejected during parsing/validation; maps to an AE ACK."""

    def __init__(self, code: str, text: str, segment: str = "", field_no: int = 0):
        super().__init__(text)
        self.code = code
        self.text = text
        self.segment = segment
        self.field_no = field_no


class MLLPError(Exception):
    """Malformed MLLP payload (framing/non-ASCII/empty)."""


@dataclass
class Envelope:
    """Best-effort MSH fields, usable even when strict parsing fails."""

    sending_app: str = ""
    sending_facility: str = ""
    receiving_app: str = ""
    receiving_facility: str = ""
    control_id: str = ""
    version: str = "2.5.1"

    @property
    def control_id_valid(self) -> bool:
        return bool(IDENT_RE.match(self.control_id))


@dataclass
class ObxItem:
    seq: int
    code: str          # OBX-3.1
    text: str          # OBX-3.2
    value: str         # OBX-5
    unit: str          # OBX-6.1
    status: str        # OBX-11 (F or C)
    value_type: str    # OBX-2


@dataclass
class ParsedMessage:
    control_id: str     # MSH-10
    message_type: str   # MSH-9
    version: str        # MSH-12
    sending_app: str
    sending_facility: str
    receiving_app: str
    receiving_facility: str
    sample_no: str      # OBR-3.1
    sample_no_filler: str
    specimen_ids: list[str] = field(default_factory=list)  # SPM-2.1
    ordering_provider: str = ""
    obx_items: list[ObxItem] = field(default_factory=list)
    raw_bytes: bytes = b""
    raw_hash: str = ""


# --------------------------------------------------------------------------- #
# MLLP helpers
# --------------------------------------------------------------------------- #

def normalize_payload(payload: bytes) -> bytes:
    """Return canonical ASCII, CR-terminated payload bytes (final CR retained).

    Raises :class:`MLLPError` for non-ASCII data, stray MLLP control bytes,
    NUL bytes or an empty message.
    """
    if not payload:
        raise MLLPError("empty message")
    if VT in payload or FS in payload:
        raise MLLPError("MLLP control character inside message payload")
    try:
        payload.decode("ascii")
    except UnicodeDecodeError as exc:
        raise MLLPError(f"non-ASCII payload: {exc}") from exc
    if b"\x00" in payload:
        raise MLLPError("NUL byte in payload")
    data = payload.replace(b"\r\n", b"\r").replace(b"\n", b"\r")
    data = data.strip(b"\r")
    if not data:
        raise MLLPError("empty message")
    return data + b"\r"


def message_hash(data: bytes) -> str:
    """Digest of the canonical message text (used for MSH-10 de-duplication)."""
    return hashlib.sha256(data).hexdigest()


def extract_envelope(data: bytes) -> Envelope:
    """Leniently extract MSH fields for building an error ACK."""
    try:
        text = data.decode("ascii", errors="ignore")
    except Exception:  # pragma: no cover - defensive
        return Envelope()
    text = text.replace("\r\n", "\r").replace("\n", "\r").strip("\r")
    first = text.split("\r", 1)[0] if text else ""
    if not first.startswith("MSH"):
        return Envelope()
    fields = first.split("|")

    def f(i: int) -> str:
        return fields[i] if i < len(fields) else ""

    def clean(value: str) -> str:
        return "".join(ch for ch in value if 32 <= ord(ch) < 127)[:64]

    version = clean(f(11)).strip()
    return Envelope(
        sending_app=clean(f(2)),
        sending_facility=clean(f(3)),
        receiving_app=clean(f(4)),
        receiving_facility=clean(f(5)),
        control_id=clean(f(9)).strip(),
        version=version or "2.5.1",
    )


def build_ack(
    envelope: Envelope,
    ack_code: str,
    text: str = "",
    err_code: str = "",
    err_segment: str = "",
    timestamp: Optional[datetime] = None,
    ack_control_id: Optional[str] = None,
) -> bytes:
    """Build an ASCII, CR-terminated ACK (MSH, MSA, optional ERR)."""
    ts = (timestamp or datetime.now(timezone.utc)).strftime("%Y%m%d%H%M%S")
    control = ack_control_id or ("ACK" + uuid.uuid4().hex[:14]).upper()
    msh = "|".join([
        "MSH", "^~\\&",
        envelope.sending_app, envelope.sending_facility,
        envelope.receiving_app, envelope.receiving_facility,
        ts, "",
        "ACK^R01^ACK", control, "P", envelope.version or "2.5.1",
    ])
    segments = [msh, "|".join(["MSA", ack_code, envelope.control_id,
                               text.replace("\r", " ").replace("|", " ")[:200]])]
    if err_code:
        err = "|".join([
            "ERR",
            f"{err_segment}^{err_code}^HL70357" if err_segment else f"{err_code}^HL70357",
            "",
            "E",
        ])
        segments.append(err)
    return ("\r".join(segments) + "\r").encode("ascii")


def mllp_frame(message: bytes) -> bytes:
    return VT + message + FS + CR


# --------------------------------------------------------------------------- #
# Strict parsing / validation
# --------------------------------------------------------------------------- #

def _segments(data: bytes) -> list[str]:
    return [s for s in data.decode("ascii").split("\r") if s]


def _fields(segment: str) -> list[str]:
    return segment.split("|")


def _field(fields: list[str], index: int) -> str:
    return fields[index] if index < len(fields) else ""


def _component(field: str, index: int) -> str:
    parts = field.split("^")
    return parts[index] if index < len(parts) else ""


def _checked_text(value: str, name: str, required: bool,
                  segment: str = "", field_no: int = 0,
                  max_len: int = 64, identifier: bool = False) -> str:
    value = value.strip()
    if not value:
        if required:
            raise HL7Error(ERR_FIELD_MISSING, f"{name} is required",
                           segment, field_no)
        return ""
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
        raise HL7Error(ERR_DATA_TYPE, f"{name} contains control characters",
                       segment, field_no)
    if len(value) > max_len:
        raise HL7Error(ERR_DATA_TYPE, f"{name} exceeds {max_len} characters",
                       segment, field_no)
    if identifier and not IDENT_RE.match(value):
        raise HL7Error(ERR_DATA_TYPE, f"{name} contains HL7 delimiters",
                       segment, field_no)
    return value


def parse_message(data: bytes) -> ParsedMessage:
    """Parse and fully validate one ORU^R01 message.

    Raises :class:`HL7Error` on the first structural/value violation.
    """
    segments = _segments(data)
    if not segments or not segments[0].startswith("MSH"):
        raise HL7Error(ERR_SEGMENT_SEQUENCE, "MSH must be the first segment", "MSH")

    msh = _fields(segments[0])
    if len(msh) < 12:
        raise HL7Error(ERR_SEGMENT_SEQUENCE, "MSH segment is incomplete", "MSH")
    if _field(msh, 1) != "^~\\&":
        raise HL7Error(ERR_TABLE_VALUE,
                       "MSH-2 encoding characters must be ^~\\&", "MSH", 2)

    version = _field(msh, 11).strip()
    if version != "2.5.1":
        raise HL7Error(ERR_UNSUPPORTED_VERSION,
                       f"unsupported HL7 version '{version or 'missing'}'; expected 2.5.1",
                       "MSH", 12)

    msg_type = _field(msh, 8).strip().upper()
    if _component(msg_type, 0) != "ORU":
        raise HL7Error(ERR_UNSUPPORTED_MESSAGE_TYPE,
                       f"unsupported message type '{msg_type}'; only ORU^R01 accepted",
                       "MSH", 9)
    if _component(msg_type, 1) != "R01":
        raise HL7Error(ERR_UNSUPPORTED_EVENT,
                       f"unsupported trigger event '{msg_type}'; only ORU^R01 accepted",
                       "MSH", 9)

    control_id = _checked_text(_field(msh, 9), "MSH-10", True, "MSH", 10,
                               identifier=True)
    sending_app = _checked_text(_field(msh, 2), "MSH-3", False, "MSH", 3)
    sending_facility = _checked_text(_field(msh, 3), "MSH-4", False, "MSH", 4)
    receiving_app = _checked_text(_field(msh, 4), "MSH-5", False, "MSH", 5)
    receiving_facility = _checked_text(_field(msh, 5), "MSH-6", False, "MSH", 6)

    processing_id = _component(_field(msh, 10), 0).strip().upper()
    if processing_id not in ("P", "T"):
        raise HL7Error(ERR_TABLE_VALUE,
                       "MSH-11 processing ID must be P or T", "MSH", 11)

    obr: Optional[list[str]] = None
    obr_seen_before_obx = True
    obx_fields: list[list[str]] = []
    specimen_ids: list[str] = []
    for seg in segments[1:]:
        f = _fields(seg)
        name = f[0]
        if name == "OBR":
            if obr is not None:
                raise HL7Error(ERR_SEGMENT_SEQUENCE,
                               "exactly one OBR segment is allowed", "OBR")
            obr = f
        elif name == "OBX":
            if obr is None:
                obr_seen_before_obx = False
            obx_fields.append(f)
        elif name == "SPM":
            specimen_id = _component(_field(f, 2), 0).strip()
            if specimen_id:
                specimen_ids.append(specimen_id[:64])
        elif name == "MSH":
            raise HL7Error(ERR_SEGMENT_SEQUENCE, "unexpected second MSH segment", "MSH")
        elif name == "":
            raise HL7Error(ERR_SEGMENT_SEQUENCE, "empty segment (double CR?)", "")
        else:
            raise HL7Error(ERR_UNKNOWN_SEGMENT, f"unsupported segment '{name}'", name)

    if obr is None:
        raise HL7Error(ERR_SEGMENT_SEQUENCE, "OBR segment is required", "OBR")
    if not obr_seen_before_obx:
        raise HL7Error(ERR_SEGMENT_SEQUENCE,
                       "OBX segments must follow the OBR segment", "OBR")
    if not obx_fields:
        raise HL7Error(ERR_SEGMENT_SEQUENCE, "at least one OBX segment is required", "OBX")
    if len(obx_fields) > MAX_OBX:
        raise HL7Error(ERR_DATA_TYPE,
                       f"at most {MAX_OBX} OBX segments are allowed", "OBX")

    # OBR-3 is an EI (entity identifier); the sample number is component 1.
    sample_no = _checked_text(_component(_field(obr, 3), 0),
                              "OBR-3.1", True, "OBR", 3, identifier=True)
    if not SAMPLE_RE.match(sample_no):
        raise HL7Error(ERR_DATA_TYPE, "OBR-3.1 sample number is invalid", "OBR", 3)
    filler = _component(_field(obr, 3), 1).strip()

    items: list[ObxItem] = []
    seen_codes: set[str] = set()
    for f in obx_fields:
        if len(f) < 6:
            raise HL7Error(ERR_SEGMENT_SEQUENCE, "OBX segment is incomplete", "OBX")
        set_id_text = _field(f, 1).strip()
        if not set_id_text.isdigit() or int(set_id_text) < 1 or int(set_id_text) > 999:
            raise HL7Error(ERR_DATA_TYPE,
                           "OBX-1 set ID must be an integer between 1 and 999", "OBX", 1)
        code = _component(_field(f, 3), 0).strip()
        if not code:
            raise HL7Error(ERR_FIELD_MISSING,
                           "OBX-3.1 observation code is required", "OBX", 3)
        if not CODE_RE.match(code):
            raise HL7Error(ERR_DATA_TYPE, "OBX-3.1 observation code is invalid", "OBX", 3)
        if code in seen_codes:
            raise HL7Error(ERR_DATA_TYPE,
                           f"duplicate OBX-3.1 code '{code}' in message", "OBX", 3)
        seen_codes.add(code)

        value_type = _field(f, 2).strip().upper()
        if value_type not in NUMERIC_VALUE_TYPES:
            raise HL7Error(ERR_DATA_TYPE,
                           f"OBX-2 value type '{value_type}' for '{code}' is not numeric",
                           "OBX", 2)

        value = _field(f, 5).strip()
        if not value:
            raise HL7Error(ERR_FIELD_MISSING,
                           f"OBX-5 value for '{code}' is required", "OBX", 5)
        if not NUMERIC_RE.match(value):
            raise HL7Error(ERR_DATA_TYPE,
                           f"OBX-5 value '{value}' for '{code}' is not numeric", "OBX", 5)
        if len(value) > 40:
            raise HL7Error(ERR_DATA_TYPE, f"OBX-5 value for '{code}' is too long", "OBX", 5)

        status = _field(f, 11).strip().upper()
        if status not in OBX_STATUSES:
            raise HL7Error(ERR_TABLE_VALUE,
                           f"OBX-11 status '{status or 'missing'}' for '{code}' "
                           "must be F or C", "OBX", 11)

        unit = _component(_field(f, 6), 0).strip()[:32]
        text = _component(_field(f, 3), 1).strip()[:64]
        items.append(ObxItem(seq=int(set_id_text), code=code, text=text, value=value,
                             unit=unit, status=status, value_type=value_type))

    return ParsedMessage(
        control_id=control_id,
        message_type="ORU^R01",
        version=version,
        sending_app=sending_app,
        sending_facility=sending_facility,
        receiving_app=receiving_app,
        receiving_facility=receiving_facility,
        sample_no=sample_no,
        sample_no_filler=filler,
        specimen_ids=specimen_ids,
        ordering_provider=_component(_field(obr, 16), 0).strip()[:64],
        obx_items=items,
        raw_bytes=data,
        raw_hash=message_hash(data),
    )
