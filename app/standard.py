"""Exact-decimal standardisation of analyser results.

A mapping table entry pairs one source item (``source_item`` + ``source_unit``)
with one canonical item (``standard_item`` + ``standard_unit``) and a *positive
decimal coefficient* given as an exact decimal string.  A stored result is
converted with::

    standard_value = Decimal(source_value) * Decimal(coefficient)

No binary floating point is involved: :mod:`decimal` performs the multiplication
exactly (within a generous fixed precision), and the product is rendered without
scientific notation so downstream warehouse loads see plain decimal strings.

Results that cannot be standardised are never dropped: they are stored as
*unavailable* with a machine-readable reason:

* ``no_mapping``  — no mapping row exists for the source item code;
* ``unit_mismatch`` — a mapping exists for the code but its expected source unit
  differs from the unit carried by the result.

The functions in this module are pure and side-effect free; all persistence and
transaction boundaries live in :mod:`app.store`.
"""

from __future__ import annotations

import re
from decimal import Decimal, localcontext
from typing import Optional

# A positive finite decimal: optional sign-free digits with an optional decimal
# point, or scientific notation.  Zero / negative / NaN / Infinity are rejected.
_POSITIVE_DECIMAL_RE = re.compile(
    r"\A(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\Z"
)

# Exact product precision.  OBX-5 is capped at 40 characters and coefficients at
# 40, so 60 significant digits is comfortably beyond any real product length.
MATH_PRECISION = 60

REASON_NO_MAPPING = "no_mapping"
REASON_UNIT_MISMATCH = "unit_mismatch"


class MappingValidationError(ValueError):
    """A mapping payload is structurally invalid (maps to HTTP 422)."""


def validate_coefficient(text: str) -> str:
    """Return the canonical coefficient string, or raise on invalid input.

    Accepted strings are positive finite decimals such as ``"1"``, ``"0.01"``,
    ``"18.815"``, ``"1E-3"``.  Zero, negative numbers, NaN, Infinity and any
    non-decimal text are rejected.  The returned value is normalised through
    :class:`~decimal.Decimal` (trailing zeros preserved as given by the input's
    canonical decimal form).
    """
    if not isinstance(text, str) or not text.strip():
        raise MappingValidationError("coefficient must be a non-empty decimal string")
    text = text.strip()
    if not _POSITIVE_DECIMAL_RE.match(text):
        raise MappingValidationError(
            f"coefficient {text!r} is not a positive decimal string")
    value = Decimal(text)
    if not value.is_finite() or value <= 0:
        raise MappingValidationError(
            f"coefficient {text!r} must be strictly positive and finite")
    return format(value, "f")


def exact_multiply(source_value: str, coefficient: str) -> str:
    """Multiply two exact decimal strings and return a plain decimal string.

    The result never uses scientific notation and keeps meaningful trailing
    zeros (decimal arithmetic, not float).
    """
    with localcontext() as ctx:
        ctx.prec = MATH_PRECISION
        product = Decimal(source_value) * Decimal(coefficient)
    return format(product, "f")


def convert(source_value: str, source_unit: str,
            mapping: Optional[dict]) -> dict:
    """Standardise one result against an optional mapping row.

    ``mapping`` is a dict with keys ``source_unit``, ``standard_item``,
    ``standard_unit``, ``coefficient`` or ``None`` when no row exists for the
    source item.  Always returns a dict describing the standard row.
    """
    if mapping is None:
        return {
            "available": False,
            "standard_item": None,
            "standard_unit": None,
            "standard_value": None,
            "coefficient": None,
            "reason": REASON_NO_MAPPING,
        }
    if source_unit != mapping["source_unit"]:
        return {
            "available": False,
            "standard_item": mapping["standard_item"],
            "standard_unit": mapping["standard_unit"],
            "standard_value": None,
            "coefficient": mapping["coefficient"],
            "reason": REASON_UNIT_MISMATCH,
        }
    return {
        "available": True,
        "standard_item": mapping["standard_item"],
        "standard_unit": mapping["standard_unit"],
        "standard_value": exact_multiply(source_value, mapping["coefficient"]),
        "coefficient": mapping["coefficient"],
        "reason": None,
    }


def index_mappings(rows: list[dict]) -> dict[str, dict]:
    """Index validated mapping rows by source item code."""
    return {row["source_item"]: row for row in rows}
