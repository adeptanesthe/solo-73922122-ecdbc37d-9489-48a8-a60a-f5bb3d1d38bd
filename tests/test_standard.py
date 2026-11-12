"""Result standardisation: exact decimal math, mapping rules, availability."""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.standard import (
    REASON_NO_MAPPING,
    REASON_UNIT_MISMATCH,
    MappingValidationError,
    convert,
    exact_multiply,
    validate_coefficient,
)


def test_coefficient_accepts_positive_decimal_strings():
    assert validate_coefficient("1") == "1"
    assert validate_coefficient(" 0.01 ") == "0.01"
    assert validate_coefficient("18.815") == "18.815"
    assert validate_coefficient("1E-3") == "0.001"
    assert validate_coefficient("1000000.000001") == "1000000.000001"


@pytest.mark.parametrize("bad", [
    "0", "-1", "1.2.3", "abc", "", "NaN", "Infinity", "1e", "+1", "0.0",
])
def test_coefficient_rejects_non_positive_or_non_decimal(bad):
    with pytest.raises(MappingValidationError):
        validate_coefficient(bad)


def test_exact_multiply_is_decimal_not_float():
    # Classic float failure cases must come out exact.
    assert exact_multiply("0.1", "0.1") == "0.01"
    assert exact_multiply("5.12", "18.815") == str(Decimal("5.12") *
                                                    Decimal("18.815"))
    assert exact_multiply("92", "0.01") == "0.92"
    # No scientific notation even for tiny/large products.
    assert exact_multiply("0.0001", "0.0001") == "0.00000001"
    assert exact_multiply("123456789", "1000") == "123456789000"
    # Trailing zeros of an exact decimal product are rendered plainly.
    assert exact_multiply("2.50", "4") == "10.00"


def test_convert_available_uses_mapping():
    mapping = {"source_item": "GLU", "source_unit": "mmol/L",
               "standard_item": "GLUCOSE", "standard_unit": "mmol/L",
               "coefficient": "1"}
    out = convert("5.12", "mmol/L", mapping)
    assert out["available"] is True
    assert out["standard_value"] == "5.12"
    assert out["standard_item"] == "GLUCOSE"
    assert out["reason"] is None


def test_convert_no_mapping_is_unavailable_with_reason():
    out = convert("42", "mg/dL", None)
    assert out["available"] is False
    assert out["standard_value"] is None
    assert out["reason"] == REASON_NO_MAPPING
    assert out["standard_item"] is None


def test_convert_unit_mismatch_is_unavailable_with_reason():
    mapping = {"source_item": "CREA", "source_unit": "umol/L",
               "standard_item": "CREATININE", "standard_unit": "mg/dL",
               "coefficient": "0.0113"}
    out = convert("1.2", "mg/dL", mapping)
    assert out["available"] is False
    assert out["standard_value"] is None
    assert out["reason"] == REASON_UNIT_MISMATCH
    # The canonical identity is still reported so consumers know what failed.
    assert out["standard_item"] == "CREATININE"
    # Correct unit converts exactly.
    ok = convert("88.4956", "umol/L", mapping)
    assert ok["available"] is True
    assert ok["standard_value"] == str(
        Decimal("88.4956") * Decimal("0.0113"))
