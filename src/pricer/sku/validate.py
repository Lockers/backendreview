from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Literal

Status = Literal["OK", "MISSING", "FORMAT_INVALID", "VALUE_INVALID"]

# Exactly 6 tokens; MODEL/COLOUR allow SPACES but not hyphens.
# MAKE: uppercase letters/digits (no spaces, no hyphens)
# MODEL: uppercase letters/digits and spaces (no hyphens)
# COLOUR: uppercase letters/digits and spaces (no hyphens)
# STORAGE/SIM/GRADE are strict.
SKU_RE = re.compile(
    r"^(?P<make>[A-Z0-9]+)-"
    r"(?P<model>[A-Z0-9 ]+)-"
    r"(?P<colour>[A-Z0-9 ]+)-"
    r"(?P<storage>(?:64|128|256|512|1000|2000)GB|1TB|2TB)-"
    r"(?P<sim>SS|DS|ES)-"
    r"(?P<grade>FAIR|GOOD|EXCELLENT|BROKEN)$"
)

ALLOWED_STORAGE = {"64GB","128GB","256GB","512GB","1000GB","2000GB","1TB","2TB"}
ALLOWED_SIM = {"SS","DS","ES"}
ALLOWED_GRADE = {"FAIR","GOOD","EXCELLENT","BROKEN"}

def _utcnow():
    return datetime.now(timezone.utc)

@dataclass(frozen=True)
class SkuParts:
    make: str
    model: str
    colour: str
    storage: str
    sim: str
    grade: str

@dataclass(frozen=True)
class SkuValidation:
    status: Status
    errors: List[str]
    parts: SkuParts | None

def validate_sku(sku: str | None) -> SkuValidation:
    if not sku or not str(sku).strip():
        return SkuValidation(status="MISSING", errors=["missing"], parts=None)

    s = sku.strip()
    if s != s.upper():
        return SkuValidation(status="FORMAT_INVALID", errors=["not_uppercase"], parts=None)

    # quick hyphen-count check (exactly 5 separators => 6 tokens)
    if s.count("-") != 5:
        return SkuValidation(status="FORMAT_INVALID", errors=["bad_hyphen_count"], parts=None)

    m = SKU_RE.match(s)
    if not m:
        # Extra hints for common issues (hyphens inside model/colour, etc.)
        errs = ["regex_fail"]
        return SkuValidation(status="FORMAT_INVALID", errors=errs, parts=None)

    parts = SkuParts(
        make=m.group("make"),
        model=" ".join(m.group("model").split()),
        colour=" ".join(m.group("colour").split()),
        storage=m.group("storage"),
        sim=m.group("sim"),
        grade=m.group("grade"),
    )

    errors: List[str] = []
    if parts.storage not in ALLOWED_STORAGE:
        errors.append("bad_storage")
    if parts.sim not in ALLOWED_SIM:
        errors.append("bad_sim")
    if parts.grade not in ALLOWED_GRADE:
        errors.append("bad_grade")

    if errors:
        return SkuValidation(status="VALUE_INVALID", errors=errors, parts=parts)

    return SkuValidation(status="OK", errors=[], parts=parts)

def doc_for_good(listing_id: str, sku: str, v: SellSkuValidation, *, source: dict) -> dict:
    assert v.status == "OK" and v.parts
    return {
        "_id": listing_id,
        "id": listing_id,
        "sku": sku,
        "sku_status": v.status,
        "sku_parts": {
            "make": v.parts.make,
            "model": v.parts.model,
            "colour": v.parts.colour,
            "storage": v.parts.storage,
            "sim": v.parts.sim,
            "grade": v.parts.grade,
        },
        "source": source,                 # <— full original SELL listing
        "validated_at": _utcnow(),
    }

def doc_for_bad(listing_id: str, sku: str | None, v: SellSkuValidation, *, source: dict) -> dict:
    return {
        "_id": listing_id,
        "id": listing_id,
        "sku": sku,
        "sku_status": v.status,
        "sku_errors": list(v.errors),
        "source": source,                 # <— full original SELL listing
        "validated_at": _utcnow(),
    }

