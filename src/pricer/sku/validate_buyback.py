from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Literal

Status = Literal["OK", "MISSING", "FORMAT_INVALID", "VALUE_INVALID"]

ALLOWED_STORAGE = {"64GB","128GB","256GB","512GB","1000GB","2000GB","1TB","2TB"}
ALLOWED_CONDITION = {"FAIR","GOOD","EXCELLENT","BROKEN","CRACKED"}

# Exactly 4 tokens: MAKE - MODEL - STORAGE - CONDITION
# MODEL allows spaces but NO hyphens; MAKE has no spaces.
SKU_RE = re.compile(
    r"^(?P<make>[A-Z0-9]+)-"
    r"(?P<model>[A-Z0-9 ]+)-"
    r"(?P<storage>(?:64|128|256|512|1000|2000)GB|1TB|2TB)-"
    r"(?P<condition>FAIR|GOOD|EXCELLENT|BROKEN|CRACKED)$"
)

def _utcnow():
    return datetime.now(timezone.utc)

@dataclass(frozen=True)
class BuybackSkuParts:
    make: str
    model: str
    storage: str
    condition: str

@dataclass(frozen=True)
class BuybackSkuValidation:
    status: Status
    errors: List[str]
    parts: BuybackSkuParts | None

def validate_buyback_sku(sku: str | None) -> BuybackSkuValidation:
    if not sku or not str(sku).strip():
        return BuybackSkuValidation(status="MISSING", errors=["missing"], parts=None)

    s = sku.strip()
    if s != s.upper():
        return BuybackSkuValidation(status="FORMAT_INVALID", errors=["not_uppercase"], parts=None)

    if s.count("-") != 3:
        return BuybackSkuValidation(status="FORMAT_INVALID", errors=["bad_hyphen_count"], parts=None)

    m = SKU_RE.match(s)
    if not m:
        return BuybackSkuValidation(status="FORMAT_INVALID", errors=["regex_fail"], parts=None)

    parts = BuybackSkuParts(
        make=m.group("make"),
        model=" ".join(m.group("model").split()),
        storage=m.group("storage"),
        condition=m.group("condition"),
    )

    errors: List[str] = []
    if parts.storage not in ALLOWED_STORAGE:
        errors.append("bad_storage")
    if parts.condition not in ALLOWED_CONDITION:
        errors.append("bad_condition")

    if errors:
        return BuybackSkuValidation(status="VALUE_INVALID", errors=errors, parts=parts)

    return BuybackSkuValidation(status="OK", errors=[], parts=parts)

def doc_for_good(listing_id: str, sku: str, v: BuybackSkuValidation, *, source: dict) -> Dict:
    assert v.status == "OK" and v.parts
    return {
        "_id": listing_id,
        "id": listing_id,
        "sku": sku,
        "sku_status": v.status,
        "sku_parts": {
            "make": v.parts.make,
            "model": v.parts.model,
            "storage": v.parts.storage,
            "condition": v.parts.condition,
        },
        "source": source,                 # <— keep entire original buyback listing
        "validated_at": _utcnow(),
    }

def doc_for_bad(listing_id: str, sku: str | None, v: BuybackSkuValidation, *, source: dict) -> Dict:
    return {
        "_id": listing_id,
        "id": listing_id,
        "sku": sku,
        "sku_status": v.status,
        "sku_errors": list(v.errors),
        "source": source,                 # <— keep entire original buyback listing
        "validated_at": _utcnow(),
    }
