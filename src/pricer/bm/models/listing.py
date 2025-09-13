from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Literal, TypedDict


Grade = Literal["PREMIUM", "EXCELLENT", "VERY_GOOD", "GOOD", "FAIR", "STALLONE"]


class Listing(TypedDict, total=False):
    id: str
    listing_id: int | None
    product_id: str
    sku: str
    grade: Grade
    publication_state: int
    quantity: int
    active: bool
    price: str | None
    min_price: str | None
    max_price: str | None
    currency: str
    title: str
    comment: str | None
    warranty_delay: int


def _money_str(v: str | None) -> str | None:
    if v is None:
        return None
    try:
        # Normalize to two decimals as string (no float!)
        q = Decimal(v).quantize(Decimal("0.01"))
        return f"{q}"
    except (InvalidOperation, ValueError):
        return None


def normalize_listing(raw: dict[str, Any], anomalies: list[str]) -> Listing:
    listing: Listing = {
        "id": raw.get("id"),
        "listing_id": raw.get("listing_id"),
        "product_id": raw.get("product_id"),
        "sku": raw.get("sku"),
        "grade": str(raw.get("grade") or "").upper() or "GOOD",
        "publication_state": int(raw.get("publication_state") or 0),
        "quantity": int(raw.get("quantity") or 0),
        "currency": raw.get("currency") or "GBP",
        "title": raw.get("title") or "",
        "comment": raw.get("comment"),
        "warranty_delay": int(raw.get("warranty_delay") or 0),
    }

    listing["active"] = listing["quantity"] > 0

    price = _money_str(raw.get("price"))
    min_price = _money_str(raw.get("min_price"))
    max_price = _money_str(raw.get("max_price"))

    if raw.get("price") is not None and price is None:
        anomalies.append(f"money_parse:price:{raw.get('price')}")
    if raw.get("min_price") is not None and min_price is None:
        anomalies.append(f"money_parse:min_price:{raw.get('min_price')}")
    if raw.get("max_price") is not None and max_price is None:
        anomalies.append(f"money_parse:max_price:{raw.get('max_price')}")

    listing["price"] = price
    listing["min_price"] = min_price
    listing["max_price"] = max_price

    return listing
