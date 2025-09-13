from __future__ import annotations

import datetime
from typing import Any, Dict, List


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def build_groups(
    buyback_docs: List[Dict[str, Any]],
    sell_docs: List[Dict[str, Any]],
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    Build unified trade-in groups from validated buyback (parents) and sell (children) docs.
    Expects:
      - buyback_docs[i]["sku_parts"] with {make, model, storage, condition}
      - sell_docs[i]["sku_parts"] with {make, model, storage, grade}
      - both sides enriched with full original 'source' document
    """
    groups: Dict[str, Dict[str, Any]] = {}

    # --- parents (buyback) ---
    for parent in buyback_docs:
        parts = parent.get("sku_parts") or {}
        make = parts.get("make")
        model = parts.get("model")
        storage = parts.get("storage")
        condition = parts.get("condition")

        if not (make and model and storage and condition):
            # Defensive: skip anything malformed (shouldn't happen after validation)
            continue

        key = f"{make}-{model}-{storage}-{condition}"

        if key not in groups:
            groups[key] = {
                "_id": key,
                "key": key,
                "parts": {"make": make, "model": model, "storage": storage, "condition": condition},
                "parents": [],
                "children": [],
                "counts": {"parents": 0, "children": 0, "active_children": 0},
                "synced_at": datetime.datetime.utcnow().isoformat(),
                "build_meta": {"run_id": run_id},
            }

        source_doc = parent.get("source") or {}
        parent_entry = {
            "id": parent.get("id"),
            "sku": parent.get("sku"),
            # ✅ pull through from original buyback listing
            "aestheticGradeCode": source_doc.get("aestheticGradeCode"),
            "productId": source_doc.get("productId"),
            "source": source_doc,
        }

        # Optional small dedupe by parent id
        existing_ids = {p.get("id") for p in groups[key]["parents"]}
        if parent_entry["id"] not in existing_ids:
            groups[key]["parents"].append(parent_entry)

    # --- children (sell) ---
    for child in sell_docs:
        parts = child.get("sku_parts") or {}
        make = parts.get("make")
        model = parts.get("model")
        storage = parts.get("storage")
        grade = parts.get("grade")

        if not (make and model and storage and grade):
            continue

        child_key = f"{make}-{model}-{storage}-{grade}"
        if child_key not in groups:
            # orphan sell listing (no parent buyback SKU) — ignore here
            continue

        source_doc = child.get("source") or {}
        child_entry = {
            "id": child.get("id"),
            "sku": child.get("sku"),
            "active": bool((source_doc.get("quantity") or 0) > 0),
            "quantity": source_doc.get("quantity"),
            "price": _num(source_doc.get("price")),
            "min_price": _num(source_doc.get("min_price")),
            "max_price": _num(source_doc.get("max_price")),
            "sku_parts": parts,
            "source": source_doc,
        }

        # Optional small dedupe by child id
        existing_ids = {c.get("id") for c in groups[child_key]["children"]}
        if child_entry["id"] not in existing_ids:
            groups[child_key]["children"].append(child_entry)

    # finalize counts
    for g in groups.values():
        g["counts"]["parents"] = len(g["parents"])
        g["counts"]["children"] = len(g["children"])
        g["counts"]["active_children"] = sum(1 for c in g["children"] if c.get("active"))

    # link CRACKED -> EXCELLENT for pricing reuse
    apply_cracked_alias(groups)

    return list(groups.values())

def apply_cracked_alias(groups: dict[str, dict]) -> None:
    """
    For any group whose condition is CRACKED, set
    price_alias_key to the corresponding EXCELLENT key (if it exists).
    No duplication of children or requests — we only create a pointer.
    """
    cracked_keys = [k for k, g in groups.items() if g["parts"]["condition"] == "CRACKED"]
    for ck in cracked_keys:
        p = groups[ck]["parts"]
        excellent_key = f"{p['make']}-{p['model']}-{p['storage']}-EXCELLENT"
        if excellent_key in groups:
            groups[ck]["price_alias_key"] = excellent_key
            groups[ck]["price_alias_reason"] = "CRACKED uses EXCELLENT prices"