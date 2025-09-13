from __future__ import annotations

import copy
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------
# SKU parsing helpers (robust to minor format drift)
# ---------------------------

def _split_sku(sku: str) -> List[str]:
    """
    We treat '-' as the segment separator. Your SKUs look like:
      BUYBACK PARENT: MAKE-MODEL-STORAGE-CONDITION
      SELL CHILD:     MAKE-MODEL-COLOUR-STORAGE-SIM-GRADE
    where MODEL can contain spaces but not '-'.
    """
    return [p.strip() for p in (sku or "").split("-") if p.strip()]


def _parse_buyback_sku_parts(sku: str) -> Optional[Dict[str, str]]:
    parts = _split_sku(sku)
    # Expected: 4 parts -> MAKE, MODEL, STORAGE, CONDITION (e.g., EXCELLENT/CRACKED)
    if len(parts) < 4:
        return None
    make = parts[0]
    model = parts[1]
    storage = parts[-2]
    condition = parts[-1]
    return {
        "make": make,
        "model": model,
        "storage": storage,
        "condition": condition,
    }


def _parse_child_sku_parts(sku: str) -> Optional[Dict[str, str]]:
    parts = _split_sku(sku)
    # Expected: 6 parts -> MAKE, MODEL, COLOUR, STORAGE, SIM, GRADE
    # Be resilient: if we have more than 6, we still pick fields by convention.
    if len(parts) < 6:
        return None
    make = parts[0]
    model = parts[1]
    colour = parts[2]
    storage = parts[3]
    sim = parts[4]
    grade = parts[5]
    return {
        "make": make,
        "model": model,
        "colour": colour,
        "storage": storage,
        "sim": sim,
        "grade": grade,
    }


def _group_key_from_parts(parts: Dict[str, str]) -> str:
    # Canonical group key: MAKE-MODEL-STORAGE-CONDITION
    return f"{parts.get('make','')}-{parts.get('model','')}-{parts.get('storage','')}-{parts.get('condition','')}"


def _excellent_peer_key(make: str, model: str, storage: str) -> str:
    return f"{make}-{model}-{storage}-EXCELLENT"


def _cracked_peer_key(make: str, model: str, storage: str) -> str:
    return f"{make}-{model}-{storage}-CRACKED"


# ---------------------------
# Builders
# ---------------------------

def _init_group(key: str, parts: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "_id": key,
        "key": key,
        "parts": {
            "make": parts.get("make"),
            "model": parts.get("model"),
            "storage": parts.get("storage"),
            "condition": parts.get("condition"),
        },
        "parents": [],
        "children": [],
        "counts": {"parents": 0, "children": 0, "active_children": 0},
        "synced_at": None,
        "build_meta": {},
    }


def _simplify_buyback_parent(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": doc.get("id"),
        "sku": doc.get("sku"),
        "aestheticGradeCode": doc.get("aestheticGradeCode"),
        "productId": doc.get("productId"),
        "source": {
            **doc,
            "_id": doc.get("id"),
            "_source": "buyback",
        },
    }


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        # handles "1409.00" and 1409
        return float(x)
    except Exception:
        return None


def _simplify_sell_child(doc: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
    sku = doc.get("sku") or ""
    sku_parts = _parse_child_sku_parts(sku) or {}
    child = {
        "id": doc.get("id"),
        "sku": sku,
        "active": bool(doc.get("active")) if doc.get("active") is not None else (int(doc.get("quantity") or 0) > 0),
        "quantity": int(doc.get("quantity") or 0),
        "price": _to_float(doc.get("price")),
        "min_price": _to_float(doc.get("min_price")),
        "max_price": _to_float(doc.get("max_price")),
        "sku_parts": sku_parts or None,
        "source": {
            **doc,
            "_id": doc.get("id"),
            "_source": "sell",
        },
    }
    return child, sku_parts or None


def _ensure_group(groups: Dict[str, Dict[str, Any]], key: str, parts: Dict[str, Any]) -> Dict[str, Any]:
    if key not in groups:
        groups[key] = _init_group(key, parts)
    return groups[key]


def _fold_buyback_parents(groups: Dict[str, Dict[str, Any]], buyback_docs: List[Dict[str, Any]]) -> None:
    for doc in buyback_docs or []:
        sku = doc.get("sku") or ""
        parts = _parse_buyback_sku_parts(sku)
        if not parts:
            continue
        key = _group_key_from_parts(parts)
        g = _ensure_group(groups, key, parts)
        g["parents"].append(_simplify_buyback_parent(doc))


def _fold_sell_children(groups: Dict[str, Dict[str, Any]], sell_docs: List[Dict[str, Any]]) -> None:
    """
    Attach sell children to their natural group by (make, model, storage, grade-as-condition).
    Typically, your natural group will be EXCELLENT; other grades are tolerated if present.
    """
    for doc in sell_docs or []:
        child, sku_parts = _simplify_sell_child(doc)
        if not child or not sku_parts:
            continue
        make = sku_parts.get("make")
        model = sku_parts.get("model")
        storage = sku_parts.get("storage")
        grade = sku_parts.get("grade")
        if not (make and model and storage and grade):
            continue

        parts = {"make": make, "model": model, "storage": storage, "condition": grade}
        key = _group_key_from_parts(parts)
        g = _ensure_group(groups, key, parts)

        # De-duplicate by child id within the group
        cid = child.get("id")
        if cid and any(c.get("id") == cid for c in g["children"]):
            continue
        g["children"].append(child)


def ensure_cracked_groups_exist(groups: Dict[str, Dict[str, Any]]) -> None:
    """
    If EXCELLENT group exists but CRACKED doesn't, create an empty CRACKED peer.
    Parents may (or may not) exist from buyback; this just guarantees the group shell.
    """
    to_create: List[Tuple[str, Dict[str, Any]]] = []
    for key, grp in list(groups.items()):
        parts = grp.get("parts") or {}
        if parts.get("condition") != "EXCELLENT":
            continue
        make = parts.get("make")
        model = parts.get("model")
        storage = parts.get("storage")
        if not (make and model and storage):
            continue
        cracked_key = _cracked_peer_key(make, model, storage)
        if cracked_key not in groups:
            cracked_parts = {"make": make, "model": model, "storage": storage, "condition": "CRACKED"}
            to_create.append((cracked_key, cracked_parts))

    for key, parts in to_create:
        _ensure_group(groups, key, parts)


def clone_children_for_cracked(groups: Dict[str, Dict[str, Any]]) -> None:
    """
    For any group whose condition is CRACKED, clone the EXCELLENT group's children
    (sell listings) into the CRACKED group. We intentionally do NOT alter the
    children's SKU/grade — they remain the EXCELLENT sell children, just mirrored
    under the CRACKED trade-in parent for pricing correlation.
    """
    for key, grp in groups.items():
        parts = grp.get("parts") or {}
        if parts.get("condition") != "CRACKED":
            continue

        make = parts.get("make")
        model = parts.get("model")
        storage = parts.get("storage")
        if not (make and model and storage):
            continue

        excellent_key = _excellent_peer_key(make, model, storage)
        src = groups.get(excellent_key)
        if not src:
            # no EXCELLENT peer — nothing to mirror
            continue

        grp.setdefault("children", [])
        src_children = src.get("children") or []

        existing_ids = {c.get("id") for c in grp["children"]}
        clones: List[Dict[str, Any]] = []
        for c in src_children:
            cid = c.get("id")
            if not cid or cid in existing_ids:
                continue
            c2 = copy.deepcopy(c)
            c2["clone_meta"] = {
                "from_group": excellent_key,
                "reason": "EXCELLENT children mirrored for CRACKED",
            }
            clones.append(c2)

        if clones:
            grp["children"].extend(clones)


def apply_cracked_alias(groups: Dict[str, Dict[str, Any]]) -> None:
    """
    Optional: mark CRACKED groups as aliasing EXCELLENT for lookups (kept from previous behavior).
    """
    for key, grp in groups.items():
        parts = grp.get("parts") or {}
        if parts.get("condition") != "CRACKED":
            continue
        make = parts.get("make")
        model = parts.get("model")
        storage = parts.get("storage")
        if not (make and model and storage):
            continue
        excellent_key = _excellent_peer_key(make, model, storage)
        if excellent_key in groups:
            grp.setdefault("alias_of", excellent_key)


# ---------------------------
# Public entry
# ---------------------------

def build_groups(
    buyback_docs: List[Dict[str, Any]],
    sell_docs: List[Dict[str, Any]],
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    Build trade-in groups using buyback parents (trade-in SKUs) and sell children
    (marketplace listings). Output shape matches your repo usage:

      {
        "_id": "<KEY>",
        "key": "<KEY>",
        "parts": { make, model, storage, condition },
        "parents": [ ... buyback parents ... ],
        "children": [ ... sell children ... ],
        "counts": { parents, children, active_children },
        "synced_at": "<iso>",
        "build_meta": { "run_id": "<run_id>" }
      }

    Additionally:
      • Ensures a CRACKED peer group exists for every EXCELLENT group.
      • Mirrors EXCELLENT children into CRACKED groups.
      • Leaves parents untouched; they come from buyback data as-is.
    """
    groups: Dict[str, Dict[str, Any]] = {}

    # 1) Fold parents and children
    _fold_buyback_parents(groups, buyback_docs)
    _fold_sell_children(groups, sell_docs)

    # 2) Guarantee CRACKED peers exist and mirror EXCELLENT children into them
    ensure_cracked_groups_exist(groups)
    clone_children_for_cracked(groups)

    # 3) (Optional) alias cracked->excellent for lookup semantics
    apply_cracked_alias(groups)

    # 4) Finalize counts and stamps
    now_iso = dt.datetime.utcnow().isoformat()
    for g in groups.values():
        children = g.get("children") or []
        parents = g.get("parents") or []
        g["counts"] = {
            "parents": len(parents),
            "children": len(children),
            "active_children": sum(1 for c in children if (c.get("active") or (int(c.get("quantity") or 0) > 0))),
        }
        g["synced_at"] = now_iso
        g.setdefault("build_meta", {})["run_id"] = run_id

    # stable order not strictly necessary; returning list is fine
    return list(groups.values())
