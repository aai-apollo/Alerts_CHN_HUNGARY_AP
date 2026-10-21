"""
Genealogy / traceability decision-tree API.

Flow:
    1. Resolve BARCODES from MongoDB (the yield summary) for the selected
       GV code + date range + yield (last).
    2. Run the recursive genealogy CTE in Redshift for those barcodes to get
       the full lineage chains (sc_apollo_mom.t_genealogy_lot_detail).
    3. Build a TOP-DOWN decision tree: each barcode's finished tyre is a root
       node; its lineage rows become nested children by level. Barcodes that
       share the same lineage (same source lots) are grouped together.

If Redshift is not configured (REDSHIFT_* missing in .env) the endpoint falls
back to a small placeholder tree so the UI still renders.
"""

from collections import defaultdict

from flask import Blueprint, request, jsonify

import config_hungary

genealogy = Blueprint("genealogy", __name__)


# ── The recursive CTE. Parameterised by a list of barcodes (anchor set). ─────
# NOTE vs. the original query: the recursive step's stray
# "WHERE gc.barcode = '<other>'" filter is removed — it broke the walk by
# restricting recursion to an unrelated barcode. The walk now follows the lot
# linkage (g.targetlotid = gc.sourcelotid) for every barcode in the anchor set.
# Genealogy row columns, joined with the lot-extension table so each row also
# carries the TARGET lot's real barcode / roll position / equipment — matching
# the reference SQL (t_genealogy_lot_detail g LEFT JOIN t_apollo_lot_extension
# lotext ON g.TargetLotId = lotext.LotID). SourceLotId of a row links to the
# TargetLotId of the next (deeper) row, so the walk continues to the end.
_ROW_COLUMNS = (
    "g.id AS id, "
    "g.operationdate AS operationdate, "
    "g.sourcelotid AS sourcelotid, "
    "g.barcode AS sourcebarcode, "
    "g.targetlotid AS targetlotid, "
    "lotext.barcode AS targetbarcode, "
    "lotext.rollposition AS rollposition, "
    "g.mheid AS mheid, "
    "g.defid AS defid, "
    "g.materialtypecategory AS materialtypecategory, "
    "g.classid AS classid, "
    "g.description AS description, "
    "g.lotcreatedon AS lotcreatedon, "
    "g.lotstatuscd AS lotstatuscd, "
    "g.equipmentid AS equipmentid, "
    "lotext.equipmentid AS targetequipmentid, "
    "g.committo AS committo, "
    "g.recipeid AS recipeid, "
    "g.suppliername AS suppliername, "
    "g.supplierbatchnr AS supplierbatchnr, "
    "g.batchnumber AS batchnumber, "
    "g.weight AS weight"
)

_GENEALOGY_JOIN = (
    "FROM sc_apollo_mom.t_genealogy_lot_detail AS g "
    "LEFT JOIN sc_apollo_mom.t_apollo_lot_extension AS lotext "
    "ON g.targetlotid = lotext.lotid"
)

# Base-table-only columns (no lot-extension join) for the deep walk levels —
# past the seed we don't need the target barcode / roll position, so we skip
# the expensive join to the large extension table.
_ROW_COLUMNS_BASE = (
    "g.id AS id, "
    "g.operationdate AS operationdate, "
    "g.sourcelotid AS sourcelotid, "
    "g.barcode AS sourcebarcode, "
    "g.targetlotid AS targetlotid, "
    "g.mheid AS mheid, "
    "g.defid AS defid, "
    "g.materialtypecategory AS materialtypecategory, "
    "g.classid AS classid, "
    "g.description AS description, "
    "g.lotcreatedon AS lotcreatedon, "
    "g.lotstatuscd AS lotstatuscd, "
    "g.equipmentid AS equipmentid, "
    "g.committo AS committo, "
    "g.recipeid AS recipeid, "
    "g.suppliername AS suppliername, "
    "g.supplierbatchnr AS supplierbatchnr, "
    "g.batchnumber AS batchnumber, "
    "g.weight AS weight"
)


def _anchor_query(n):
    """
    Level-1 rows for the seed barcodes, matched on the TARGET barcode
    (lotext.barcode = t_zf_test_completed.barcode). These rows give the tyre's
    direct components (equipmentid + sourcelotid) which the per-equipment
    genealogy groups by. The barcode list is passed ONCE.
    """
    placeholders = ", ".join(["%s"] * n)
    # Match on TRIM(lotext.barcode): the lot-extension barcode can carry
    # leading/trailing whitespace (or padding) that the t_zf_test_completed
    # barcode does not. An untrimmed `lotext.barcode IN (...)` silently drops
    # such barcodes even though they exist in BOTH tables, so the tyre never
    # produces a level-1 row and is dropped from the whole pipeline.
    return (
        f"SELECT {_ROW_COLUMNS} "
        f"{_GENEALOGY_JOIN} "
        f"WHERE TRIM(lotext.barcode) IN ({placeholders}) "
        f"ORDER BY g.lotcreatedon DESC"
    )


def _to_list(val):
    if not val:
        return []
    return val if isinstance(val, list) else [val]


def resolve_barcodes(body):
    """
    Barcodes for the selected GV / date / from the Mongo summary
    (same collection & filter shape used by /api/dashboard).

    Returns a de-duplicated list of barcode strings (empty if none match).
    """
    # An explicit barcode list wins (e.g. drill-down from a single tyre).
    explicit = _to_list(body.get("barcodes") or body.get("barcode"))
    if explicit:
        return [str(b) for b in explicit if b]

    start = body.get("start_uf_date") or body.get("uf_date")
    end = body.get("end_uf_date") or start
    gv_codes = _to_list(body.get("gv_code"))

    last = body.get("last")
    try:
        last = int(last)
    except (TypeError, ValueError):
        last = 1

    yield_filter = (
        {"rerun": 0, "last": {"$in": [0, 1]}} if last == 0 else {"last": 1}
    )

    summary_col = config_hungary.data_merged_summary_historical_data

    mongo_filter = {**yield_filter}

    if gv_codes:
        mongo_filter["sku"] = {"$in": gv_codes}

    if start and end:
        import yield_calculator
        uf_start, _ = yield_calculator.day_range_str(start)
        _, uf_end = yield_calculator.day_range_str(end)
        mongo_filter["uf_time"] = {"$gte": uf_start, "$lte": uf_end}

    barcodes = [
        str(b)
        for b in summary_col.distinct("barcode", mongo_filter)
        if b
    ]

    return barcodes


import os

# Cap the number of barcodes traced per request — a GV/date selection can
# resolve to thousands of barcodes; tracing all of them at once would be huge.
MAX_BARCODES = int(os.getenv("GENEALOGY_MAX_BARCODES", "50"))

# Small in-process TTL cache for the (slow) Redshift cart lookup, keyed by the
# sorted barcode set. Repeated opens of the same GV/date return instantly.
import time as _time

_CART_CACHE = {}
_CART_TTL = int(os.getenv("GENEALOGY_CART_TTL", "300"))  # seconds


def _cart_cache_get(key):
    entry = _CART_CACHE.get(key)
    if not entry:
        return None
    value, expires = entry
    if _time.time() > expires:
        _CART_CACHE.pop(key, None)
        return None
    return value


def _cart_cache_set(key, value):
    _CART_CACHE[key] = (value, _time.time() + _CART_TTL)
    return value


def run_genealogy_query(barcodes):
    """
    Walk the lineage chain level by level (BFS), following the plant rule the
    reference queries use: a row's SOURCE lot is the next level's TARGET lot
    (target <- source). Level 1 anchors on the finished-tyre barcode; each
    subsequent level queries all rows whose targetlotid is in the previous
    level's sourcelotids, until the raw material (chain end) or the depth cap.

    A Redshift recursive CTE fans out explosively on this highly-connected
    graph and never returns, so the walk is done iteratively with a visited-lot
    guard plus depth / per-level / total caps. Results are cached per barcode
    set (see _cart_cache) so re-expanding is instant.

    Returns flat rows tagged with `level` (1 = the seed/finished tyre).
    """
    if not barcodes:
        return []

    barcodes = list(barcodes)[:MAX_BARCODES]

    # Cache the (slow) deep walk per barcode set so re-expanding is instant.
    cache_key = ("deep", tuple(sorted(str(b) for b in barcodes)))
    cached = _cart_cache_get(cache_key)
    if cached is not None:
        return cached

    # Bounds — a lineage branch can fan out heavily, so cap depth, the lots
    # expanded per level, and total lots visited to keep the walk fast.
    max_depth = int(os.getenv("GENEALOGY_DEEP_DEPTH", "6"))
    per_level_cap = int(os.getenv("GENEALOGY_PER_LEVEL_CAP", "30"))
    total_cap = int(os.getenv("GENEALOGY_TOTAL_CAP", "180"))

    # Level 1: seed rows for the finished-tyre barcodes (anchored on g.barcode).
    anchor = config_hungary.redshift_query(_anchor_query(len(barcodes)), barcodes)
    for r in anchor:
        r["level"] = 1
    all_rows = list(anchor)

    frontier = {
        str(r.get("sourcelotid"))
        for r in anchor
        if r.get("sourcelotid") is not None
    }
    visited = set(frontier)

    level = 1
    while frontier and level < max_depth and len(all_rows) < total_cap:
        level += 1
        # Only expand a bounded number of lots per level (deep fan-out is huge).
        lots = list(frontier)[:per_level_cap]
        next_frontier = set()

        placeholders = ", ".join(["%s"] * len(lots))
        # Deep levels don't need the lot-extension join (no target barcode
        # needed past the seed) — a plain lookup on the base table is far
        # cheaper than joining the large extension table each level.
        q = (
            f"SELECT {_ROW_COLUMNS_BASE} "
            f"FROM sc_apollo_mom.t_genealogy_lot_detail AS g "
            f"WHERE g.targetlotid IN ({placeholders})"
        )
        children = config_hungary.redshift_query(q, lots)
        for r in children:
            r["level"] = level
            all_rows.append(r)
            if len(all_rows) >= total_cap:
                break
            src = r.get("sourcelotid")
            if src is not None and str(src) not in visited:
                visited.add(str(src))
                next_frontier.add(str(src))

        frontier = next_frontier

    _cart_cache_set(cache_key, all_rows)
    return all_rows


def _json_safe(value):
    """Stringify dates/decimals so jsonify never chokes on DB types."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


# ── Equipment master (sapworkcenter -> machine name / division / type) ────────
# Loaded once from Mongo (populated by load_equipment_master.py) and cached, so
# lineage nodes can be labelled & classified by their real workcenter instead
# of guessing from the equipment-id prefix.
_EQUIP_MASTER = None

# division / machinetype (upper-cased, spaces removed) -> (stage_key, label).
_DIVISION_STAGE = {
    "MIXING": ("mixing", "Mixer"),
    "PCRCURING": ("machine_extruder", "Curing"),
    "PCRSTOCKPREPARATION": ("machine_extruder", "Component"),
    "TYREBUILDING": ("extruder", "Green Tyre / TBM"),
    "STC": ("machine_extruder", "Component"),
}


def _load_equip_master():
    """{sapworkcenter(upper) -> master doc}, cached. Empty if not populated."""
    global _EQUIP_MASTER
    if _EQUIP_MASTER is not None:
        return _EQUIP_MASTER
    master = {}
    try:
        for d in config_hungary.equipment_master.find(
            {}, {"_id": 0, "sapworkcenter": 1, "division": 1,
                 "workcentersubgroup": 1, "machinetype": 1, "workgroup": 1}
        ):
            key = str(d.get("sapworkcenter") or "").strip().upper()
            if key:
                master[key] = d
    except Exception:
        master = {}
    _EQUIP_MASTER = master
    return master


def equipment_info(equipmentid):
    """Master record for an equipment id (case-insensitive), or None."""
    if not equipmentid:
        return None
    return _load_equip_master().get(str(equipmentid).strip().upper())


def _stage_from_master(info):
    """(stage_key, label) from a master doc's division, or None."""
    if not info:
        return None
    div = "".join(str(info.get("division") or "").upper().split())
    return _DIVISION_STAGE.get(div)


def _classify_stage(row):
    """
    Human stage + node colour-key for a genealogy row.

    Preferred: the equipment master (sapworkcenter -> division) drives the
    stage when the equipmentid is known. Falls back to the plant heuristic:
      * a lot whose EQUIPMENT id contains 'MX' (e.g. 'MMXFB2') is a MIXER,
      * materialtypecategory 'RM' is the raw material (chain end),
      * 'FT' finished tyre, 'GTCT' green tyre (TBM), 'CompoundSFG' a
        semi-finished component (Bodyply/Tread/etc. — built on an extruder).
    Returns (stage_key, stage_label).
    """
    mtc = str(row.get("materialtypecategory") or "").strip()
    equip = str(row.get("equipmentid") or "").upper()
    tgt_equip = str(row.get("targetequipmentid") or "").upper()

    # 1) Equipment master wins when the equipment id (or target equip) is known.
    for eid in (equip, tgt_equip):
        stage = _stage_from_master(equipment_info(eid))
        if stage:
            # RM still terminates the chain even if an id happens to resolve.
            if mtc.upper() == "RM":
                return "raw_material", "Raw Material"
            return stage

    # 2) Heuristic fallback.
    # Mixer: equipment id contains "MX" (e.g. MMXFB2). Checked first so a mixer
    # lot is labelled as such regardless of its material category.
    if "MX" in equip or "MX" in tgt_equip:
        return "mixing", "Mixer"
    if mtc.upper() == "RM":
        return "raw_material", "Raw Material"
    if mtc == "FT":
        return "tbm", "Finished Tyre"
    if mtc == "GTCT":
        return "extruder", "Green Tyre / TBM"
    if mtc == "CompoundSFG":
        return "machine_extruder", "Component"
    return "source_order", (mtc or "Lot")


def _row_node(row):
    """One genealogy row -> a tree node."""
    node_id = _json_safe(row.get("id")) or _json_safe(row.get("sourcelotid"))
    stage_key, stage_label = _classify_stage(row)
    label = (
        row.get("description")
        or stage_label
        or row.get("targetbarcode")
        or row.get("sourcebarcode")
        or "Lot"
    )
    # Resolve the machine from the equipment master so the node can show the
    # real workcenter name (e.g. "Final Batch Mixer") alongside the material
    # description. targetequipmentid is preferred (the machine that produced
    # this lot); fall back to equipmentid.
    info = (equipment_info(row.get("targetequipmentid"))
            or equipment_info(row.get("equipmentid")))
    machine = ""
    machine_type = ""
    if info:
        machine = (info.get("workcentersubgroup")
                   or info.get("machinetype") or "").strip()
        machine_type = (info.get("machinetype") or "").strip()
    return {
        "id": f"{row.get('root_barcode')}-{node_id}-{row.get('level')}",
        "label": str(label),
        "level": row.get("level"),
        "stage": stage_key,
        "stageLabel": stage_label,
        "meta": {
            "machine": machine,
            "machineType": machine_type,
            "targetBarcode": _json_safe(row.get("targetbarcode")),
            "sourceBarcode": _json_safe(row.get("sourcebarcode")),
            "rollPosition": _json_safe(row.get("rollposition")),
            "sourceLotId": _json_safe(row.get("sourcelotid")),
            "targetLotId": _json_safe(row.get("targetlotid")),
            "classId": _json_safe(row.get("classid")),
            "mheId": _json_safe(row.get("mheid")),
            "equipmentId": _json_safe(row.get("equipmentid")),
            "targetEquipmentId": _json_safe(row.get("targetequipmentid")),
            "batchNumber": _json_safe(row.get("batchnumber")),
            "recipeId": _json_safe(row.get("recipeid")),
            "supplierName": _json_safe(row.get("suppliername")),
            "supplierBatch": _json_safe(row.get("supplierbatchnr")),
            "weight": _json_safe(row.get("weight")),
            "lotStatus": _json_safe(row.get("lotstatuscd")),
            "operationDate": _json_safe(row.get("operationdate")),
            "lotCreatedOn": _json_safe(row.get("lotcreatedon")),
        },
        "children": [],
    }


def build_trees_from_rows(rows, order_map=None):
    """
    Turn flat genealogy rows into TOP-DOWN trees — one tree per seed barcode
    (the finished tyre at level 1), children nested underneath.

    Linkage: a child row's targetlotid == its parent's sourcelotid. Level-1
    rows (one per seed barcode) are the roots. Barcodes whose lineage source
    lots are identical are grouped so the UI can collapse duplicates.
    `order_map` supplies the production order id per barcode.
    """
    # Index every row's node by that row's own sourcelotid, so a parent (which
    # has sourcelotid == child.targetlotid) can find its children.
    nodes_by_source = defaultdict(list)
    node_of_row = {}

    for row in rows:
        node = _row_node(row)
        node_of_row[id(row)] = (row, node)
        src = _json_safe(row.get("sourcelotid"))
        if src is not None:
            nodes_by_source[src].append(node)

    # Attach children to parents (parent.sourcelotid == child.targetlotid).
    # NOTE: level-1 rows are included here too. The anchor can return several
    # level-1 rows for one barcode (e.g. a Curing row AND its Green-Tyre row),
    # and those rows chain to each other (one row's targetlotid == another's
    # sourcelotid). Linking them prevents a barcode's lineage from being split
    # into several disconnected roots (which dropped the deep levels).
    seen_child = set()
    for row, node in node_of_row.values():
        target = _json_safe(row.get("targetlotid"))
        if target is None:
            continue
        parents = nodes_by_source.get(target)
        if parents:
            # Don't let a node become its own child (self-referential lots).
            parent = parents[0]
            if parent is node:
                continue
            parent["children"].append(node)
            seen_child.add(node["id"])

    trees = []
    signature_groups = defaultdict(list)

    # A seed row is a genuine tree ROOT only if it wasn't attached as a child
    # of another row above. This collapses the multiple level-1 rows of one
    # barcode into a single connected tree.
    seed_rows = [
        r for r in rows
        if (r.get("level") or 1) <= 1
        and node_of_row[id(r)][1]["id"] not in seen_child
    ]

    for row in seed_rows:
        _, root_node = node_of_row[id(row)]
        # The finished-tyre barcode is the TARGET barcode of the seed rows.
        barcode = _json_safe(row.get("targetbarcode")) or _json_safe(row.get("sourcebarcode"))
        order = (order_map or {}).get(str(barcode), {})
        # Attach the cartridge / material info (breaker, tread, bead, etc.) for
        # this finished tyre as extra children so the trace reaches the carts.
        cart_children = order.get("cartNodes") or []
        if cart_children:
            root_node.setdefault("children", []).extend(cart_children)
        trees.append({
            "barcode": barcode,
            "orderId": order.get("orderId"),
            "recipe": order.get("recipe"),
            "tree": root_node,
        })

    # Group barcodes by identical descendant-lot signatures.
    lots_by_barcode = defaultdict(set)
    for r in rows:
        bc = _json_safe(r.get("targetbarcode")) or _json_safe(r.get("sourcebarcode"))
        if bc and r.get("sourcelotid") is not None:
            lots_by_barcode[bc].add(_json_safe(r.get("sourcelotid")))
    for bc, lots in lots_by_barcode.items():
        signature_groups[tuple(sorted(lots))].append(bc)

    groups = [
        {"barcodes": bcs, "count": len(bcs)}
        for bcs in signature_groups.values()
    ]

    return trees, groups


# ── Mongo-based lineage (used when Redshift lineage isn't configured) ────────
# The historical summary already stores the per-tyre production chain
# (UF station -> curing press/mould -> TBM -> GT code), so we can build a
# REAL, per-barcode decision tree from it without Redshift. Only the raw
# material / mixing / extruder detail (which lives in Redshift) is absent.

def _fetch_barcode_lineage_docs(barcodes):
    """
    {barcode -> summary doc} with the lineage fields, for the given barcodes.
    Picks the last=1 (final) record per barcode.
    """
    if not barcodes:
        return {}

    summary_col = config_hungary.data_merged_summary_historical_data
    projection = {
        "_id": 0, "barcode": 1, "sku": 1, "gt_code": 1, "barcode_tbm": 1,
        "ptb_id": 1, "cur_id": 1, "cur_mouldid": 1, "uf_id": 1,
        "shift": 1, "uf_time": 1, "cur_time": 1, "ptb_time": 1,
    }
    docs = {}
    cursor = summary_col.find(
        {"barcode": {"$in": list(barcodes)}, "last": 1},
        projection,
    )
    for d in cursor:
        bc = str(d.get("barcode"))
        # First final record wins; a barcode has one finished-tyre row.
        if bc not in docs:
            docs[bc] = d
    return docs


def _process_node(nid, label, stage, stage_label, meta=None, children=None):
    return {
        "id": nid,
        "label": label,
        "stage": stage,
        "stageLabel": stage_label,
        "meta": {k: v for k, v in (meta or {}).items()
                 if v not in (None, "", "N/A")},
        "children": children if children is not None else [],
    }


def build_process_hierarchy(barcodes, gv_code, qr_status):
    """
    Build the process hierarchy WITHOUT showing barcodes:

        SKU (GV)
          -> Order (production order / gt_code)
             -> UFM (uf_id)
                -> Curing (cur_id + mould)
                   -> TBM (ptb_id)          [genealogy trace grafts here]

    Barcodes are grouped by their (order, uf_id, cur_id, ptb_id) signature so
    each distinct process path appears once. Each TBM leaf keeps a
    representative barcode (`traceBarcode`) — the Redshift equipment lineage is
    fetched for that barcode and grafted under the TBM node.

    Returns (root_node, tbm_leaves) where tbm_leaves is a list of the TBM nodes
    (each carrying `traceBarcode` in its meta) so the caller can attach traces.
    """
    lineage_docs = _fetch_barcode_lineage_docs(barcodes)
    sku = gv_code or "SKU"

    # Nodes are keyed by IDENTITY (not by path) so the SAME entity — e.g. one
    # TBM fed by several curing presses — becomes a SINGLE shared node with
    # multiple parents (a DAG), instead of one duplicate per path. We record
    # parent->child links as a set of edges; the same child node object is
    # reused wherever it appears.
    node_by_id = {}
    child_ids = defaultdict(set)   # parentId -> {childId, ...}
    tbm_leaves = []

    def _ensure(node_id, make):
        if node_id not in node_by_id:
            node_by_id[node_id] = make()
        return node_by_id[node_id]

    sku_id = f"sku-{sku}"
    node_by_id[sku_id] = _process_node(
        sku_id, sku, "raw_material", "SKU",
        {"gvCode": sku, "qrStatus": qr_status}, [])

    for bc in barcodes:
        bc_s = str(bc).strip()
        doc = lineage_docs.get(bc_s) or {}
        order = doc.get("gt_code") or "No Order"
        uf = doc.get("uf_id") or "—"
        cur = doc.get("cur_id") or "—"
        tbm = doc.get("ptb_id") or "—"

        order_id = f"order-{order}"
        uf_id = f"uf-{uf}"
        cur_id = f"cur-{cur}"
        tbm_id = f"tbm-{tbm}"

        _ensure(order_id, lambda o=order: _process_node(
            order_id, f"Order {o}", "source_order", "Order", {"order": o}, []))
        _ensure(uf_id, lambda u=uf: _process_node(
            uf_id, f"UFM {u}", "mixing", "UFM", {"ufStation": u}, []))
        _ensure(cur_id, lambda cu=cur, d=doc: _process_node(
            cur_id, f"Curing {cu}", "machine_extruder", "Curing",
            {"curingPress": cu, "mould": d.get("cur_mouldid")}, []))

        tbm_label = str(tbm)
        if not tbm_label.upper().startswith("TBM"):
            tbm_label = f"TBM {tbm_label}"
        tbm_node = _ensure(tbm_id, lambda t=tbm, l=tbm_label, b=bc_s, d=doc: _process_node(
            tbm_id, l, "extruder", "TBM / Green Tyre",
            {"tbm": t, "traceBarcode": b, "buildTime": d.get("ptb_time")}, []))

        child_ids[sku_id].add(order_id)
        child_ids[order_id].add(uf_id)
        child_ids[uf_id].add(cur_id)
        child_ids[cur_id].add(tbm_id)

    tbm_leaves = [n for nid, n in node_by_id.items()
                  if nid.startswith("tbm-")]

    # Materialise the DAG into the nested `children` structure the frontend
    # renders. A shared child appears under each of its parents by the same id;
    # the frontend dedupes nodes by id and keeps an edge per parent, so the
    # shared node is drawn ONCE with converging edges.
    def _build(node_id, seen_path):
        node = node_by_id[node_id]
        kids = [
            _build(cid, seen_path | {node_id})
            for cid in sorted(child_ids.get(node_id, ()))
            if cid not in seen_path            # guard against any cycle
        ]
        return {**node, "children": kids}

    root = _build(sku_id, set())
    return root, tbm_leaves


def _dedupe_nodes(nodes):
    """
    Merge sibling nodes that represent the SAME lot/machine so the trace shows
    one node per real thing instead of duplicates. Nodes are keyed by their
    source lot id (falling back to label+stage); the first wins and absorbs any
    children the duplicates carry.
    """
    seen = {}
    out = []
    for n in nodes or []:
        meta = n.get("meta") or {}
        key = (meta.get("sourceLotId")
               or f"{n.get('label')}|{n.get('stage')}")
        if key in seen:
            keep = seen[key]
            # Absorb children from the duplicate (whichever has more wins).
            dup_kids = n.get("children") or []
            if dup_kids and not (keep.get("children") or []):
                keep["children"] = dup_kids
            continue
        seen[key] = n
        out.append(n)
    # Recurse so the whole subtree is deduped.
    for n in out:
        n["children"] = _dedupe_nodes(n.get("children") or [])
    return out


def _trace_children_for_barcode(barcode, order_map=None):
    """
    The Redshift equipment-lineage nodes for a barcode, ready to graft UNDER a
    TBM node (SKU->...->TBM->trace). The TBM node already represents the green
    tyre, so we descend PAST the redundant finished-tyre / green-tyre (GTCT)
    wrapper and return the real component lineage
    (Component -> Mixer -> Raw Material), deduped so the same lot never appears
    twice.
    """
    try:
        rows = run_genealogy_query([str(barcode).strip()])
        built, _ = build_trees_from_rows(rows, order_map or {})
    except Exception:
        return []
    if not built:
        return []
    root = built[0].get("tree") or {}

    # Peel off the green-tyre / finished-tyre wrapper layers: collect the
    # children of every top node whose stage is the FT/green-tyre level, since
    # the TBM node already stands for the green tyre.
    wrapper_stages = {"tbm", "extruder"}  # Finished Tyre, Green Tyre / TBM
    components = []
    for child in root.get("children", []) or []:
        if child.get("stage") in wrapper_stages:
            components.extend(child.get("children") or [])
        else:
            components.append(child)

    return _dedupe_nodes(components)


@genealogy.route("/api/genealogy/tree", methods=["POST"])
def get_genealogy_tree():
    """
    Body: { start_uf_date, end_uf_date, gv_code, qr_status, last, tyre_grading }
          (or an explicit "barcode"/"barcodes" to trace directly).

    Returns a process hierarchy SKU -> Order -> UFM -> Curing -> TBM with the
    Redshift equipment lineage (Component -> Mixer -> Raw Material) grafted
    under each TBM node. Barcodes are used only to query; they are NOT shown.
    """
    body = request.get_json(silent=True) or {}

    # Resolve the real barcodes from Mongo first — this does NOT need Redshift.
    try:
        barcodes = resolve_barcodes(body)
    except Exception as e:
        return jsonify({
            "success": False, "hasData": False,
            "message": f"Failed to resolve barcodes: {e}",
        }), 500

    if not barcodes:
        return jsonify({
            "success": True, "hasData": False,
            "message": "No barcodes found for the selected filters.",
            "filtersApplied": body,
            "data": {"barcodeCount": 0, "groups": [], "trees": []},
        })

    # Cap the number of barcodes surfaced (a GV/date can resolve to thousands).
    total_barcodes = len(barcodes)
    barcodes = barcodes[:MAX_BARCODES]

    # ── DEEP mode: equipment lineage for ONE (representative) barcode ────────
    # Runs when a TBM node is expanded (deep=true + its traceBarcode). Returns
    # only the trace children (Component -> Mixer -> Raw Material) to graft
    # under that TBM node.
    deep = bool(body.get("deep")) and config_hungary.redshift_configured()
    if deep:
        one = _to_list(body.get("barcode") or body.get("barcodes")) or barcodes[:1]
        one = [str(b).strip() for b in one][:1]
        try:
            children = _trace_children_for_barcode(one[0]) if one else []
        except Exception as e:
            return jsonify({
                "success": False, "hasData": False,
                "message": f"Genealogy query failed: {e}",
                "data": {},
            }), 500
        return jsonify({
            "success": True, "hasData": bool(children), "isPlaceholder": False,
            "message": "Equipment lineage fetched.",
            "filtersApplied": body,
            "data": {"traceBarcode": one[0] if one else None,
                     "children": children, "deep": True},
        })

    # ── Default path ─────────────────────────────────────────────────────────
    # Build the process hierarchy SKU -> Order -> UFM -> Curing -> TBM from the
    # Mongo summary (no barcodes shown). Then graft the Redshift equipment
    # lineage (Component -> Mixer -> Raw Material) under each TBM node using a
    # representative barcode. The Redshift walk is expensive, so only the first
    # TBM leaf is traced up front; the rest load lazily (deep mode) on expand.
    redshift = config_hungary.redshift_configured()

    root, tbm_leaves = build_process_hierarchy(
        barcodes, body.get("gv_code"), body.get("qr_status")
    )

    if redshift and tbm_leaves:
        eager = int(os.getenv("GENEALOGY_EAGER_BARCODES", "1"))
        eager_ids = {leaf["id"] for leaf in tbm_leaves[:eager]}

        # Graft the equipment lineage onto the ACTUAL TBM node(s) inside `root`
        # (build_process_hierarchy returns copies, so we must walk root itself).
        def _graft(node):
            if node.get("id") in eager_ids and not (node.get("children") or []):
                trace_bc = (node.get("meta") or {}).get("traceBarcode")
                if trace_bc:
                    try:
                        node["children"] = _trace_children_for_barcode(trace_bc)
                    except Exception:
                        node["children"] = []
            for ch in node.get("children", []) or []:
                _graft(ch)

        _graft(root)

    return jsonify({
        "success": True,
        "hasData": bool(tbm_leaves),
        "isPlaceholder": not redshift,
        "message": (
            "Process hierarchy with Redshift equipment lineage."
            if redshift else
            "Process hierarchy from the yield summary (Redshift not configured)."
        ),
        "filtersApplied": body,
        "data": {
            "barcodeCount": total_barcodes,
            "tbmCount": len(tbm_leaves),
            "root": root,
            # Barcode->TBM node id map so the UI can trigger lazy trace loads.
            "supportsDeep": redshift,
        },
    })


# ============================================================================
# STANDALONE INGESTION: Redshift genealogy  ->  MongoDB
# ----------------------------------------------------------------------------
# Self-contained job that reads the genealogy / digital-thread data from
# Redshift and stores one document per finished-tyre barcode into MongoDB.
# It does NOT use config_hungary (which only exposes Mongo collections, not a
# Redshift connection) and does NOT touch the Flask Blueprint above — run it
# directly:  python data_genealogy.py
#
#   DB         : TUB_CIMCORP_VISUAL_merged_cleaned_data_historical_data
#   Collection : data_geneolgys
#   Mongo URI  : mongodb://localhost:27017/
#
# Join chain (shared barcode key):
#   sc_mhshistory.t_tbm_pcr_productiondata.vst_vhmc_mes_gt_barcode
#     = sc_mhshistory.t_curing_history.barcodeid
#     = sc_apollo_mom.t_zf_test_completed.barcode
# ============================================================================

import hashlib
import re
import datetime as _dt

# ── Redshift connection (explicit creds, as provided) ───────────────────────
REDSHIFT_HOST = "atl-dl-prd-01.cuvxusm4qify.ap-south-1.redshift.amazonaws.com"
REDSHIFT_DATABASE = "db_prd_plant_gyo"
REDSHIFT_PORT = 5439
REDSHIFT_USER = "usr_digital_twin_readonly"
REDSHIFT_PASSWORD = "UXY0%BSJa9dD#vP"

# ── MongoDB target ──────────────────────────────────────────────────────────
# MongoDB connection string is read from config_hungary.py.
GENEOLOGY_MONGO_URI = config_hungary.MONGO_URI
GENEOLOGY_DB_NAME = "TUB_CIMCORP_VISUAL_merged_cleaned_data_historical_data"
GENEOLOGY_COLLECTION_NAME = "data_geneolgys"
# Per-barcode tracking output (first-level components + machine arrays).
TRACKING_COLLECTION_NAME = "data_merged_tbm_curing_scrap_data"

# ── Date window to read Manual trigger START ──────────────────────────────────────────────────────
# HARD-CODED window (disabled). Kept for reference / manual back-fills — to run
# a fixed range again, re-enable these two lines (or pass the range on the CLI:
#   python data_leneage.py "2026-10-15 06:00:00" "2026-10-19 17:00:00").
# UFM_START = "2026-10-15 06:00:00"
# UFM_END = "2026-10-19 17:00:00"

# ── Date window to read Manual trigger END ──────────────────────────────────────────────────────


# ── Cronjob START ──────────────────────────────────────────────────────

# ROLLING 24-HOUR WINDOW (default).
# Every run reads the LAST 24 HOURS up to "now", so an hourly cronjob keeps
# back-tracking a moving 24h window (each run re-processes the previous day and
# picks up anything newly arrived; the per-barcode / per-(equipment, day)
# upserts make re-processing idempotent).
#
# NOTE: `now` here is the LOCAL time of the machine running the job (the
# MobaXterm / cron host). If that host is not on plant (Europe/Budapest) time,
# adjust the host clock or convert here — the window string is passed straight
# into the t_zf_test_completed."timestamp" filter.
_UFM_NOW = _dt.datetime.now()
UFM_END = _UFM_NOW.strftime("%Y-%m-%d %H:%M:%S")
UFM_START = (_UFM_NOW - _dt.timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")

# ── Cronjob END ──────────────────────────────────────────────────────

# Mongo insert batch size.
GENEOLOGY_INSERT_BATCH = int(os.getenv("GENEOLOGY_INSERT_BATCH", "10000"))

# MongoDB hard-limits a single BSON document to 16 MB. Keep a safety margin
# (command envelope, _id, equipment-level fields) so we never hit the limit.
# A document larger than this is SPLIT across multiple "part" docs instead of
# being truncated — the full lineage down to raw material is preserved.
MONGO_MAX_BSON_BYTES = 16 * 1024 * 1024
GENEOLOGY_DOC_MAX_BYTES = int(
    os.getenv("GENEOLOGY_DOC_MAX_BYTES", str(15 * 1024 * 1024))
)


def _bson_size(doc):
    """Encoded BSON size (bytes) of a single document."""
    from bson import BSON
    return len(BSON.encode(doc))


def _split_genealogy_doc(doc, max_bytes=GENEOLOGY_DOC_MAX_BYTES):
    """
    Return a list of documents that each encode to <= max_bytes, without
    dropping ANY data. Small docs are returned unchanged (single-element list).

    Oversized docs are split along the `sourcelotids` array into sequential
    "part" documents. Every part keeps the equipment-level summary fields and
    is tagged with part_index / part_count and a part-suffixed _id so re-runs
    replace cleanly. If a SINGLE sourcelotid entry is itself too large, its
    `linkage` list is further chunked across parts so no part exceeds the cap.
    The complete lineage (every node, full depth to raw material) is retained.
    """
    if _bson_size(doc) <= max_bytes:
        return [doc]

    base_id = doc["_id"]
    source_entries = doc.get("sourcelotids") or []

    # Fields shared by every part (everything except the heavy array).
    shell = {k: v for k, v in doc.items() if k not in ("_id", "sourcelotids")}

    # Approx overhead of a part shell (so we know how much room the array has).
    shell_probe = dict(shell)
    shell_probe["_id"] = f"{base_id}::part0000"
    shell_probe["sourcelotids"] = []
    shell_probe["part_index"] = 0
    shell_probe["part_count"] = 0
    shell_overhead = _bson_size(shell_probe)
    # Per-part budget for the sourcelotids array. Reserve an extra safety slack
    # on top of the shell overhead so BSON array/key framing added when the
    # entries are packed back together can never push a part over max_bytes.
    _SLACK = 64 * 1024
    budget = max_bytes - shell_overhead - _SLACK
    if budget < 1024:
        budget = max(1024, max_bytes // 2)
    # When a SINGLE sourcelotid entry has to be chunked, aim each chunk well
    # below `budget` (half), because that lone entry becomes an entire part on
    # its own and still needs room for the entry/meta/array framing.
    chunk_budget = max(1024, budget // 2)

    # First, make sure no single sourcelotid entry exceeds the per-part budget.
    # If one does, break its `linkage` list into chunks across several entries
    # (same sourcelotid, tagged with linkage_chunk / linkage_chunks).
    normalized_entries = []
    for entry in source_entries:
        if _bson_size(entry) <= budget:
            normalized_entries.append(entry)
            continue
        linkage = entry.get("linkage") or []
        meta = {k: v for k, v in entry.items() if k != "linkage"}
        meta_overhead = _bson_size({
            **meta, "linkage_chunk": 0, "linkage_chunks": 0, "linkage": []
        })
        chunk, chunk_bytes = [], meta_overhead
        chunks = []
        for node in linkage:
            nb = _bson_size(node)
            if chunk and chunk_bytes + nb > chunk_budget:
                chunks.append(chunk)
                chunk, chunk_bytes = [], meta_overhead
            chunk.append(node)
            chunk_bytes += nb
        if chunk:
            chunks.append(chunk)
        total_chunks = len(chunks) or 1
        for ci, c in enumerate(chunks or [[]]):
            normalized_entries.append({
                **meta,
                "linkage_chunk": ci,
                "linkage_chunks": total_chunks,
                "linkage": c,
            })

    # Now pack the (possibly chunked) entries greedily into parts. A chunked
    # entry is forced onto its own part so its size is bounded by chunk_budget.
    parts, current, current_bytes = [], [], 0
    for entry in normalized_entries:
        eb = _bson_size(entry)
        is_chunked = "linkage_chunk" in entry
        if current and (is_chunked or current_bytes + eb > budget):
            parts.append(current)
            current, current_bytes = [], 0
        current.append(entry)
        current_bytes += eb
        if is_chunked:
            parts.append(current)
            current, current_bytes = [], 0
    if current:
        parts.append(current)

    part_count = len(parts) or 1
    out = []
    for pi, entries in enumerate(parts or [[]]):
        part = dict(shell)
        part["_id"] = f"{base_id}::part{pi:04d}"
        part["part_index"] = pi
        part["part_count"] = part_count
        part["sourcelotids"] = entries
        out.append(part)
    return out


def _redshift_connect():
    """Open a psycopg2 connection to Redshift using the explicit creds."""
    import psycopg2
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        dbname=REDSHIFT_DATABASE,
        port=REDSHIFT_PORT,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )
    # Autocommit: each statement is its own transaction, so a failed query
    # (e.g. a missing table in one chunk) can't poison the connection and
    # abort every later query ("current transaction is aborted").
    conn.autocommit = True
    return conn


def _redshift_fetch(conn, sql, params=None):
    """Run a query and return a list of dict rows (column name -> value)."""
    from psycopg2.extras import RealDictCursor
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or [])
            return [dict(r) for r in cur.fetchall()]
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def _resolve_zf_anchor(conn, start=None, end=None, barcodes=None, skus=None):
    """
    Anchor the WHOLE pipeline on sc_apollo_mom.t_zf_test_completed (the UFM
    test record). The barcode set is driven ENTIRELY by this table: every
    barcode whose UFM test time (`timestamp`, exposed downstream as `uf_time`)
    falls in the window is returned, and all other tables (genealogy, TBM,
    curing, repair) are left-join enrichments onto this set.

    Returns {barcode -> {barcode, date_of_ufm, ufm_machine, sku}} taking ONLY
    these columns from t_zf_test_completed:
        barcode, timestamp AS date_of_ufm (-> uf_time), ufm AS ufm_machine, sku
    The LATEST test row per barcode wins (timestamp desc). NOTE: `timestamp` is
    the physical column; `uf_time` is its business name in the output docs.
    """
    where = []
    params = []

    # When an EXPLICIT barcode list is supplied, the barcode IS the intent —
    # do NOT also constrain by the time window (a barcode whose UFM test time
    # is outside the default window would otherwise be silently dropped). The
    # window only applies to a window-driven (no explicit barcodes) run.
    explicit_barcodes = [str(b).strip() for b in (barcodes or []) if b]

    if not explicit_barcodes:
        if start:
            where.append('"timestamp" >= %s')
            params.append(start)
        if end:
            where.append('"timestamp" <= %s')
            params.append(end)
    if explicit_barcodes:
        # Match on TRIM(barcode) so stored whitespace/padding never hides a
        # barcode that otherwise exists in t_zf_test_completed.
        where.append("TRIM(barcode) IN (" + ", ".join(["%s"] * len(explicit_barcodes)) + ")")
        params.extend(explicit_barcodes)
    if skus:
        sk = [str(s).strip() for s in skus if s]
        if sk:
            where.append("sku IN (" + ", ".join(["%s"] * len(sk)) + ")")
            params.extend(sk)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = (
        "SELECT barcode, date_of_ufm, ufm_machine, sku FROM ( "
        "  SELECT TRIM(barcode) AS barcode, "
        '         "timestamp" AS date_of_ufm, '
        "         ufm AS ufm_machine, "
        "         sku, "
        "         ROW_NUMBER() OVER ( "
        "             PARTITION BY TRIM(barcode) "
        '             ORDER BY "timestamp" DESC '
        "         ) AS rn "
        "  FROM sc_apollo_mom.t_zf_test_completed "
        f"  {where_sql} "
        ") z WHERE rn = 1 "
    )

    anchor = {}
    for r in _redshift_fetch(conn, sql, params):
        bc = str(r.get("barcode") or "").strip()
        if not bc:
            continue
        anchor[bc] = {
            "barcode": bc,
            "date_of_ufm": _json_safe(r.get("date_of_ufm")),
            "ufm_machine": _json_safe(r.get("ufm_machine")),
            "sku": _json_safe(r.get("sku")),
        }
    return anchor


def _fetch_production_header_standalone(conn, barcodes):
    """
    {barcode -> {gt_code, tbm_name, order_number, ptb_id, ptb_time, <spot
    positions>}} from sc_mhshistory.t_tbm_pcr_productiondata (joined on
    vst_vhmc_mes_gt_barcode).
    Columns read from the TBM table:
        equipmentid                   -> tbm_name AND ptb_id
        producedon                    -> ptb_time
        vst_vhmc_mes_gt_code          -> gt_code
        vst_vhmc_mes_production_order -> order_number
        vrl_vhmc_mes_br1_spot_position, vrl_vhmc_mes_br2_spot_position,
        vrl_vhmc_mes_cwap_spot_pos,    vrl_vhmc_mes_td_spot_position,
        vrl_vhmc_mes_pa_spot_position, vrl_vhmc_mes_barcode_spot_pos,
        vrl_vhmc_mes_bp1_spot_pos,     vrl_vhmc_mes_bp2_spot_pos
    The materials (breaker, sidewall, etc.) are NOT taken from the TBM cart
    columns — they come from mheid in t_genealogy_lot_detail.
    """
    if not barcodes:
        return {}

    mapping = {}
    chunk = 500
    for i in range(0, len(barcodes), chunk):
        part = barcodes[i:i + chunk]
        placeholders = ", ".join(["%s"] * len(part))
        sql = (
            "WITH prod AS ( "
            "  SELECT TRIM(vst_vhmc_mes_gt_barcode) AS barcode, "
            "         equipmentid AS tbm_name, "
            "         equipmentid AS ptb_id, "
            "         producedon AS ptb_time, "
            "         vst_vhmc_mes_gt_code AS gt_code, "
            "         vst_vhmc_mes_production_order AS order_number, "
            "         vrl_vhmc_mes_br1_spot_position, "
            "         vrl_vhmc_mes_br2_spot_position, "
            "         vrl_vhmc_mes_cwap_spot_pos, "
            "         vrl_vhmc_mes_td_spot_position, "
            "         vrl_vhmc_mes_pa_spot_position, "
            "         vrl_vhmc_mes_barcode_spot_pos, "
            "         vrl_vhmc_mes_bp1_spot_pos, "
            "         vrl_vhmc_mes_bp2_spot_pos, "
            "         ROW_NUMBER() OVER ( "
            "             PARTITION BY vst_vhmc_mes_gt_barcode "
            "             ORDER BY producedon DESC "
            "         ) AS rn "
            "  FROM sc_mhshistory.t_tbm_pcr_productiondata "
            f"  WHERE TRIM(vst_vhmc_mes_gt_barcode) IN ({placeholders}) "
            ") "
            "SELECT barcode, tbm_name, ptb_id, ptb_time, gt_code, order_number, "
            "       vrl_vhmc_mes_br1_spot_position, vrl_vhmc_mes_br2_spot_position, "
            "       vrl_vhmc_mes_cwap_spot_pos, vrl_vhmc_mes_td_spot_position, "
            "       vrl_vhmc_mes_pa_spot_position, vrl_vhmc_mes_barcode_spot_pos, "
            "       vrl_vhmc_mes_bp1_spot_pos, vrl_vhmc_mes_bp2_spot_pos "
            "FROM prod WHERE rn = 1"
        )
        try:
            for r in _redshift_fetch(conn, sql, part):
                bc = str(r.get("barcode") or "").strip()
                if not bc:
                    continue
                mapping[bc] = {
                    "tbm_name": _json_safe(r.get("tbm_name")),
                    "ptb_id": _json_safe(r.get("ptb_id")),
                    "ptb_time": _json_safe(r.get("ptb_time")),
                    "gt_code": _json_safe(r.get("gt_code")),
                    "order_number": _json_safe(r.get("order_number")),
                    "vrl_vhmc_mes_br1_spot_position": _json_safe(r.get("vrl_vhmc_mes_br1_spot_position")),
                    "vrl_vhmc_mes_br2_spot_position": _json_safe(r.get("vrl_vhmc_mes_br2_spot_position")),
                    "vrl_vhmc_mes_cwap_spot_pos": _json_safe(r.get("vrl_vhmc_mes_cwap_spot_pos")),
                    "vrl_vhmc_mes_td_spot_position": _json_safe(r.get("vrl_vhmc_mes_td_spot_position")),
                    "vrl_vhmc_mes_pa_spot_position": _json_safe(r.get("vrl_vhmc_mes_pa_spot_position")),
                    "vrl_vhmc_mes_barcode_spot_pos": _json_safe(r.get("vrl_vhmc_mes_barcode_spot_pos")),
                    "vrl_vhmc_mes_bp1_spot_pos": _json_safe(r.get("vrl_vhmc_mes_bp1_spot_pos")),
                    "vrl_vhmc_mes_bp2_spot_pos": _json_safe(r.get("vrl_vhmc_mes_bp2_spot_pos")),
                }
        except Exception as e:
            print(f"  [warn] Production-header lookup failed for a chunk: {e}")

    return mapping


def _fetch_curing_standalone(conn, barcodes):
    """
    {barcode -> {curing_press, curing_time, cur_mouldid}} from
    sc_mhshistory.t_curing_history (joined barcodeid -> barcode). Only:
        curingpressid   -> curing_press (cur_id)
        curingtimestamp -> curing_time  (cur_time)
        curingmouldid   -> cur_mouldid
    """
    if not barcodes:
        return {}

    mapping = {}
    chunk = 500
    for i in range(0, len(barcodes), chunk):
        part = barcodes[i:i + chunk]
        placeholders = ", ".join(["%s"] * len(part))
        sql = (
            "WITH cur AS ( "
            "  SELECT TRIM(barcodeid) AS barcode, "
            "         curingpressid AS curing_press, "
            "         curingtimestamp AS curing_time, "
            "         curingmouldid AS cur_mouldid, "
            "         ROW_NUMBER() OVER ( "
            "             PARTITION BY TRIM(barcodeid) "
            "             ORDER BY curingtimestamp DESC "
            "         ) AS rn "
            "  FROM sc_mhshistory.t_curing_history "
            f"  WHERE TRIM(barcodeid) IN ({placeholders}) "
            ") "
            "SELECT barcode, curing_press, curing_time, cur_mouldid FROM cur WHERE rn = 1"
        )
        try:
            for r in _redshift_fetch(conn, sql, part):
                bc = str(r.get("barcode") or "").strip()
                if not bc:
                    continue
                mapping[bc] = {
                    "curing_press": _json_safe(r.get("curing_press")),
                    "curing_time": _json_safe(r.get("curing_time")),
                    "cur_mouldid": _json_safe(r.get("cur_mouldid")),
                }
        except Exception as e:
            print(f"  [warn] Curing lookup failed for a chunk: {e}")

    return mapping


# ── Mono-ply digital-thread parameter ───────────────────────────────────────
# From sc_mhshistory.t_pcr_tbm_productparameterchanges where parametername
# LIKE 'Mono%'. engunit 1/3 -> Monoply, 2 -> Dual ply. Joined to the
# production table on materialid = recipename (GT code) + equipmentid.
def _ply_type_from_engunit(engunit):
    if engunit is None:
        return None
    s = str(engunit).strip()
    if s in ("1", "3"):
        return "Monoply"
    if s == "2":
        return "Dual ply"
    return None


def _fetch_mono_ply_standalone(conn, barcodes):
    """{barcode -> {mono_parametername, mono_engunit, ply_type}}."""
    if not barcodes:
        return {}

    mapping = {}
    chunk = 500
    for i in range(0, len(barcodes), chunk):
        part = barcodes[i:i + chunk]
        placeholders = ", ".join(["%s"] * len(part))
        sql = (
            "WITH mono AS ( "
            "  SELECT TRIM(p.vst_vhmc_mes_gt_barcode) AS barcode, "
            "         tptp.parametername, tptp.engunit, "
            "         ROW_NUMBER() OVER ( "
            "             PARTITION BY TRIM(p.vst_vhmc_mes_gt_barcode) "
            "             ORDER BY tptp.updatedon DESC, p.producedon DESC "
            "         ) AS rn "
            "  FROM sc_mhshistory.t_tbm_pcr_productiondata AS p "
            "  JOIN sc_mhshistory.t_pcr_tbm_productparameterchanges AS tptp "
            "    ON tptp.materialid = p.recipename "
            "   AND tptp.equipmentid = p.equipmentid "
            "  WHERE tptp.parametername LIKE 'Mono%%' "
            f"    AND TRIM(p.vst_vhmc_mes_gt_barcode) IN ({placeholders}) "
            ") "
            "SELECT barcode, parametername, engunit FROM mono WHERE rn = 1"
        )
        try:
            for r in _redshift_fetch(conn, sql, part):
                bc = str(r.get("barcode") or "").strip()
                if not bc:
                    continue
                engunit = r.get("engunit")
                mapping[bc] = {
                    "mono_parametername": _json_safe(r.get("parametername")),
                    "mono_engunit": _json_safe(engunit),
                    "ply_type": _ply_type_from_engunit(engunit),
                }
        except Exception as e:
            print(f"  [warn] Mono-ply lookup failed for a chunk: {e}")

    return mapping


# ── Defect lookup (mv_tire -> stock_unit_repair) ────────────────────────────
# A finished tyre is counted as a DEFECT when it carries a repair comment:
#   sc_wms_apollo_pcr.mv_tire.tire_barcode  = t_zf_test_completed.barcode
#   sc_wms_apollo_pcr.mv_tire.stt_id        = stock_unit_repair.sur_stt_id
# A tyre is a defect if its sur_repair_comment is present (non-empty).
def _fetch_defect_map(conn, barcodes):
    """
    {barcode -> bool is_defect}. True when the tyre has a non-empty
    sur_repair_comment in stock_unit_repair (joined via mv_tire.stt_id).
    Barcodes with no repair row are simply absent (treated as non-defect).
    """
    if not barcodes:
        return {}

    mapping = {}
    chunk = 500
    for i in range(0, len(barcodes), chunk):
        part = [str(b).strip() for b in barcodes[i:i + chunk] if b]
        if not part:
            continue
        placeholders = ", ".join(["%s"] * len(part))
        # One row per barcode. mv_tire can hold DUPLICATE rows for the same
        # barcode, so keep only the LATEST mv_tire record per barcode
        # (last_change_time DESC) before joining to stock_unit_repair. The
        # defect flag then reflects that latest tyre record's repair comment.
        sql = (
            "SELECT mv.barcode AS barcode, "
            "       MAX( "
            "           CASE WHEN sur.sur_repair_comment IS NOT NULL "
            "                 AND TRIM(sur.sur_repair_comment) <> '' "
            "                THEN 1 ELSE 0 END "
            "       ) AS is_defect "
            "FROM ( "
            "    SELECT TRIM(tire_barcode) AS barcode, "
            "           stt_id, "
            "           ROW_NUMBER() OVER ( "
            "               PARTITION BY TRIM(tire_barcode) "
            "               ORDER BY last_change_time DESC "
            "           ) AS rn "
            "    FROM sc_wms_apollo_pcr.mv_tire "
            f"    WHERE TRIM(tire_barcode) IN ({placeholders}) "
            ") mv "
            "LEFT JOIN sc_wms_apollo_pcr.stock_unit_repair AS sur "
            "  ON mv.stt_id = sur.sur_stt_id "
            "WHERE mv.rn = 1 "
            "GROUP BY mv.barcode"
        )
        try:
            for r in _redshift_fetch(conn, sql, part):
                bc = str(r.get("barcode") or "").strip()
                if not bc:
                    continue
                mapping[bc] = bool(r.get("is_defect"))
        except Exception as e:
            print(f"  [warn] Defect lookup failed for a chunk: {e}")

    return mapping


# ── Repair detail lookup (mv_tire -> stock_unit_repair) ─────────────────────
# The actual repair columns for each tyre, mapped into the tracking doc:
#   stock_unit_repair.sur_repair_time    -> sur_repair_time
#   stock_unit_repair.sur_repair_code    -> re_code
#   stock_unit_repair.sur_repair_comment -> re_comment
# Joined via mv_tire.stt_id = stock_unit_repair.sur_stt_id, using the LATEST
# mv_tire row per barcode (last_change_time DESC). Barcodes with no repair row
# are simply absent (the tracking doc keeps those fields null).
def _fetch_repair_map(conn, barcodes):
    """
    {barcode -> {sur_repair_time, re_code, re_comment}} from
    sc_wms_apollo_pcr.stock_unit_repair (joined via mv_tire.stt_id). The latest
    mv_tire record per barcode wins; its linked repair row supplies the values.
    """
    if not barcodes:
        return {}

    mapping = {}
    chunk = 500
    for i in range(0, len(barcodes), chunk):
        part = [str(b).strip() for b in barcodes[i:i + chunk] if b]
        if not part:
            continue
        placeholders = ", ".join(["%s"] * len(part))
        # One row per barcode: keep only the LATEST mv_tire record
        # (last_change_time DESC) before joining to stock_unit_repair.
        sql = (
            "SELECT mv.barcode AS barcode, "
            "       sur.sur_repair_time AS sur_repair_time, "
            "       sur.sur_repair_code AS re_code, "
            "       sur.sur_repair_comment AS re_comment "
            "FROM ( "
            "    SELECT TRIM(tire_barcode) AS barcode, "
            "           stt_id, "
            "           ROW_NUMBER() OVER ( "
            "               PARTITION BY TRIM(tire_barcode) "
            "               ORDER BY last_change_time DESC "
            "           ) AS rn "
            "    FROM sc_wms_apollo_pcr.mv_tire "
            f"    WHERE TRIM(tire_barcode) IN ({placeholders}) "
            ") mv "
            "LEFT JOIN sc_wms_apollo_pcr.stock_unit_repair AS sur "
            "  ON mv.stt_id = sur.sur_stt_id "
            "WHERE mv.rn = 1"
        )
        try:
            for r in _redshift_fetch(conn, sql, part):
                bc = str(r.get("barcode") or "").strip()
                if not bc:
                    continue
                mapping[bc] = {
                    "sur_repair_time": _json_safe(r.get("sur_repair_time")),
                    "re_code": _json_safe(r.get("re_code")),
                    "re_comment": _json_safe(r.get("re_comment")),
                }
        except Exception as e:
            print(f"  [warn] Repair-detail lookup failed for a chunk: {e}")

    return mapping


def _fetch_last_change_time(conn, barcodes):
    """
    {barcode -> last_change_time (str)} — the LATEST mv_tire record per barcode
    (ORDER BY last_change_time DESC). Used as each tyre's OCCURRENCE time to
    drive the rerun / last flags in the tracking collection. Barcodes with no
    mv_tire row are simply absent.
    """
    if not barcodes:
        return {}

    mapping = {}
    chunk = 500
    for i in range(0, len(barcodes), chunk):
        part = [str(b).strip() for b in barcodes[i:i + chunk] if b]
        if not part:
            continue
        placeholders = ", ".join(["%s"] * len(part))
        # Keep only the latest mv_tire row per barcode (handles duplicates).
        sql = (
            "SELECT barcode, last_change_time FROM ( "
            "    SELECT TRIM(tire_barcode) AS barcode, "
            "           last_change_time, "
            "           ROW_NUMBER() OVER ( "
            "               PARTITION BY TRIM(tire_barcode) "
            "               ORDER BY last_change_time DESC "
            "           ) AS rn "
            "    FROM sc_wms_apollo_pcr.mv_tire "
            f"    WHERE TRIM(tire_barcode) IN ({placeholders}) "
            ") mv "
            "WHERE mv.rn = 1"
        )
        try:
            for r in _redshift_fetch(conn, sql, part):
                bc = str(r.get("barcode") or "").strip()
                if not bc:
                    continue
                mapping[bc] = _json_safe(r.get("last_change_time"))
        except Exception as e:
            print(f"  [warn] last_change_time lookup failed for a chunk: {e}")

    return mapping


def _fetch_genealogy_rows_standalone(conn, barcodes):
    """
    FULL lineage walk per finished-tyre barcode: follow every row's
    sourcelotid into the next level's targetlotid (target <- source),
    repeatedly, until the chain reaches the end level (no more source lots) or
    a safety cap is hit.

        level 1 : anchored on g.barcode (the finished tyre), enriched via the
                  lot-extension join (target barcode / roll position).
        level n : rows whose g.targetlotid IN (level n-1's sourcelotids).

    Each row is stamped with:
        level          -> depth (1 = seed/finished tyre)
        _root_barcode  -> the finished-tyre barcode this lineage belongs to,
                          so the ingestion can group the whole chain per tyre.

    The backward walk mirrors run_genealogy_query (the Blueprint), done
    iteratively with a visited-lot guard so a highly-connected graph can't
    loop or run away.
    """
    if not barcodes:
        return []

    # Depth guard (env-overridable). A branch can fan out heavily.
    max_depth = int(os.getenv("GENEALOGY_DEEP_DEPTH", "25"))
    # Big IN-lists keep the number of Redshift round-trips tiny. Redshift
    # handles very large IN lists fine; 10k keeps each statement reasonable.
    in_chunk = int(os.getenv("GENEALOGY_IN_CHUNK", "10000"))

    # ── Level 1: match t_zf_test_completed.barcode == genealogy TARGETBARCODE
    # (lotext.barcode). These rows carry equipmentid + sourcelotid for the
    # tyre. Each level-1 row is tagged with its matched tyre barcode ONLY so
    # per-equipment / per-sourcelotid tyre & defect COUNTS can be computed —
    # the deeper chain is NOT tracked per barcode. ──────────────────────────
    requested = {str(b).strip() for b in barcodes if b}
    out = []                          # level-1 rows (tagged) + deep rows (once)
    frontier = set()                  # sourcelotids to expand for the linkage
    for i in range(0, len(barcodes), in_chunk):
        part = [str(b).strip() for b in barcodes[i:i + in_chunk] if b]
        if not part:
            continue
        try:
            rows = _redshift_fetch(conn, _anchor_query(len(part)), part)
        except Exception as e:
            print(f"  [warn] Lineage level-1 lookup failed for a chunk: {e}")
            continue
        for r in rows:
            tb = str(r.get("targetbarcode") or "").strip()
            if tb not in requested:
                continue
            row = dict(r)
            row["level"] = 1
            row["_root_barcode"] = tb      # matched tyre barcode (counts only)
            out.append(row)
            src = row.get("sourcelotid")
            if src is not None:
                frontier.add(str(src))

    # ── ONE GLOBAL BFS over the lot graph (NOT per barcode). The whole
    # reachable sub-graph is pulled in ~max_depth bulk queries and each deep
    # row is stored exactly ONCE (no per-barcode duplication). These deep rows
    # provide the sourcelotid -> targetlotid linkage down to the raw material;
    # they are barcode-independent, so they carry no _root_barcode. ─────────
    fetched = set()
    level = 1
    while frontier and level < max_depth:
        level += 1
        lot_ids = [lid for lid in frontier if lid not in fetched]
        if not lot_ids:
            break
        next_frontier = set()
        for k in range(0, len(lot_ids), in_chunk):
            batch = lot_ids[k:k + in_chunk]
            placeholders = ", ".join(["%s"] * len(batch))
            q = (
                f"SELECT {_ROW_COLUMNS_BASE} "
                f"FROM sc_apollo_mom.t_genealogy_lot_detail AS g "
                f"WHERE g.targetlotid IN ({placeholders})"
            )
            try:
                rows = _redshift_fetch(conn, q, batch)
            except Exception as e:
                print(f"  [warn] Lineage BFS level-{level} failed for a chunk: {e}")
                rows = []
            for r in rows:
                row = dict(r)
                row["level"] = level
                out.append(row)               # stored ONCE (no per-barcode copy)
                src = r.get("sourcelotid")
                if src is not None and str(src) not in fetched:
                    next_frontier.add(str(src))
            for lid in batch:
                fetched.add(lid)
        frontier = next_frontier

    print(f"  Lineage rows: {len(out)} "
          f"[level-1 tagged + deep graph; lots fetched: {len(fetched)}]")
    return out


# Scrap fields that are OWNED by the historical scrap pipeline, not by this
# lineage job. When this job creates a NEW tire_barcode doc they are seeded as
# null; when the doc already exists they are LEFT UNTOUCHED (see the write
# loop's $setOnInsert) so an existing scrap value is never clobbered.
# NOTE: sur_repair_time / re_code / re_comment are NOT here — this job now
# fetches them from stock_unit_repair and refreshes them every run.
_TRACKING_NULL_DEFECT_FIELDS = (
    "condition_code", "description", "scrap_category", "qr_status",
)

# TBM spot-position columns copied verbatim from t_tbm_pcr_productiondata.
_TBM_SPOT_POSITION_FIELDS = (
    "vrl_vhmc_mes_br1_spot_position", "vrl_vhmc_mes_br2_spot_position",
    "vrl_vhmc_mes_cwap_spot_pos", "vrl_vhmc_mes_td_spot_position",
    "vrl_vhmc_mes_pa_spot_position", "vrl_vhmc_mes_barcode_spot_pos",
    "vrl_vhmc_mes_bp1_spot_pos", "vrl_vhmc_mes_bp2_spot_pos",
)


def _tracking_doc(barcode, lineage_rows=None, equip_map=None,
                  last_change_time=None, uf_time=None, uf_id=None,
                  header=None, curing=None, repair=None, sku=None):
    """
    ONE document PER tire_barcode for the `data_merged_tbm_curing_scrap_data`
    collection.

    Stores the barcode's FIRST-LEVEL genealogy components (no chain), the
    occurrence time (`last_change_time`), the UFM test time (`uf_time`,
    = t_zf_test_completed.timestamp), the UFM machine (`uf_id`, =
    t_zf_test_completed.ufm) and the `sku` (= t_zf_test_completed.sku). The
    `rerun` / `last` flags are filled in by the write loop (which knows about
    the barcode's other occurrences).

    TBM header fields (from t_tbm_pcr_productiondata): `ptb_id`
    (= equipmentid), `ptb_time` (= producedon) and the eight spot-position
    columns. Curing fields (from t_curing_history): `cur_id`
    (= curingpressid), `cur_time` (= curingtimestamp) and `cur_mouldid`
    (= curingmouldid).

    Repair fields (from stock_unit_repair, joined via mv_tire.stt_id):
    `sur_repair_time` (= sur_repair_time), `re_code` (= sur_repair_code) and
    `re_comment` (= sur_repair_comment). Absent when the tyre has no repair row.

    The remaining scrap fields (condition_code, description, scrap_category,
    qr_status) are seeded null — the write loop only applies them on INSERT so
    existing scrap data is kept.
    """
    header = header or {}
    curing = curing or {}
    repair = repair or {}

    components = []
    for r in sorted(lineage_rows or [], key=lambda x: x.get("level") or 1):
        if (r.get("level") or 1) != 1:
            continue
        eq1 = str(r.get("equipmentid") or "").strip()
        g = sg = mt = ms = None
        if equip_map and eq1:
            info1 = equip_map.get(eq1) or equip_map.get(eq1.upper())
            if info1:
                g = info1.get("workcentergroup")
                sg = info1.get("workcentersubgroup")
                mt = info1.get("machinetype")
                ms = info1.get("machinesupplier")
        components.append({
            "sourcelotid": _json_safe(r.get("sourcelotid")),
            "targetlotid": _json_safe(r.get("targetlotid")),
            "equipmentid": _json_safe(r.get("equipmentid")),
            "mheid": _json_safe(r.get("mheid")),
            "defid": _json_safe(r.get("defid")),
            "workcentergroup": g,
            "workcentersubgroup": sg,
            "machinetype": mt,
            "machinesupplier": ms,
        })

    lct = _json_safe(last_change_time)
    doc = {
        "_id": barcode,                   # keyed per tire_barcode (upsert key)
        "tire_barcode": barcode,
        "sku": _json_safe(sku),           # t_zf_test_completed.sku
        "uf_id": _json_safe(uf_id),       # t_zf_test_completed.ufm
        "uf_time": _json_safe(uf_time),   # t_zf_test_completed.timestamp
        "last_change_time": lct,
        "rerun": 0,                 # set by the write loop
        "last": 1,                  # set by the write loop
        "components": components,
        # TBM header (t_tbm_pcr_productiondata)
        "ptb_id": header.get("ptb_id"),
        "ptb_time": header.get("ptb_time"),
        # Curing (t_curing_history)
        "cur_id": curing.get("curing_press"),
        "cur_time": curing.get("curing_time"),
        "cur_mouldid": curing.get("cur_mouldid"),
        # Repair (stock_unit_repair)
        "sur_repair_time": repair.get("sur_repair_time"),
        "re_code": repair.get("re_code"),
        "re_comment": repair.get("re_comment"),
        "ingested_at": _dt.datetime.utcnow().isoformat(),
    }
    for col in _TBM_SPOT_POSITION_FIELDS:
        doc[col] = header.get(col)
    return doc


def _parse_dt(value):
    """Parse a 'YYYY-MM-DD HH:MM:SS' string (or datetime) into a datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, _dt.datetime):
        return value
    s = str(value)
    # Mongo extended-json / ISO 'T' separator support.
    if isinstance(value, dict) and "$date" in value:
        s = str(value["$date"])
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%d"):
        try:
            return _dt.datetime.strptime(s[:len(fmt) + 6], fmt)
        except Exception:
            continue
    try:
        return _dt.datetime.fromisoformat(s.replace("Z", "").strip())
    except Exception:
        return None


def _split_day_windows(start, end):
    """
    Split an overall [start, end) range into consecutive 24h "production day"
    windows that each run from one 06:00 to the next 06:00.

    The window boundaries step by exactly 24h starting at `start`:
        13-10 06:00 .. 15-10 06:00  ->  [13-10 06:00, 14-10 06:00),
                                         [14-10 06:00, 15-10 06:00)
    A sub-day run (e.g. one hour) still yields ONE full window whose end is the
    next-morning 06:00 (start + 24h):
        16-10 06:00 .. 16-10 07:00  ->  [16-10 06:00, 17-10 06:00)

    Returns a list of (win_start_str, win_end_str) in 'YYYY-MM-DD HH:MM:SS'.
    Falls back to a single (start, end) window if the dates can't be parsed.
    """
    sdt = _parse_dt(start)
    edt = _parse_dt(end)
    if sdt is None or edt is None:
        return [(str(start), str(end))]

    fmt = "%Y-%m-%d %H:%M:%S"
    windows = []
    cur = sdt
    # If the range is sub-day (end within 24h of start) OR exactly one day,
    # this produces exactly one window ending at start + 24h.
    while cur < edt:
        nxt = cur + _dt.timedelta(days=1)
        windows.append((cur.strftime(fmt), nxt.strftime(fmt)))
        cur = nxt
    if not windows:
        # end <= start (or sub-day) -> single next-morning window.
        nxt = sdt + _dt.timedelta(days=1)
        windows.append((sdt.strftime(fmt), nxt.strftime(fmt)))
    return windows


def _window_for_time(ts, windows):
    """Return the (start, end) window a timestamp falls in, or None."""
    tdt = _parse_dt(ts)
    if tdt is None:
        return None
    for (ws, we) in windows:
        wsd, wed = _parse_dt(ws), _parse_dt(we)
        if wsd is not None and wed is not None and wsd <= tdt < wed:
            return (ws, we)
    return None


def _equipment_genealogy_docs(start, end, lineage_rows, defect_map,
                              equip_map=None, bc_meta=None, bc_window=None):
    """
    Build the `data_geneolgys` documents — ONE document PER (equipmentid, day
    window). The overall [start, end) range is split into 06:00->06:00 day
    windows (see _split_day_windows); each barcode is routed to the window its
    UFM test time (uf_time) falls in via `bc_window`. Re-running the same day
    hits the SAME deterministic _id (equipmentid + window), so the same
    equipmentid + sourcelotid updates the SAME document.

    (NOT per barcode, NOT per SKU).

    Each equipmentid document holds every DISTINCT sourcelotid seen on that
    equipment. If an equipmentid has multiple sourcelotids, each is stored as
    an individual entry in the `sourcelotids` array. For each sourcelotid, the
    linkage walks sourcelotid -> targetlotid level by level down to the raw
    material (every lotid linked).

        {
          _id,
          equipmentid, workcentergroup, workcentersubgroup,
          machinetype, machinesupplier,
          sku: [                             # ONE entry per sku
            {
              sku,
              vst_vhmc_mes_production_order: [   # grouped per production order
                {
                  vst_vhmc_mes_production_order,  # the order id
                  tbm: [...],                     # tbm(s) for THIS order
                  curing: [...],                  # curing(s) for THIS order
                  ufm: [...],                     # ufm(s) for THIS order
                }, ...
              ],
            }, ...
          ],
          total_tyres, defect_count,         # OUTSIDE (equipment level)
          sourcelotids: [
            {
              sourcelotid,
              mheid, defid,                  # scalar, for this sourcelotid
              total_tyres, defect_count,     # INSIDE (per sourcelotid)
              linkage: [ { sourcelotid, targetlotid, equipmentid, mheid,
                           defid, workcentergroup, workcentersubgroup,
                           machinetype, machinesupplier }, ...to raw material ]
            }, ...
          ]
        }

    _id is deterministic (equipmentid + window) so re-runs replace cleanly.
    """
    lineage_rows = lineage_rows or []
    defect_map = defect_map or {}

    def _bc_is_defect(bc):
        return bool(defect_map.get(str(bc).strip()))

    def _as_meta_list(value):
        """One-or-more -> list of non-empty string values."""
        out = []
        for v in (value if isinstance(value, (list, tuple, set)) else [value]):
            s = _json_safe(v)
            if s is not None and str(s).strip() != "":
                out.append(s)
        return out

    # Index rows by targetlotid for the in-memory linkage walk.
    by_target = defaultdict(list)
    for r in lineage_rows:
        tgt = r.get("targetlotid")
        if tgt is not None:
            by_target[str(tgt)].append(r)

    def _enrich(eq):
        """data_integration fields for an equipmentid (or all None)."""
        if equip_map and eq:
            info = equip_map.get(eq) or equip_map.get(str(eq).upper())
            if info:
                return (info.get("workcentergroup"),
                        info.get("workcentersubgroup"),
                        info.get("machinetype"),
                        info.get("machinesupplier"))
        return (None, None, None, None)

    def _build_linkage(start_lotid, _guard=None):
        """
        BRANCHING linkage. For `start_lotid`, take EVERY row whose
        targetlotid == start_lotid (a lot can be built from many source lots),
        emit each as a node, and recurse into each node's own sourcelotid —
        continuing level by level until the raw material (no more matches).

        Each node carries its fields plus a nested `linkage` array = the rows
        that feed ITS sourcelotid. A per-path visited guard prevents cycles.
        """
        current = str(start_lotid) if start_lotid is not None else None
        if not current:
            return []
        _guard = _guard or set()
        if current in _guard:
            return []
        _guard = _guard | {current}

        nodes = []
        for row in by_target.get(current, []):
            g, sg, mt, ms = _enrich(str(row.get("equipmentid") or "").strip())
            src = row.get("sourcelotid")
            node = {
                "sourcelotid": _json_safe(src),
                "targetlotid": _json_safe(row.get("targetlotid")),
                "equipmentid": _json_safe(row.get("equipmentid")),
                "mheid": _json_safe(row.get("mheid")),
                "defid": _json_safe(row.get("defid")),
                "workcentergroup": g,
                "workcentersubgroup": sg,
                "machinetype": mt,
                "machinesupplier": ms,
                # Upstream rows that feed THIS source lot (recursion).
                "linkage": _build_linkage(src, _guard),
            }
            nodes.append(node)
        return nodes

    # equipmentid PREFIXES to EXCLUDE entirely (case-insensitive). Tyre-building
    # (PTBM*) and curing (PCUR*) equipment are skipped — no document is created
    # for them. e.g. "PTBM13", "PCUR07".
    _EXCLUDE_EQ_PREFIXES = ("PTBM", "PCUR")

    # ── Group by equipmentid -> sourcelotid, collecting the distinct barcodes
    # that pass through each (for the inside / outside tyre & defect counts),
    # plus the distinct mheid / defid seen for each (equipmentid, sourcelotid).
    #
    # Everything is keyed PER DAY WINDOW: the overall range is split into
    # 06:00->06:00 windows and each barcode is routed to the window its UFM
    # test time (uf_time, from bc_window) falls in. A barcode with no resolved
    # window falls back to the FIRST window so it is never dropped.
    windows = _split_day_windows(start, end)
    bc_window = bc_window or {}
    default_win = windows[0] if windows else (str(start), str(end))

    def _win_of(bc):
        w = bc_window.get(str(bc).strip())
        return tuple(w) if w else default_win

    # (equipmentid, window) -> sourcelotid -> set(barcodes)
    eq_src_bcs = defaultdict(lambda: defaultdict(set))
    # (equipmentid, window) -> sourcelotid -> {"mheid": [...], "defid": [...]}
    eq_src_mhe_def = defaultdict(lambda: defaultdict(
        lambda: {"mheid": [], "defid": []}
    ))
    for r in lineage_rows:
        eq = str(r.get("equipmentid") or "").strip()
        src = r.get("sourcelotid")
        # Skip null / empty equipmentid.
        if not eq or src is None:
            continue
        # Skip equipmentids starting with an excluded prefix (PTBM / PCUR).
        if eq.upper().startswith(_EXCLUDE_EQ_PREFIXES):
            continue
        bc = str(r.get("_root_barcode") or "").strip()
        # Deep rows carry no _root_barcode; they only feed the linkage walk and
        # are not window-routed here.
        if not bc:
            continue
        win = _win_of(bc)
        src_key = str(src)
        # Collect distinct mheid / defid for this (equipmentid, window, sourcelotid).
        md = eq_src_mhe_def[(eq, win)][src_key]
        for v in _as_meta_list(r.get("mheid")):
            if v not in md["mheid"]:
                md["mheid"].append(v)
        for v in _as_meta_list(r.get("defid")):
            if v not in md["defid"]:
                md["defid"].append(v)
        eq_src_bcs[(eq, win)][src_key].add(bc)

    docs = []
    for (eq, win), src_map in eq_src_bcs.items():
        win_start, win_end = win
        g, sg, mt, ms = _enrich(eq)
        source_entries = []
        eq_bcs = set()
        for src, bcs in src_map.items():
            eq_bcs |= bcs
            n_def = sum(1 for b in bcs if _bc_is_defect(b))
            md = eq_src_mhe_def[(eq, win)][src]
            source_entries.append({
                "sourcelotid": src,
                "mheid": md["mheid"][0] if md["mheid"] else None,
                "defid": md["defid"][0] if md["defid"] else None,
                "total_tyres": len(bcs),       # INSIDE (per sourcelotid)
                "defect_count": n_def,         # INSIDE (per sourcelotid)
                "linkage": _build_linkage(src),
            })
        eq_def = sum(1 for b in eq_bcs if _bc_is_defect(b))

        # Build ONE entry per sku across the barcodes that passed through this
        # equipment. Inside each sku, group by vst_vhmc_mes_production_order so
        # the tbm / curing / ufm are tracked PER production order (which order
        # ran on which tbm / curing_press / ufm). Order follows first
        # appearance; nested lists keep first-appearance order too.
        sku_order = []
        sku_groups = {}
        for b in eq_bcs:
            meta = (bc_meta or {}).get(b) or {}
            sku_vals = _as_meta_list(meta.get("sku")) or [None]
            orders = _as_meta_list(meta.get("production_order")) or [None]
            tbms = _as_meta_list(meta.get("tbm"))
            cures = _as_meta_list(meta.get("curing_press"))
            ufms = _as_meta_list(meta.get("ufm"))
            for sku_val in sku_vals:
                if sku_val not in sku_groups:
                    sku_groups[sku_val] = {"order_sequence": [], "orders": {}}
                    sku_order.append(sku_val)
                order_map = sku_groups[sku_val]["orders"]
                order_seq = sku_groups[sku_val]["order_sequence"]
                for order_val in orders:
                    if order_val not in order_map:
                        order_map[order_val] = {
                            "tbm": [],
                            "curing": [],
                            "ufm": [],
                        }
                        order_seq.append(order_val)
                    po_lists = order_map[order_val]
                    for v in tbms:
                        if v not in po_lists["tbm"]:
                            po_lists["tbm"].append(v)
                    for v in cures:
                        if v not in po_lists["curing"]:
                            po_lists["curing"].append(v)
                    for v in ufms:
                        if v not in po_lists["ufm"]:
                            po_lists["ufm"].append(v)

        sku_entries = []
        for sku_val in sku_order:
            grp = sku_groups[sku_val]
            po_entries = []
            for order_val in grp["order_sequence"]:
                po_lists = grp["orders"][order_val]
                po_entries.append({
                    "vst_vhmc_mes_production_order": order_val,
                    "tbm": po_lists["tbm"],
                    "curing": po_lists["curing"],
                    "ufm": po_lists["ufm"],
                })
            sku_entries.append({
                "sku": sku_val,
                "vst_vhmc_mes_production_order": po_entries,
            })

        # _id is deterministic per (equipmentid, day window) so re-running the
        # SAME day updates the SAME document (same equipmentid + sourcelotid).
        key_src = f"{eq}|{win_start}|{win_end}"
        docs.append({
            "_id": hashlib.md5(key_src.encode("utf-8")).hexdigest(),
            "equipmentid": eq,
            "workcentergroup": g,
            "workcentersubgroup": sg,
            "machinetype": mt,
            "machinesupplier": ms,
            "start_date": win_start,
            "end_date": win_end,
            "sku": sku_entries,
            "total_tyres": len(eq_bcs),        # OUTSIDE (equipment level)
            "defect_count": eq_def,            # OUTSIDE (equipment level)
            "sourcelotids": source_entries,
            "ingested_at": _dt.datetime.utcnow().isoformat(),
        })
    return docs


# ── Equipment-integration lookup (data_integration) ─────────────────────────
# production_db_hungary.data_integration maps a workcenter (sapworkcenter) to
# its group / subgroup / machine type / supplier. Each lineage node's
# equipmentid is matched to sapworkcenter to attach those fields.
EQUIP_INTEGRATION_DB = "production_db_hungary"
EQUIP_INTEGRATION_COLLECTION = "data_integration"


def _load_equipment_integration(mongo):
    """
    {sapworkcenter -> {workcentergroup, workcentersubgroup, machinetype,
    machinesupplier}} from production_db_hungary.data_integration.

    Keyed both as-is and upper-cased so the equipmentid match is
    case-insensitive.
    """
    coll = mongo[EQUIP_INTEGRATION_DB][EQUIP_INTEGRATION_COLLECTION]
    mapping = {}
    projection = {
        "_id": 0,
        "sapworkcenter": 1,
        "workcentergroup": 1,
        "workcentersubgroup": 1,
        "machinetype": 1,
        "machinesupplier": 1,
    }
    try:
        for d in coll.find({}, projection):
            key = str(d.get("sapworkcenter") or "").strip()
            if not key:
                continue
            info = {
                "workcentergroup": d.get("workcentergroup"),
                "workcentersubgroup": d.get("workcentersubgroup"),
                "machinetype": d.get("machinetype"),
                "machinesupplier": d.get("machinesupplier"),
            }
            mapping[key] = info
            mapping[key.upper()] = info
    except Exception as e:
        print(f"  [warn] data_integration load failed: {e}")
    return mapping


# ── SKU universe (production_db_hungary.channel_no_ufm) ─────────────────────
# The Redshift read is restricted to the SKUs configured in this collection:
# only t_zf_test_completed rows whose `sku` is in channel_no_ufm.sku are read.
SKU_SOURCE_DB = "production_db_hungary"
SKU_SOURCE_COLLECTION = "channel_no_ufm"


def _load_channel_skus(mongo):
    """
    Distinct, non-empty `sku` values from
    production_db_hungary.channel_no_ufm. This is the ONLY SKU set read from
    Redshift. Returns a list of SKU strings (empty if none / on error).
    """
    try:
        coll = mongo[SKU_SOURCE_DB][SKU_SOURCE_COLLECTION]
        skus = [
            str(s).strip()
            for s in coll.distinct("sku")
            if s is not None and str(s).strip() != ""
        ]
        return sorted(set(skus))
    except Exception as e:
        print(f"  [warn] channel_no_ufm SKU load failed: {e}")
        return []


def ingest_genealogy_to_mongo(start=None, end=None, barcodes=None, skus=None):
    """
    Resolve barcodes by SKU + timestamp window, keep only those present in the
    genealogy table, then write TWO collections:

      * data_merged_tbm_curing_scrap_data — ONE doc per barcode OCCURRENCE
        (barcode + last_change_time): `tire_barcode`, `uf_time`
        (t_zf_test_completed.timestamp), `last_change_time`,
        first-level genealogy `components`, and the `rerun` / `last` flags
        (first occurrence rerun=0; later ones rerun=1; only the latest
        occurrence has last=1).

      * data_geneolgys — ONE doc per EQUIPMENTID: its distinct sourcelotids
        (array), each walking its source->target linkage down to the raw
        material; total_tyres / defect_count both OUTSIDE (equipment level)
        and INSIDE (per sourcelotid).

    Anchored on t_zf_test_completed for the timestamp window / sku.
    Returns the number of per-barcode tracking docs written.
    """
    from pymongo import MongoClient
    from pymongo.errors import BulkWriteError

    start = start or UFM_START
    end = end or UFM_END

    # ── Restrict the Redshift read to the SKUs in channel_no_ufm ──
    # Only applies to a window-driven run (no explicit barcodes) and when a
    # SKU list wasn't already passed in. An explicit --barcodes run is the
    # barcode's intent, so it is NOT SKU-filtered.
    if not barcodes and not skus:
        print(f"Loading SKU universe from {SKU_SOURCE_DB}.{SKU_SOURCE_COLLECTION}...")
        _sku_mc = MongoClient(GENEOLOGY_MONGO_URI, serverSelectionTimeoutMS=5000)
        try:
            skus = _load_channel_skus(_sku_mc)
        finally:
            _sku_mc.close()
        print(f"channel_no_ufm SKUs to read from Redshift: {len(skus)}")
        if not skus:
            print("No SKUs found in channel_no_ufm — nothing to read from Redshift.")
            return 0

    print("Connecting to Redshift...")
    conn = _redshift_connect()
    try:
        print(f"Resolving barcodes from t_zf_test_completed ({start} .. {end})...")
        anchor_map = _resolve_zf_anchor(
            conn, start=start, end=end, barcodes=barcodes, skus=skus
        )
        bcs = list(anchor_map.keys())
        print(f"Barcodes to trace: {len(bcs)}")
        if not bcs:
            print("No barcodes found — nothing to ingest.")
            return 0

        print("Fetching TBM production header (tbm_name / gt_code)...")
        header_map = _fetch_production_header_standalone(conn, bcs)
        print(f"Barcodes with production header: {len(header_map)}")

        print("Fetching curing (curing_press)...")
        curing_map = _fetch_curing_standalone(conn, bcs)
        print(f"Barcodes with curing: {len(curing_map)}")

        print("Fetching Mono-ply digital-thread parameter...")
        ply_map = _fetch_mono_ply_standalone(conn, bcs)
        print(f"Barcodes with Mono-ply info: {len(ply_map)}")

        print("Fetching defect flags (mv_tire -> stock_unit_repair)...")
        defect_map = _fetch_defect_map(conn, bcs)
        print(f"Barcodes with a repair comment (defects): "
              f"{sum(1 for v in defect_map.values() if v)}")

        print("Fetching repair detail (stock_unit_repair: time/code/comment)...")
        repair_map = _fetch_repair_map(conn, bcs)
        print(f"Barcodes with a repair row: {len(repair_map)}")

        print("Fetching last_change_time (mv_tire) for rerun/last...")
        lct_map = _fetch_last_change_time(conn, bcs)
        print(f"Barcodes with a last_change_time: {len(lct_map)}")

        print("Fetching genealogy lineage rows...")
        lineage_rows = _fetch_genealogy_rows_standalone(conn, bcs)
        print(f"Genealogy lineage rows fetched: {len(lineage_rows)}")
    finally:
        conn.close()

    # Group the LEVEL-1 rows by their matched tyre barcode (`_root_barcode`,
    # set only on level-1 rows). Deep graph rows carry no _root_barcode and are
    # NOT grouped per barcode — they feed the equipment linkage walk only.
    lineage_by_bc = defaultdict(list)
    for r in lineage_rows:
        bc = str(r.get("_root_barcode") or "").strip()
        if bc:
            lineage_by_bc[bc].append(r)

    # ── BARCODE SET is driven ENTIRELY by t_zf_test_completed (uf_time
    # window / sku). EVERY anchor barcode is written to the tracking
    # collection; genealogy / TBM / curing / repair are LEFT-JOIN enrichments
    # that NEVER shrink the set. A barcode with no genealogy rows still gets a
    # doc (empty `components`), so barcodes present in t_zf_test_completed are
    # no longer dropped just because they are missing elsewhere.
    tracking_bcs = list(bcs)

    # `genealogy_bcs` is used ONLY for OUTPUT 2 (data_geneolgys), which is
    # inherently per-equipment and requires lineage rows to exist.
    genealogy_bcs = [bc for bc in bcs if lineage_by_bc.get(bc)]
    missing_gene = len(bcs) - len(genealogy_bcs)
    print(f"Barcodes from t_zf_test_completed: {len(tracking_bcs)} "
          f"(all written to tracking; {missing_gene} have no genealogy rows "
          f"-> empty components)")
    if not tracking_bcs:
        print("No barcodes from t_zf_test_completed — nothing to ingest.")
        return 0

    print(f"Connecting to MongoDB ({GENEOLOGY_MONGO_URI})...")
    # Tuned for bulk-insert throughput: w=1 (fast ack), big connection pool,
    # and wire compression (pymongo uses whichever listed codec is installed,
    # and silently ignores the rest) so large batches move faster.
    mongo = MongoClient(
        GENEOLOGY_MONGO_URI,
        serverSelectionTimeoutMS=5000,
        w=1,
        maxPoolSize=50,
        retryWrites=False,
        compressors="zstd,snappy,zlib",
    )
    try:
        # Equipment-master lookup so every equipmentid can be enriched
        # (workcentergroup / subgroup / machinetype / machinesupplier) from
        # production_db_hungary.data_integration (sapworkcenter match).
        print("Loading equipment integration (data_integration)...")
        equip_map = _load_equipment_integration(mongo)
        print(f"data_integration workcenters loaded: {len(equip_map)}")

        from pymongo import UpdateOne, ReplaceOne

        # ── OUTPUT 1: per-tire_barcode tracking docs ───────────────────────
        # ONE document per tire_barcode (keyed by _id = tire_barcode). The
        # upsert MERGES into any existing doc:
        #   * if the tire_barcode already exists  -> UPDATE uf_id, uf_time,
        #     last, rerun, components (plus the TBM/curing fields this job
        #     fetches). The scrap/repair fields owned by the historical scrap
        #     pipeline are left untouched.
        #   * if the tire_barcode does NOT exist  -> INSERT the full doc, with
        #     the scrap/repair fields seeded null.
        # rerun: 1 if a doc for this barcode already existed before this run
        #        (i.e. the barcode is being re-ingested), else 0. last stays 1
        #        because there is exactly one doc per barcode.
        tracking_coll = mongo[GENEOLOGY_DB_NAME][TRACKING_COLLECTION_NAME]
        try:
            tracking_coll.create_index("tire_barcode")
        except Exception as e:
            print(f"  [tracking] index on tire_barcode skipped: {e}")
        tracking_docs = [
            _tracking_doc(bc, lineage_by_bc.get(bc), equip_map,
                          last_change_time=lct_map.get(bc),
                          uf_time=(anchor_map.get(bc) or {}).get("date_of_ufm"),
                          uf_id=(anchor_map.get(bc) or {}).get("ufm_machine"),
                          header=header_map.get(bc),
                          curing=curing_map.get(bc),
                          repair=repair_map.get(bc),
                          sku=(anchor_map.get(bc) or {}).get("sku"))
            for bc in tracking_bcs
        ]

        # ── rerun flag: pre-fetch which barcodes already exist (few bulk $in
        # queries, NOT one find() per barcode) so a re-ingested barcode is
        # flagged rerun=1 in RAM. ──
        all_barcodes = [d["tire_barcode"] for d in tracking_docs]
        existing_bc = set()
        PREFETCH_CHUNK = 20000
        for i in range(0, len(all_barcodes), PREFETCH_CHUNK):
            chunk_bcs = all_barcodes[i:i + PREFETCH_CHUNK]
            try:
                cursor = tracking_coll.find(
                    {"_id": {"$in": chunk_bcs}}, {"_id": 1}
                )
                for e in cursor:
                    existing_bc.add(e.get("_id"))
            except Exception as e:
                print(f"  [tracking] existing-barcode prefetch skipped: {e}")

        for d in tracking_docs:
            d["rerun"] = 1 if d["tire_barcode"] in existing_bc else 0
            d["last"] = 1

        # ── Write with per-barcode upsert (UpdateOne) ───────────────────────
        # $set       -> the fields this job owns (refreshed every run).
        # $setOnInsert -> the scrap/repair fields + _id/tire_barcode/ingested_at
        #                 (written ONCE, only when the doc is first created, so
        #                 existing scrap data is never overwritten).
        t_upserted = 0
        for i in range(0, len(tracking_docs), GENEOLOGY_INSERT_BATCH):
            batch = tracking_docs[i:i + GENEOLOGY_INSERT_BATCH]
            ops = []
            for d in batch:
                set_fields = {
                    "tire_barcode": d["tire_barcode"],
                    "sku": d["sku"],
                    "uf_id": d["uf_id"],
                    "uf_time": d["uf_time"],
                    "last_change_time": d["last_change_time"],
                    "rerun": d["rerun"],
                    "last": d["last"],
                    "components": d["components"],
                    "ptb_id": d["ptb_id"],
                    "ptb_time": d["ptb_time"],
                    "cur_id": d["cur_id"],
                    "cur_time": d["cur_time"],
                    "cur_mouldid": d["cur_mouldid"],
                    "sur_repair_time": d["sur_repair_time"],
                    "re_code": d["re_code"],
                    "re_comment": d["re_comment"],
                }
                for col in _TBM_SPOT_POSITION_FIELDS:
                    set_fields[col] = d.get(col)
                set_on_insert = {
                    "ingested_at": d["ingested_at"],
                }
                for col in _TRACKING_NULL_DEFECT_FIELDS:
                    set_on_insert[col] = None
                ops.append(UpdateOne(
                    {"_id": d["_id"]},
                    {"$set": set_fields, "$setOnInsert": set_on_insert},
                    upsert=True,
                ))
            try:
                res = tracking_coll.bulk_write(ops, ordered=False)
                t_upserted += len(res.upserted_ids) + res.inserted_count + res.modified_count
            except BulkWriteError as bwe:
                errs = bwe.details.get("writeErrors", [])
                if errs:
                    print(f"  [tracking] batch {i // GENEOLOGY_INSERT_BATCH}: "
                          f"{len(errs)} errors, e.g. {errs[0].get('errmsg')}")

        print(f"Tracking docs written (data_merged_tbm_curing_scrap_data): "
              f"{len(tracking_docs)} (upserted/updated {t_upserted})")

        # ── OUTPUT 2: per-EQUIPMENTID genealogy docs ──────────────────────
        # One document per equipmentid (NOT per SKU, NOT per barcode).
        # Pass ALL lineage rows: the LEVEL-1 rows (tagged with _root_barcode)
        # drive the equipmentid -> sourcelotid grouping and the tyre / defect
        # counts, while the DEEP graph rows provide the sourcelotid ->
        # targetlotid linkage down to the raw material.
        # Per-barcode sku / tbm / curing_press / ufm / production_order so each
        # equipment doc can group them: one entry per sku, carrying the related
        # production_order / tbm / curing / ufm for that sku.
        bc_meta = {}
        for bc in genealogy_bcs:
            anchor = anchor_map.get(bc) or {}
            header = header_map.get(bc) or {}
            curing = curing_map.get(bc) or {}
            bc_meta[bc] = {
                "sku": anchor.get("sku"),
                "tbm": header.get("tbm_name"),
                "curing_press": curing.get("curing_press"),
                "ufm": anchor.get("ufm_machine"),
                "production_order": header.get("order_number"),
            }

        # ── Route each barcode to its 06:00->06:00 day window by its UFM test
        # time (uf_time). A 2-day run is split into two day documents; a
        # sub-day run becomes one next-morning window. ──
        day_windows = _split_day_windows(start, end)
        bc_window = {}
        for bc in genealogy_bcs:
            uf_time = (anchor_map.get(bc) or {}).get("date_of_ufm")
            win = _window_for_time(uf_time, day_windows)
            if win:
                bc_window[bc] = win

        gene_coll = mongo[GENEOLOGY_DB_NAME][GENEOLOGY_COLLECTION_NAME]
        gene_docs = _equipment_genealogy_docs(
            start, end, lineage_rows, defect_map, equip_map, bc_meta,
            bc_window=bc_window
        )

        # A single equipmentid can accumulate a linkage tree larger than
        # MongoDB's 16 MB per-document limit (DocumentTooLarge). Split any
        # oversized document across sequential "part" docs WITHOUT dropping
        # lineage — full depth to raw material is preserved.
        write_docs = []
        split_count = 0
        for d in gene_docs:
            parts = _split_genealogy_doc(d)
            if len(parts) > 1:
                split_count += 1
                print(f"  [genealogy] equipmentid {d.get('equipmentid')!r} "
                      f"doc too large — split into {len(parts)} parts")
            write_docs.extend(parts)

        # Remove any stale docs from a previous run for the SAME equipment
        # windows (a prior run may have written a single un-split doc, or a
        # different number of parts). Each base _id and its "::partNNNN"
        # children are cleared so this run's upserts stay idempotent.
        base_ids = [d["_id"] for d in gene_docs]
        if base_ids:
            del_or = []
            for bid in base_ids:
                del_or.append({"_id": bid})
                del_or.append({"_id": {"$regex": f"^{re.escape(bid)}::part"}})
            try:
                gene_coll.delete_many({"$or": del_or})
            except Exception as e:
                print(f"  [genealogy] stale-doc cleanup skipped: {e}")

        # Build ops, then flush by cumulative BSON size so the bulk COMMAND
        # itself never exceeds the 16 MB limit (one giant doc per command).
        g_upserted = 0

        def _flush(ops):
            nonlocal g_upserted
            if not ops:
                return
            try:
                res = gene_coll.bulk_write(ops, ordered=False)
                g_upserted += (len(res.upserted_ids)
                               + res.inserted_count
                               + res.modified_count)
            except BulkWriteError as bwe:
                other = bwe.details.get("writeErrors", [])
                if other:
                    print(f"  [genealogy] {len(other)} write errors, "
                          f"e.g. {other[0].get('errmsg')}")

        pending, pending_bytes = [], 0
        for d in write_docs:
            db = _bson_size(d)
            if pending and (
                len(pending) >= GENEOLOGY_INSERT_BATCH
                or pending_bytes + db > GENEOLOGY_DOC_MAX_BYTES
            ):
                _flush(pending)
                pending, pending_bytes = [], 0
            pending.append(ReplaceOne({"_id": d["_id"]}, d, upsert=True))
            pending_bytes += db
        _flush(pending)

        print(f"Equipment genealogy docs written (data_geneolgys): "
              f"{len(write_docs)} docs from {len(gene_docs)} equipment "
              f"({split_count} split; upserted/updated {g_upserted})")

        return len(tracking_docs)
    finally:
        mongo.close()


if __name__ == "__main__":
    # Usage:
    #   python data_genealogy.py                       (default window below)
    #   python data_genealogy.py 2026-10-03 2026-10-04 (explicit window)
    #   python data_genealogy.py --barcodes BC1 BC2    (explicit barcodes)
    import sys

    args = sys.argv[1:]
    start = UFM_START
    end = UFM_END
    explicit = None
    if args and args[0] == "--barcodes":
        explicit = args[1:]
    elif len(args) >= 2:
        start, end = args[0], args[1]
    elif len(args) == 1:
        start = args[0]

    ingest_genealogy_to_mongo(start=start, end=end, barcodes=explicit)
