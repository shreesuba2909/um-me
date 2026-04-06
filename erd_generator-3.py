"""
erd_generator.py

Parses DDL SQL and renders an Entity Relationship Diagram via Graphviz.

Returns the diagram as a base64-encoded PNG so the FastAPI endpoint can
serve it without writing temp files to disk.

Also supports:
- draw.io XML export        (generate_erd_xml)
- PowerDesigner .pdm export (generate_erd_pdm)
- ERD directly from JSON    (generate_erd_from_model)
"""

import re
import base64
import logging
import uuid
import time as _time
import xml.etree.ElementTree as ET
from xml.dom import minidom
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from graphviz import Digraph

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Data classes
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class Column:
    name: str
    data_type: str
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_nullable: bool = True
    is_unique: bool = False
    default: Optional[str] = None

@dataclass
class ForeignKey:
    from_table: str
    from_col: str
    to_table: str
    to_col: str

@dataclass
class Table:
    name: str
    columns: List[Column] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────────────
# DDL Parser
# ──────────────────────────────────────────────────────────────────────────────

class DDLParser:

    TABLE_RE = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"[`\"\[]?([\w]+(?:\.[\w]+)*)[`\"\]]?",
        re.IGNORECASE,
    )

    COLUMN_RE = re.compile(
        r'^\s*[`"\[]?(\w+)[`"\]]?\s+([\w]+(?:\s*\([^)]*\))?)(.*?)$',
        re.IGNORECASE
    )

    PK_INLINE_RE = re.compile(r'\bPRIMARY\s+KEY\b', re.IGNORECASE)

    FK_CONSTRAINT_RE = re.compile(
        r'FOREIGN\s+KEY\s*\([`"\[]?(\w+)[`"\]]?\)\s*REFERENCES\s+'
        r'[`"\[]?([\w]+(?:\.[\w]+)*)[`"\]]?\s*\([`"\[]?(\w+)[`"\]]?\)',
        re.IGNORECASE,
    )

    PK_CONSTRAINT_RE = re.compile(r'PRIMARY\s+KEY\s*\(([^)]+)\)', re.IGNORECASE)
    NOT_NULL_RE = re.compile(r'\bNOT\s+NULL\b', re.IGNORECASE)
    UNIQUE_RE = re.compile(r'\bUNIQUE\b', re.IGNORECASE)
    DEFAULT_RE = re.compile(r"\bDEFAULT\s+('?[^,\s)]+'?)", re.IGNORECASE)

    def parse(self, sql_text: str) -> Tuple[Dict[str, Table], List[ForeignKey]]:
        tables: Dict[str, Table] = {}
        foreign_keys: List[ForeignKey] = []

        blocks = re.split(r'(?=CREATE\s+TABLE)', sql_text, flags=re.IGNORECASE)

        for block in blocks:
            block = block.strip()
            if not block:
                continue

            m = self.TABLE_RE.match(block)
            if not m:
                continue

            table_name = m.group(1)
            table = Table(name=table_name)

            body = self._extract_body(block)
            if not body:
                continue

            for line in self._split_definitions(body):
                line = line.strip().rstrip(',').strip()
                if not line:
                    continue

                upper = line.upper().lstrip()

                pk_m = self.PK_CONSTRAINT_RE.search(line)
                if upper.startswith('PRIMARY') and pk_m:
                    pks = [c.strip().strip('`"[]') for c in pk_m.group(1).split(',')]
                    table.primary_keys.extend(pks)
                    continue

                fk_m = self.FK_CONSTRAINT_RE.search(line)
                if fk_m:
                    foreign_keys.append(
                        ForeignKey(
                            from_table=table_name,
                            from_col=fk_m.group(1),
                            to_table=fk_m.group(2),
                            to_col=fk_m.group(3),
                        )
                    )
                    continue

                if re.match(r'^(UNIQUE|INDEX|KEY|CHECK|CONSTRAINT)\b', upper):
                    continue

                col_m = self.COLUMN_RE.match(line)
                if col_m:
                    col_name = col_m.group(1)
                    col_type = col_m.group(2).strip()
                    rest = col_m.group(3)

                    is_pk = bool(self.PK_INLINE_RE.search(rest))
                    is_unique = bool(self.UNIQUE_RE.search(rest))
                    nullable = not bool(self.NOT_NULL_RE.search(rest)) and not is_pk

                    default_m = self.DEFAULT_RE.search(rest)

                    if is_pk:
                        table.primary_keys.append(col_name)

                    table.columns.append(
                        Column(
                            name=col_name,
                            data_type=col_type.upper(),
                            is_primary_key=is_pk,
                            is_nullable=nullable,
                            is_unique=is_unique,
                            default=(default_m.group(1) if default_m else None),
                        )
                    )

            tables[table_name] = table

        fk_lookup = {(fk.from_table, fk.from_col): fk for fk in foreign_keys}

        for table in tables.values():
            for col in table.columns:
                if (table.name, col.name) in fk_lookup:
                    col.is_foreign_key = True

            for pk_name in table.primary_keys:
                for col in table.columns:
                    if col.name == pk_name:
                        col.is_primary_key = True

        return tables, foreign_keys

    def _extract_body(self, block: str) -> Optional[str]:
        depth, start = 0, None
        for i, ch in enumerate(block):
            if ch == '(':
                if depth == 0:
                    start = i + 1
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0 and start is not None:
                    return block[start:i]
        return None

    def _split_definitions(self, body: str) -> List[str]:
        parts: List[str] = []
        current: List[str] = []
        depth = 0

        for ch in body:
            if ch == '(':
                depth += 1
                current.append(ch)
            elif ch == ')':
                depth -= 1
                current.append(ch)
            elif ch == ',' and depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(ch)

        if current:
            parts.append(''.join(current))

        return parts
# ──────────────────────────────────────────────────────────────────────────────
# HTML Table Builder for Graphviz
# ──────────────────────────────────────────────────────────────────────────────

def _html_table(table: Table) -> str:
    """Build an HTML-label Graphviz node for a table."""
    rows = [
        (
            '<TR>'
            '<TD COLSPAN="3" BGCOLOR="#1e2d4a" ALIGN="CENTER">'
            '<FONT COLOR="#4f8ef7" FACE="Helvetica Bold" POINT-SIZE="13">'
            f'<B>{table.name}</B>'
            '</FONT>'
            '</TD>'
            '</TR>'
        )
    ]

    for col in table.columns:
        if col.is_primary_key:
            icon = '<FONT COLOR="#fbbf24">🔑</FONT>'
        elif col.is_foreign_key:
            icon = '<FONT COLOR="#a78bfa">🔗</FONT>'
        else:
            icon = ''

        nullable_marker = '' if col.is_nullable else '<FONT COLOR="#f87171"> *</FONT>'
        unique_marker = '<FONT COLOR="#34d399"> U</FONT>' if col.is_unique else ''

        row_bg = '#162032' if col.is_primary_key else '#0d1520'
        name_color = '#4f8ef7' if col.is_primary_key else '#e2e8f0'

        rows.append(
            '<TR>'
            f'<TD BGCOLOR="{row_bg}" ALIGN="LEFT" WIDTH="20">{icon}</TD>'
            f'<TD BGCOLOR="{row_bg}" ALIGN="LEFT">'
            f'<FONT COLOR="{name_color}" FACE="Helvetica" POINT-SIZE="11">'
            f'{"<B>" if col.is_primary_key else ""}{col.name}{"</B>" if col.is_primary_key else ""}'
            f'{nullable_marker}{unique_marker}'
            '</FONT>'
            '</TD>'
            f'<TD BGCOLOR="{row_bg}" ALIGN="RIGHT">'
            f'<FONT COLOR="#34d399" FACE="Courier" POINT-SIZE="10">{col.data_type}</FONT>'
            '</TD>'
            '</TR>'
        )

    inner = '\n'.join(rows)

    return (
        '<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="6">\n'
        f'{inner}\n'
        '</TABLE>>'
    )


# ──────────────────────────────────────────────────────────────────────────────
# Shared Graphviz builder
# ──────────────────────────────────────────────────────────────────────────────

def _build_dot(tables: Dict[str, Table], foreign_keys: List[ForeignKey],
               title: str, fmt: str) -> Digraph:

    dot = Digraph(name="ERD", comment=title, format=fmt)
    dot.attr(
        rankdir="LR", bgcolor="#0d0f14", fontname="Helvetica",
        pad="0.5", nodesep="0.8", ranksep="1.2",
        label=title, labelloc="t", fontcolor="#64748b", fontsize="14",
    )

    dot.attr("node", shape="none", margin="0", fontname="Helvetica")
    dot.attr("edge", color="#4f8ef7", fontcolor="#94a3b8", fontsize="10",
             fontname="Helvetica", arrowsize="0.8")

    for table in tables.values():
        dot.node(table.name, label=_html_table(table))

    for fk in foreign_keys:
        if fk.from_table in tables and fk.to_table in tables:
            dot.edge(
                fk.from_table,
                fk.to_table,
                label=f" {fk.from_col} → {fk.to_col} ",
                arrowhead="crow", arrowtail="tee",
                dir="both",
                color="#a78bfa",
                style="solid",
            )
    return dot


# ──────────────────────────────────────────────────────────────────────────────
# PNG ERD Generator
# ──────────────────────────────────────────────────────────────────────────────

def generate_erd_base64(sql_text: str,
                        title: str = "Entity Relationship Diagram") -> dict:
    """Parse SQL DDL and render an ERD as a base64 PNG."""

    parser = DDLParser()
    try:
        tables, foreign_keys = parser.parse(sql_text)
    except Exception as e:
        logger.error("DDL parse error: %s", e)
        return {
            "image_base64": None,
            "format": "png",
            "table_count": 0,
            "relationship_count": 0,
            "error": str(e),
        }

    if not tables:
        return {
            "image_base64": None,
            "format": "png",
            "table_count": 0,
            "relationship_count": 0,
            "error": "No tables found in SQL. Include CREATE TABLE statements."
        }

    try:
        dot = _build_dot(tables, foreign_keys, title, "png")
        png_bytes = dot.pipe(format="png")

        return {
            "image_base64": base64.b64encode(png_bytes).decode("utf-8"),
            "format": "png",
            "table_count": len(tables),
            "relationship_count": len(foreign_keys),
            "error": None,
        }

    except Exception as e:
        logger.error("Graphviz render error: %s", e)
        return {
            "image_base64": None,
            "format": "png",
            "table_count": len(tables),
            "relationship_count": len(foreign_keys),
            "error": f"Graphviz render failed: {e}. Ensure Graphviz is installed.",
        }


# ──────────────────────────────────────────────────────────────────────────────
# draw.io XML ERD Generator
# ──────────────────────────────────────────────────────────────────────────────

def generate_erd_xml(sql_text: str,
                     title: str = "Entity Relationship Diagram") -> dict:
    """
    Parse DDL SQL and return a draw.io-compatible XML string.
    """

    parser = DDLParser()
    try:
        tables, foreign_keys = parser.parse(sql_text)
    except Exception as e:
        logger.error("DDL parse error: %s", e)
        return {
            "xml": None, "format": "drawio",
            "table_count": 0, "relationship_count": 0,
            "error": str(e),
        }

    if not tables:
        return {
            "xml": None, "format": "drawio",
            "table_count": 0, "relationship_count": 0,
            "error": "No tables found in SQL."
        }

    # draw.io layout constants
    TABLE_WIDTH = 280
    COL_HEIGHT = 30
    HEADER_HEIGHT = 40
    H_GAP = 60
    V_GAP = 40
    COLS_PER_ROW = 3

    root = ET.Element("mxGraphModel")
    settings = {
        "dx": "1422", "dy": "762",
        "grid": "1", "gridSize": "10",
        "guides": "1", "tooltips": "1",
        "connect": "1", "arrows": "1",
        "fold": "1", "page": "1",
        "pageScale": "1", "pageWidth": "1169",
        "pageHeight": "827", "math": "0", "shadow": "0"
    }
    for k, v in settings.items():
        root.set(k, v)

    parent = ET.SubElement(root, "root")
    ET.SubElement(parent, "mxCell", id="0")
    ET.SubElement(parent, "mxCell", id="1", parent="0")

    cell_id = 2
    table_cell_ids: Dict[str, int] = {}
    col_cell_ids: Dict[Tuple, int] = {}

    # Table nodes
    for idx, table in enumerate(tables.values()):
        row = idx // COLS_PER_ROW
        col_pos = idx % COLS_PER_ROW
        x = col_pos * (TABLE_WIDTH + H_GAP)
        y = row * (HEADER_HEIGHT + len(table.columns) * COL_HEIGHT + V_GAP)

        header_id = cell_id
        table_cell_ids[table.name] = header_id
        cell_id += 1

        header_cell = ET.SubElement(parent, "mxCell",
                                    id=str(header_id),
                                    value=table.name,
                                    style=("shape=table;startSize=30;container=1;"
                                           "collapsible=0;childLayout=tableLayout;"
                                           "fixedRows=1;rowLines=0;fontStyle=1;"
                                           "align=center;resizeLast=1;"
                                           "fillColor=#1e2d4a;fontColor=#4f8ef7;"
                                           "strokeColor=#4f8ef7;fontSize=13;"),
                                    vertex="1", parent="1")

        geo = ET.SubElement(header_cell, "mxGeometry",
                            x=str(x), y=str(y),
                            width=str(TABLE_WIDTH),
                            height=str(HEADER_HEIGHT + len(table.columns) * COL_HEIGHT),
                            **{"as": "geometry"})

        # Column rows
        for col in table.columns:
            col_id = cell_id
            col_cell_ids[(table.name, col.name)] = col_id
            cell_id += 1

            if col.is_primary_key:
                prefix, fill, font_color, font_style = "PK  ", "#162032", "#4f8ef7", "1"
            elif col.is_foreign_key:
                prefix, fill, font_color, font_style = "FK  ", "#0d1520", "#a78bfa", "0"
            else:
                prefix, fill, font_color, font_style = "      ", "#0d1520", "#e2e8f0", "0"

            nullable_str = "" if col.is_nullable else " NOT NULL"
            unique_str = " UNIQUE" if col.is_unique else ""

            label = f"{prefix}{col.name}    {col.data_type}{nullable_str}{unique_str}"

            col_cell = ET.SubElement(parent, "mxCell",
                                     id=str(col_id),
                                     value=label,
                                     style=(f"shape=tableRow;horizontal=0;startSize=0;"
                                            f"swimlaneHead=0;swimlaneBody=0;fillColor={fill};"
                                            f"collapsible=0;dropTarget=0;align=left;spacingLeft=8;"
                                            f"fontColor={font_color};strokeColor=#232840;"
                                            f"fontSize=11;fontStyle={font_style};"
                                            f"fontFamily=Courier New;"),
                                     vertex="1", parent=str(header_id))

            col_geo = ET.SubElement(col_cell, "mxGeometry",
                                    y=str(HEADER_HEIGHT + table.columns.index(col) * COL_HEIGHT),
                                    width=str(TABLE_WIDTH),
                                    height=str(COL_HEIGHT),
                                    **{"as": "geometry"})

    # FK edges
    for fk in foreign_keys:
        if fk.from_table not in tables or fk.to_table not in tables:
            continue

        src_id = col_cell_ids.get((fk.from_table, fk.from_col),
                                  table_cell_ids.get(fk.from_table))
        tgt_id = col_cell_ids.get((fk.to_table, fk.to_col),
                                  table_cell_ids.get(fk.to_table))

        if src_id is None or tgt_id is None:
            continue

        edge_cell = ET.SubElement(parent, "mxCell",
                                  id=str(cell_id),
                                  value=f"{fk.from_col} → {fk.to_col}",
                                  style=("edgeStyle=entityRelationEdgeStyle;"
                                         "endArrow=ERzeroToMany;startArrow=ERmandOne;"
                                         "strokeColor=#a78bfa;fontColor=#94a3b8;fontSize=10;"
                                         "exitX=1;exitY=0.5;entryX=0;entryY=0.5;"),
                                  edge="1", source=str(src_id),
                                  target=str(tgt_id), parent="1")
        cell_id += 1

        ET.SubElement(edge_cell, "mxGeometry", relative="1", **{"as": "geometry"})

    raw = ET.tostring(root, encoding="unicode")
    pretty = minidom.parseString(raw).toprettyxml(indent="  ")

    return {
        "xml": pretty,
        "format": "drawio",
        "table_count": len(tables),
        "relationship_count": len(foreign_keys),
        "error": None,
    }


# ──────────────────────────────────────────────────────────────────────────────
# ERD from JSON data model
# ──────────────────────────────────────────────────────────────────────────────

def _tables_from_model_json(data_model: dict) -> Tuple[Dict[str, Table], List[ForeignKey]]:
    """
    Convert a JSON data model into Table + ForeignKey objects.
    """

    tables: Dict[str, Table] = {}
    foreign_keys: List[ForeignKey] = []

    def _col_type(col: dict) -> str:
        return str(col.get("type") or col.get("data_type") or "TEXT").upper()

    def _process_table(t: dict):
        name = t.get("name", "unknown")
        tbl = Table(name=name)

        pk_list = t.get("primary_key", [])
        if isinstance(pk_list, str):
            pk_list = [pk_list]

        tbl.primary_keys = list(pk_list)

        for col in t.get("columns", []):
            cname = col.get("name", "col")
            ctype = _col_type(col)
            is_pk = col.get("primary_key", False) or cname in tbl.primary_keys
            is_fk = col.get("is_foreign_key", False)
            nullable = col.get("nullable", True)
            unique = col.get("unique", False)

            if is_pk and cname not in tbl.primary_keys:
                tbl.primary_keys.append(cname)

            tbl.columns.append(
                Column(
                    name=cname,
                    data_type=ctype,
                    is_primary_key=is_pk,
                    is_foreign_key=is_fk,
                    is_nullable=nullable,
                    is_unique=unique,
                )
            )

        tables[name] = tbl

    def _process_relationships(rels: list):
        for r in (rels or []):
            fk = ForeignKey(
                from_table=r.get("from_table", ""),
                from_col=r.get("from_column", r.get("from_col", "")),
                to_table=r.get("to_table", ""),
                to_col=r.get("to_column", r.get("to_col", "")),
            )
            if fk.from_table and fk.to_table:
                foreign_keys.append(fk)
                if fk.from_table in tables:
                    for col in tables[fk.from_table].columns:
                        if col.name == fk.from_col:
                            col.is_foreign_key = True

    relational = data_model.get("relational_model") or (
        data_model if data_model.get("tables") else None
    )
    if relational:
        for t in relational.get("tables", []):
            _process_table(t)
        _process_relationships(relational.get("relationships", []))

    analytical = data_model.get("analytical_model") or (
        data_model if data_model.get("fact_tables") else None
    )
    if analytical:
        for t in analytical.get("fact_tables", []):
            _process_table(t)
        for t in analytical.get("dimension_tables", []):
            _process_table(t)
        _process_relationships(analytical.get("relationships", []))

    return tables, foreign_keys


def generate_erd_from_model(data_model: dict,
                            title: str = "Entity Relationship Diagram") -> dict:
    """Generate ERD PNG directly from JSON model."""

    try:
        tables, foreign_keys = _tables_from_model_json(data_model)
    except Exception as e:
        logger.error("Model-to-ERD parse error: %s", e)
        return {
            "image_base64": None,
            "format": "png",
            "table_count": 0,
            "relationship_count": 0,
            "error": str(e),
        }

    if not tables:
        return {
            "image_base64": None,
            "format": "png",
            "table_count": 0,
            "relationship_count": 0,
            "error": "No tables found in data model."
        }

    try:
        dot = _build_dot(tables, foreign_keys, title, "png")
        png_bytes = dot.pipe(format="png")

        return {
            "image_base64": base64.b64encode(png_bytes).decode("utf-8"),
            "format": "png",
            "table_count": len(tables),
            "relationship_count": len(foreign_keys),
            "error": None,
        }

    except Exception as e:
        return {
            "image_base64": None,
            "format": "png",
            "table_count": len(tables),
            "relationship_count": len(foreign_keys),
            "error": f"Graphviz render failed: {e}",
        }


# ──────────────────────────────────────────────────────────────────────────────
# PowerDesigner PDM export
# ──────────────────────────────────────────────────────────────────────────────

_PD_TYPE_MAP = {
    "INT": ("int", None, None),
    "INTEGER": ("int", None, None),
    "INT64": ("bigint", None, None),
    "BIGINT": ("bigint", None, None),
    "SMALLINT": ("smallint", None, None),
    "TINYINT": ("tinyint", None, None),
    "FLOAT": ("float", None, None),
    "FLOAT64": ("float", None, None),
    "DOUBLE": ("double", None, None),
    "DECIMAL": ("decimal", 18, 2),
    "NUMERIC": ("numeric", 18, 2),
    "REAL": ("real", None, None),
    "BOOLEAN": ("bit", None, None),
    "BOOL": ("bit", None, None),
    "BIT": ("bit", None, None),
    "CHAR": ("char", 255, None),
    "VARCHAR": ("varchar", 255, None),
    "STRING": ("nvarchar", 4000, None),
    "TEXT": ("text", None, None),
    "NCHAR": ("nchar", 255, None),
    "NVARCHAR": ("nvarchar", 255, None),
    "NTEXT": ("ntext", None, None),
    "DATE": ("date", None, None),
    "TIME": ("time", None, None),
    "DATETIME": ("datetime", None, None),
    "TIMESTAMP": ("timestamp", None, None),
    "TIMESTAMPTZ": ("timestamp", None, None),
    "TIMESTAMP_NTZ": ("timestamp", None, None),
    "BLOB": ("blob", None, None),
    "CLOB": ("clob", None, None),
    "BINARY": ("binary", None, None),
    "VARBINARY": ("varbinary", None, None),
    "UUID": ("uniqueidentifier", None, None),
    "JSON": ("nvarchar", 4000, None),
    "JSONB": ("nvarchar", 4000, None),
    "SUPER": ("nvarchar", 4000, None),
    "VARIANT": ("nvarchar", 4000, None),
}

def _pd_type_info(raw_type: str):
    base = re.split(r"[\s(]", raw_type)[0].upper()
    paren_m = re.search(r"((\d+)(?:,\s*(\d+))?)", raw_type)

    length = int(paren_m.group(2)) if paren_m else None
    precision = int(paren_m.group(3)) if paren_m and paren_m.group(3) else None

    if base in _PD_TYPE_MAP:
        pd_name, default_len, default_prec = _PD_TYPE_MAP[base]
        return (
            pd_name,
            length if length is not None else default_len,
            precision if precision is not None else default_prec,
        )

    return (raw_type.lower(), length, precision)


def _pdm_id(counter: list) -> str:
    counter[0] += 1
    return f"o{counter[0]}"


def _make_guid() -> str:
    return str(uuid.uuid4()).upper()


def _sub(parent_el, tag: str, text: str = None, **attrs):
    el = ET.SubElement(parent_el, tag)
    for k, v in attrs.items():
        el.set(k, str(v))
    if text is not None:
        el.text = str(text)
    return el


def generate_erd_pdm(sql_text: str,
                     title: str = "Physical Data Model") -> dict:
    """Generate SAP PowerDesigner .pdm XML."""

    parser = DDLParser()

    try:
        tables, foreign_keys = parser.parse(sql_text)
    except Exception as e:
        logger.error("DDL parse error: %s", e)
        return {
            "xml": None,
            "table_count": 0,
            "relationship_count": 0,
            "error": str(e),
        }

    if not tables:
        return {
            "xml": None,
            "table_count": 0,
            "relationship_count": 0,
            "error": "No tables found in SQL."
        }

    ctr = [2]
    model_guid = _make_guid()
    now_ts = str(int(_time.time()))

    table_ids: Dict[str, str] = {}
    col_ids: Dict[Tuple, str] = {}
    pk_ids: Dict[str, str] = {}
    ref_ids: List[Tuple] = []

    for tname in tables:
        table_ids[tname] = _pdm_id(ctr)
        pk_ids[tname] = _pdm_id(ctr)
        for col in tables[tname].columns:
            col_ids[(tname, col.name)] = _pdm_id(ctr)

    for fk in foreign_keys:
        ref_ids.append((fk, _pdm_id(ctr)))

    root = ET.Element("Model")
    root.set("xmlns:a", "attribute")
    root.set("xmlns:c", "collection")
    root.set("xmlns:o", "object")

    root_obj = _sub(root, "o:RootObject", Id="o1")
    children = _sub(root_obj, "c:Children")
    model_el = _sub(children, "o:Model", Id="o2")

    _sub(model_el, "a:ObjectID", text=model_guid)
    _sub(model_el, "a:Name", text=title)
    _sub(model_el, "a:Code", text=re.sub(r"\W+", "_", title).upper())
    _sub(model_el, "a:CreationDate", text=now_ts)
    _sub(model_el, "a:Creator", text="erd_generator")
    _sub(model_el, "a:ModificationDate", text=now_ts)
    _sub(model_el, "a:Modifier", text="erd_generator")
    _sub(model_el, "a:ModelOptions", text="")

    diag_coll = _sub(model_el, "c:PhysicalDiagrams")
    diag_el = _sub(diag_coll, "o:PhysicalDiagram", Id=_pdm_id(ctr))
    _sub(diag_el, "a:ObjectID", text=_make_guid())
    _sub(diag_el, "a:Name", text="DefaultDiagram")
    _sub(diag_el, "c:Symbols")

    tables_coll = _sub(model_el, "c:Tables")

    for tname, table in tables.items():
        tid = table_ids[tname]

        t_el = _sub(tables_coll, "o:Table", Id=tid)
        _sub(t_el, "a:ObjectID", text=_make_guid())
        _sub(t_el, "a:Name", text=tname)
        _sub(t_el, "a:Code", text=tname)
        _sub(t_el, "a:CreationDate", text=now_ts)
        _sub(t_el, "a:Creator", text="erd_generator")
        _sub(t_el, "a:ModificationDate", text=now_ts)
        _sub(t_el, "a:Modifier", text="erd_generator")

        cols_coll = _sub(t_el, "c:Columns")

        pk_col_ids = []

        for col in table.columns:
            cid = col_ids[(tname, col.name)]
            pd_type, length, prec = _pd_type_info(col.data_type)

            c_el = _sub(cols_coll, "o:Column", Id=cid)
            _sub(c_el, "a:ObjectID", text=_make_guid())
            _sub(c_el, "a:Name", text=col.name)
            _sub(c_el, "a:Code", text=col.name)
            _sub(c_el, "a:CreationDate", text=now_ts)
            _sub(c_el, "a:Creator", text="erd_generator")
            _sub(c_el, "a:ModificationDate", text=now_ts)
            _sub(c_el, "a:Modifier", text="erd_generator")
            _sub(c_el, "a:DataType", text=pd_type)
            _sub(c_el, "a:Mandatory", text="1" if not col.is_nullable else "0")

            if length is not None:
                _sub(c_el, "a:Length", text=str(length))
            if prec is not None:
                _sub(c_el, "a:Precision", text=str(prec))
            if col.default is not None:
                _sub(c_el, "a:DefaultValue", text=col.default)

            if col.is_primary_key:
                pk_col_ids.append(cid)

        if pk_col_ids or table.primary_keys:
            pkid = pk_ids[tname]

            keys_c = _sub(t_el, "c:Keys")
            pk_el = _sub(keys_c, "o:Key", Id=pkid)

            _sub(pk_el, "a:ObjectID", text=_make_guid())
            _sub(pk_el, "a:Name", text=f"PK_{tname}")
            _sub(pk_el, "a:Code", text=f"PK_{tname}")
            _sub(pk_el, "a:CreationDate", text=now_ts)
            _sub(pk_el, "a:Creator", text="erd_generator")
            _sub(pk_el, "a:ModificationDate", text=now_ts)
            _sub(pk_el, "a:Modifier", text="erd_generator")

            kc_coll = _sub(pk_el, "c:Key.Columns")
            for cid in pk_col_ids:
                ET.SubElement(kc_coll, "o:Column").set("Ref", cid)

            primary_key_ref = _sub(t_el, "c:PrimaryKey")
            ET.SubElement(primary_key_ref, "o:Key").set("Ref", pkid)

    if ref_ids:
        refs_coll = _sub(model_el, "c:References")

        for fk, rid in ref_ids:
            if fk.from_table not in tables or fk.to_table not in tables:
                continue

            r_el = _sub(refs_coll, "o:Reference", Id=rid)
            ref_name = f"FK_{fk.from_table}_{fk.from_col}"

            _sub(r_el, "a:ObjectID", text=_make_guid())
            _sub(r_el, "a:Name", text=ref_name)
            _sub(r_el, "a:Code", text=ref_name)
            _sub(r_el, "a:CreationDate", text=now_ts)
            _sub(r_el, "a:Creator", text="erd_generator")
            _sub(r_el, "a:ModificationDate", text=now_ts)
            _sub(r_el, "a:Modifier", text="erd_generator")

            ET.SubElement(_sub(r_el, "c:ParentTable"),
                          "o:Table").set("Ref", table_ids[fk.to_table])
            ET.SubElement(_sub(r_el, "c:ChildTable"),
                          "o:Table").set("Ref", table_ids[fk.from_table])

            parent_pkid = pk_ids.get(fk.to_table)
            if parent_pkid:
                ET.SubElement(_sub(r_el, "c:ParentKey"),
                              "o:Key").set("Ref", parent_pkid)

            joins_coll = _sub(r_el, "c:Joins")
            join_el = _sub(joins_coll, "o:ReferenceJoin", Id=_pdm_id(ctr))
            _sub(join_el, "a:ObjectID", text=_make_guid())

            child_col_id = col_ids.get((fk.from_table, fk.from_col))
            parent_col_id = col_ids.get((fk.to_table, fk.to_col))

            if child_col_id:
                ET.SubElement(_sub(join_el, "c:Object1"),
                              "o:Column").set("Ref", child_col_id)
            if parent_col_id:
                ET.SubElement(_sub(join_el, "c:Object2"),
                              "o:Column").set("Ref", parent_col_id)

    raw = ET.tostring(root, encoding="unicode", xml_declaration=False)
    pretty = minidom.parseString(raw).toprettyxml(indent="  ", encoding=None)

    body_lines = pretty.splitlines()[1:]

    header = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<?PowerDesigner AppLocale="UTF-8" ID="{{{model_guid}}}" '
        f'LastModificationDate="{now_ts}" Name="{title}" '
        f'Objects="{len(tables) + len(foreign_keys)}" '
        f'Symbols="0" Target="ANSI SQL-92" '
        'Type="{CDE44E21-9669-11D1-9914-006097355D9B}" '
        'signature="PDM_DATA_MODEL_XML" version="16.7.0.5765"?>\n'
        '<!-- do not edit this file -->\n'
    )

    return {
        "xml": header + "\n".join(body_lines),
        "table_count": len(tables),
        "relationship_count": len(foreign_keys),
        "error": None,
    }


# ──────────────────────────────────────────────────────────────────────────────
# PowerDesigner LDM export
# ──────────────────────────────────────────────────────────────────────────────

# Logical type mapping: maps physical/DDL types → abstract logical types
_LDM_TYPE_MAP = {
    # Integer family
    "INT": "Integer",
    "INTEGER": "Integer",
    "INT64": "Long Integer",
    "BIGINT": "Long Integer",
    "SMALLINT": "Short Integer",
    "TINYINT": "Short Integer",
    # Floating point / decimal
    "FLOAT": "Float",
    "FLOAT64": "Float",
    "DOUBLE": "Float",
    "REAL": "Float",
    "DECIMAL": "Decimal",
    "NUMERIC": "Decimal",
    # Boolean
    "BOOLEAN": "Boolean",
    "BOOL": "Boolean",
    "BIT": "Boolean",
    # String / text
    "CHAR": "Characters",
    "VARCHAR": "Variable Characters",
    "STRING": "Variable Characters",
    "TEXT": "Long Text",
    "NCHAR": "Characters",
    "NVARCHAR": "Variable Characters",
    "NTEXT": "Long Text",
    # Date / time
    "DATE": "Date",
    "TIME": "Time",
    "DATETIME": "Date & Time",
    "TIMESTAMP": "Date & Time",
    "TIMESTAMPTZ": "Date & Time",
    "TIMESTAMP_NTZ": "Date & Time",
    # Binary / blob
    "BLOB": "Binary Large Object",
    "CLOB": "Character Large Object",
    "BINARY": "Binary",
    "VARBINARY": "Binary",
    # Other
    "UUID": "Identifier",
    "JSON": "Long Text",
    "JSONB": "Long Text",
    "SUPER": "Long Text",
    "VARIANT": "Long Text",
}


def _ldm_logical_type(raw_type: str) -> str:
    """Map a DDL/physical type string to a logical type name."""
    base = re.split(r"[\s(]", raw_type)[0].upper()
    return _LDM_TYPE_MAP.get(base, "Text")


def _ldm_id(counter: list) -> str:
    counter[0] += 1
    return f"o{counter[0]}"


# Cardinality labels for relationship ends
_CARDINALITY_ONE  = "1,1"   # mandatory one (parent side)
_CARDINALITY_MANY = "0,n"   # optional many (child / FK side)


def generate_erd_ldm(sql_text: str,
                     title: str = "Logical Data Model") -> dict:
    """
    Parse DDL SQL and produce a SAP PowerDesigner Logical Data Model (.ldm) XML.

    Key differences from PDM:
    - Elements are Entities / Attributes (not Tables / Columns)
    - Data types are abstract logical types (Integer, Text, Date…), not DB-physical ones
    - Relationships carry cardinality notation (1,1 → 0,n) instead of raw FK join specs
    - No database-target or engine-specific metadata
    - The file signature identifies the model as an LDM, not a PDM
    """

    parser = DDLParser()

    try:
        tables, foreign_keys = parser.parse(sql_text)
    except Exception as e:
        logger.error("DDL parse error: %s", e)
        return {
            "xml": None,
            "entity_count": 0,
            "relationship_count": 0,
            "error": str(e),
        }

    if not tables:
        return {
            "xml": None,
            "entity_count": 0,
            "relationship_count": 0,
            "error": "No tables found in SQL.",
        }

    ctr = [2]
    model_guid = _make_guid()
    now_ts = str(int(_time.time()))

    # Assign stable IDs for every entity, attribute, and identifier (PK)
    # Association + symbol IDs are allocated later after layout is computed
    entity_ids:    Dict[str, str]   = {}
    attr_ids:      Dict[Tuple, str] = {}
    ident_ids:     Dict[str, str]   = {}

    for tname in tables:
        entity_ids[tname] = _ldm_id(ctr)
        ident_ids[tname]  = _ldm_id(ctr)
        for col in tables[tname].columns:
            attr_ids[(tname, col.name)] = _ldm_id(ctr)

    # Pre-allocate one association ID per FK (symbol IDs added in layout block)
    rel_ids: List[Tuple] = []
    for fk in foreign_keys:
        rel_ids.append((fk, _ldm_id(ctr)))

    # ── XML skeleton ──────────────────────────────────────────────────────────
    root = ET.Element("Model")
    root.set("xmlns:a", "attribute")
    root.set("xmlns:c", "collection")
    root.set("xmlns:o", "object")

    root_obj  = _sub(root, "o:RootObject", Id="o1")
    children  = _sub(root_obj, "c:Children")
    model_el  = _sub(children, "o:Model", Id="o2")

    _sub(model_el, "a:ObjectID",         text=model_guid)
    _sub(model_el, "a:Name",             text=title)
    _sub(model_el, "a:Code",             text=re.sub(r"\W+", "_", title).upper())
    _sub(model_el, "a:CreationDate",     text=now_ts)
    _sub(model_el, "a:Creator",          text="erd_generator")
    _sub(model_el, "a:ModificationDate", text=now_ts)
    _sub(model_el, "a:Modifier",         text="erd_generator")

    # Layout constants for symbol positioning
    ENTITY_W     = 160
    ENTITY_H_HDR = 30    # header row height
    ENTITY_H_ROW = 20    # per-attribute row height
    H_GAP        = 100
    V_GAP        = 80
    COLS_PER_ROW = 3

    # Pre-compute each entity's canvas position so association links can
    # reference the correct source/target coordinates
    entity_positions: Dict[str, Tuple[int, int]] = {}
    for idx, tname in enumerate(tables):
        col_pos = idx % COLS_PER_ROW
        row_pos = idx // COLS_PER_ROW
        x = col_pos * (ENTITY_W + H_GAP) + 20
        n_attrs  = len(tables[tname].columns)
        row_h    = ENTITY_H_HDR + n_attrs * ENTITY_H_ROW + 10
        y = row_pos * (row_h + V_GAP) + 20
        entity_positions[tname] = (x, y)

    # Symbol IDs — one per entity, one per association link
    entity_sym_ids: Dict[str, str] = {}
    assoc_sym_ids:  List[Tuple]    = []   # (fk, sym_id)

    for tname in tables:
        entity_sym_ids[tname] = _ldm_id(ctr)
    for fk, rid in rel_ids:
        assoc_sym_ids.append((fk, rid, _ldm_id(ctr)))

    # ── Diagram (with Symbols populated) ─────────────────────────────────────
    diag_coll = _sub(model_el, "c:LogicalDiagrams")
    diag_el   = _sub(diag_coll, "o:LogicalDiagram", Id=_ldm_id(ctr))
    _sub(diag_el, "a:ObjectID", text=_make_guid())
    _sub(diag_el, "a:Name",     text="DefaultDiagram")

    symbols_coll = _sub(diag_el, "c:Symbols")

    # Entity symbols — each needs a position/size so PD renders it on canvas
    for tname, table in tables.items():
        x, y    = entity_positions[tname]
        n_attrs = len(table.columns)
        h       = ENTITY_H_HDR + n_attrs * ENTITY_H_ROW + 10

        sym_el = _sub(symbols_coll, "o:EntitySymbol", Id=entity_sym_ids[tname])
        _sub(sym_el, "a:Rect", text=f"{x} {y} {x + ENTITY_W} {y + h}")
        _sub(sym_el, "a:GrabHandles", text="8")
        _sub(sym_el, "a:AutomaticSize", text="1")
        # Link symbol back to the model entity
        ET.SubElement(_sub(sym_el, "c:Object"), "o:Entity").set(
            "Ref", entity_ids[tname]
        )

    # Association link symbols — connect two entity symbols visually
    for fk, rid, sym_id in assoc_sym_ids:
        if fk.from_table not in tables or fk.to_table not in tables:
            continue

        lnk_el = _sub(symbols_coll, "o:AssociationSymbol", Id=sym_id)

        # Source = parent entity symbol (the "1" side)
        ET.SubElement(
            _sub(lnk_el, "c:SourceSymbol"),
            "o:EntitySymbol"
        ).set("Ref", entity_sym_ids[fk.to_table])

        # Destination = child entity symbol (the "many" side)
        ET.SubElement(
            _sub(lnk_el, "c:DestinationSymbol"),
            "o:EntitySymbol"
        ).set("Ref", entity_sym_ids[fk.from_table])

        # Link back to the association object defined below
        ET.SubElement(
            _sub(lnk_el, "c:Object"),
            "o:Association"
        ).set("Ref", rid)

    # ── Entities ──────────────────────────────────────────────────────────────
    entities_coll = _sub(model_el, "c:Entities")

    for tname, table in tables.items():
        eid = entity_ids[tname]

        e_el = _sub(entities_coll, "o:Entity", Id=eid)
        _sub(e_el, "a:ObjectID",         text=_make_guid())
        _sub(e_el, "a:Name",             text=tname)
        _sub(e_el, "a:Code",             text=tname)
        _sub(e_el, "a:CreationDate",     text=now_ts)
        _sub(e_el, "a:Creator",          text="erd_generator")
        _sub(e_el, "a:ModificationDate", text=now_ts)
        _sub(e_el, "a:Modifier",         text="erd_generator")

        # Attributes
        attrs_coll  = _sub(e_el, "c:Attributes")
        pk_attr_ids = []

        for col in table.columns:
            aid          = attr_ids[(tname, col.name)]
            logical_type = _ldm_logical_type(col.data_type)

            a_el = _sub(attrs_coll, "o:EntityAttribute", Id=aid)
            _sub(a_el, "a:ObjectID",         text=_make_guid())
            _sub(a_el, "a:Name",             text=col.name)
            _sub(a_el, "a:Code",             text=col.name)
            _sub(a_el, "a:CreationDate",     text=now_ts)
            _sub(a_el, "a:Creator",          text="erd_generator")
            _sub(a_el, "a:ModificationDate", text=now_ts)
            _sub(a_el, "a:Modifier",         text="erd_generator")
            _sub(a_el, "a:DataType",         text=logical_type)
            _sub(a_el, "a:Mandatory",
                 text="1" if (not col.is_nullable or col.is_primary_key) else "0")

            if col.is_primary_key:
                pk_attr_ids.append(aid)

        # Primary Identifier
        if pk_attr_ids or table.primary_keys:
            iid = ident_ids[tname]

            idents_coll = _sub(e_el, "c:Identifiers")
            ident_el    = _sub(idents_coll, "o:Identifier", Id=iid)

            _sub(ident_el, "a:ObjectID",          text=_make_guid())
            _sub(ident_el, "a:Name",              text=f"PI_{tname}")
            _sub(ident_el, "a:Code",              text=f"PI_{tname}")
            _sub(ident_el, "a:CreationDate",      text=now_ts)
            _sub(ident_el, "a:Creator",           text="erd_generator")
            _sub(ident_el, "a:ModificationDate",  text=now_ts)
            _sub(ident_el, "a:Modifier",          text="erd_generator")
            _sub(ident_el, "a:PrimaryIdentifier", text="1")

            ia_coll = _sub(ident_el, "c:Identifier.Attributes")
            for aid in pk_attr_ids:
                ET.SubElement(ia_coll, "o:EntityAttribute").set("Ref", aid)

            pi_ref = _sub(e_el, "c:PrimaryIdentifier")
            ET.SubElement(pi_ref, "o:Identifier").set("Ref", iid)

    # ── Associations (correct PD LDM collection name) ─────────────────────────
    # PowerDesigner LDM uses c:Associations / o:Association — NOT c:Relationships
    if rel_ids:
        assocs_coll = _sub(model_el, "c:Associations")

        for fk, rid, _sym_id in assoc_sym_ids:
            if fk.from_table not in tables or fk.to_table not in tables:
                continue

            a_el     = _sub(assocs_coll, "o:Association", Id=rid)
            rel_name = f"{fk.to_table}_has_{fk.from_table}"
            rel_code = f"R_{fk.from_table}_{fk.from_col}"

            _sub(a_el, "a:ObjectID",         text=_make_guid())
            _sub(a_el, "a:Name",             text=rel_name)
            _sub(a_el, "a:Code",             text=rel_code)
            _sub(a_el, "a:CreationDate",     text=now_ts)
            _sub(a_el, "a:Creator",          text="erd_generator")
            _sub(a_el, "a:ModificationDate", text=now_ts)
            _sub(a_el, "a:Modifier",         text="erd_generator")

            # ── Association ends ──────────────────────────────────────────────
            # PowerDesigner models each end of an association as o:AssociationEnd
            # End1 = parent / "one" side   (referenced entity, e.g. Customer)
            # End2 = child  / "many" side  (referencing entity, e.g. Orders)
            ends_coll = _sub(a_el, "c:AssociationEnds")

            # End 1 — parent (multiplicity 1)
            end1_id = _ldm_id(ctr)
            end1_el = _sub(ends_coll, "o:AssociationEnd", Id=end1_id)
            _sub(end1_el, "a:ObjectID",      text=_make_guid())
            _sub(end1_el, "a:Name",          text=fk.to_table)
            _sub(end1_el, "a:Code",          text=fk.to_table)
            _sub(end1_el, "a:CreationDate",  text=now_ts)
            _sub(end1_el, "a:Creator",       text="erd_generator")
            _sub(end1_el, "a:ModificationDate", text=now_ts)
            _sub(end1_el, "a:Modifier",      text="erd_generator")
            _sub(end1_el, "a:Multiplicity",  text="1")       # exactly one
            _sub(end1_el, "a:IsNavigable",   text="1")
            ET.SubElement(
                _sub(end1_el, "c:Entity"),
                "o:Entity"
            ).set("Ref", entity_ids[fk.to_table])

            # End 2 — child (multiplicity 0..*)
            end2_id = _ldm_id(ctr)
            end2_el = _sub(ends_coll, "o:AssociationEnd", Id=end2_id)
            _sub(end2_el, "a:ObjectID",      text=_make_guid())
            _sub(end2_el, "a:Name",          text=fk.from_table)
            _sub(end2_el, "a:Code",          text=fk.from_table)
            _sub(end2_el, "a:CreationDate",  text=now_ts)
            _sub(end2_el, "a:Creator",       text="erd_generator")
            _sub(end2_el, "a:ModificationDate", text=now_ts)
            _sub(end2_el, "a:Modifier",      text="erd_generator")
            _sub(end2_el, "a:Multiplicity",  text="0..*")    # zero-to-many
            _sub(end2_el, "a:IsNavigable",   text="1")
            ET.SubElement(
                _sub(end2_el, "c:Entity"),
                "o:Entity"
            ).set("Ref", entity_ids[fk.from_table])

    # ── Serialise ─────────────────────────────────────────────────────────────
    total_symbols = len(tables) + len([x for x in assoc_sym_ids
                                       if x[0].from_table in tables
                                       and x[0].to_table in tables])
    total_objects = len(tables) + len(foreign_keys)

    raw        = ET.tostring(root, encoding="unicode", xml_declaration=False)
    pretty     = minidom.parseString(raw).toprettyxml(indent="  ", encoding=None)
    body_lines = pretty.splitlines()[1:]

    header = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<?PowerDesigner AppLocale="UTF-8" ID="{{{model_guid}}}" '
        f'LastModificationDate="{now_ts}" Name="{title}" '
        f'Objects="{total_objects}" '
        f'Symbols="{total_symbols}" '
        'Type="{CDE44E21-9669-11D1-9915-006097355D9B}" '
        'signature="LDM_DATA_MODEL_XML" version="16.7.0.5765"?>\n'
        '<!-- do not edit this file -->\n'
    )

    return {
        "xml": header + "\n".join(body_lines),
        "entity_count": len(tables),
        "relationship_count": len(foreign_keys),
        "error": None,
    }