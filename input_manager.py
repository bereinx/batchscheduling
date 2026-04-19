from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any
from zipfile import ZipFile
import xml.etree.ElementTree as ET

import pandas as pd

try:
    from .sample_data import (
        make_blendstocks_example,
        make_two_month_orders_example,
        make_two_month_settings_example,
        make_two_month_tanks_example,
    )
except ImportError:
    from sample_data import (
        make_blendstocks_example,
        make_two_month_orders_example,
        make_two_month_settings_example,
        make_two_month_tanks_example,
    )


@dataclass(frozen=True)
class InputBundle:
    settings: dict[str, Any]
    orders_df: pd.DataFrame
    blendstocks_df: pd.DataFrame
    tanks_df: pd.DataFrame


SETTINGS_SCHEMA: list[dict[str, str]] = [
    {"parameter": "schedule_start", "group": "Planning", "description": "ISO datetime for scheduling start"},
    {"parameter": "schedule_end", "group": "Planning", "description": "ISO datetime for scheduling end"},
    {"parameter": "horizon_days", "group": "Planning", "description": "Total planning horizon in days"},
    {"parameter": "batch_hours", "group": "Operations", "description": "Blend duration per batch"},
    {"parameter": "qc_hours", "group": "Operations", "description": "QC hold time after each batch"},
    {"parameter": "batches_per_order", "group": "Operations", "description": "Total batches split for each order"},
    {"parameter": "tanks_per_order", "group": "Operations", "description": "Number of product tanks used for each order"},
    {"parameter": "deadstock_fraction", "group": "Operations", "description": "Residual heel fraction in each product tank"},
    {"parameter": "interim_relax_pct", "group": "Quality", "description": "Allowed intermediate off-spec buffer"},
    {"parameter": "solver_time_limit_sec", "group": "Solver", "description": "MILP solver time limit"},
    {"parameter": "mip_gap_rel", "group": "Solver", "description": "Relative optimality gap"},
    {"parameter": "base_spec_penalty", "group": "Penalty", "description": "Base quality slack penalty"},
    {"parameter": "key_spec_multiplier", "group": "Penalty", "description": "Weight multiplier for RON/RVP/Sulfur"},
    {"parameter": "hard_slack_penalty", "group": "Penalty", "description": "Tank capacity slack penalty"},
    {"parameter": "inventory_slack_penalty", "group": "Penalty", "description": "Inventory slack penalty"},
    {"parameter": "domestic_late_penalty", "group": "Penalty", "description": "Penalty for domestic lateness"},
]

NUMERIC_SETTINGS = {
    "horizon_days",
    "batch_hours",
    "qc_hours",
    "batches_per_order",
    "tanks_per_order",
    "deadstock_fraction",
    "interim_relax_pct",
    "solver_time_limit_sec",
    "mip_gap_rel",
    "base_spec_penalty",
    "key_spec_multiplier",
    "hard_slack_penalty",
    "inventory_slack_penalty",
    "domestic_late_penalty",
}

DATETIME_SETTINGS = {"schedule_start", "schedule_end"}

SHEET_ORDER = ["Settings", "Orders", "Blendstocks", "Tanks"]
XML_NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pkgrel": "http://schemas.openxmlformats.org/package/2006/relationships",
}


def make_example_bundle() -> InputBundle:
    return InputBundle(
        settings=make_two_month_settings_example(),
        orders_df=make_two_month_orders_example(),
        blendstocks_df=make_blendstocks_example(),
        tanks_df=make_two_month_tanks_example(),
    )


def bundle_to_session_state(bundle: InputBundle, session_state: Any) -> None:
    session_state["settings"] = bundle.settings.copy()
    session_state["orders_df"] = bundle.orders_df.copy()
    session_state["blendstocks_df"] = bundle.blendstocks_df.copy()
    session_state["tanks_df"] = bundle.tanks_df.copy()
    session_state["solve_result"] = None
    session_state["selected_batch_id"] = None


def settings_to_frame(settings: dict[str, Any]) -> pd.DataFrame:
    metadata = {row["parameter"]: row for row in SETTINGS_SCHEMA}
    rows = []
    for parameter, value in settings.items():
        item = metadata.get(parameter, {"group": "Other", "description": ""})
        rows.append(
            {
                "parameter": parameter,
                "value": value,
                "group": item["group"],
                "description": item["description"],
            }
        )
    return pd.DataFrame(rows)


def settings_from_frame(frame: pd.DataFrame, base_settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = (base_settings or make_two_month_settings_example()).copy()
    working = frame.copy()
    working.columns = [str(column).strip() for column in working.columns]
    if "parameter" not in working.columns or "value" not in working.columns:
        raise ValueError("Settings 시트에는 'parameter'와 'value' 컬럼이 필요합니다.")

    for row in working.dropna(subset=["parameter"]).itertuples():
        parameter = str(row.parameter).strip()
        if not parameter:
            continue
        value = row.value
        if parameter in NUMERIC_SETTINGS:
            numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
            if pd.isna(numeric):
                raise ValueError(f"Settings 시트의 '{parameter}' 값이 숫자가 아닙니다.")
            if parameter in {"horizon_days", "batch_hours", "qc_hours", "batches_per_order", "tanks_per_order", "solver_time_limit_sec"}:
                value = int(round(float(numeric)))
            else:
                value = float(numeric)
        elif parameter in DATETIME_SETTINGS:
            value = pd.Timestamp(value).isoformat()
        settings[parameter] = value

    return settings


def load_bundle_from_excel(file_like: Any) -> InputBundle:
    workbook = _read_xlsx_tables(file_like)
    missing = [sheet for sheet in SHEET_ORDER if sheet not in workbook]
    if missing:
        raise ValueError(f"엑셀 업로드 파일에 다음 시트가 없습니다: {', '.join(missing)}")

    settings = settings_from_frame(workbook["Settings"])
    orders_df = workbook["Orders"].copy()
    blendstocks_df = workbook["Blendstocks"].copy()
    tanks_df = workbook["Tanks"].copy()

    if "due_at" in orders_df.columns:
        orders_df["due_at"] = pd.to_datetime(orders_df["due_at"], errors="coerce")
    for column in ["market_type", "sulfur_class", "grade_name", "region", "order_id"]:
        if column in orders_df.columns:
            orders_df[column] = orders_df[column].astype(str).str.strip()
    if "service" in tanks_df.columns:
        tanks_df["service"] = tanks_df["service"].astype(str).str.strip()

    return InputBundle(
        settings=settings,
        orders_df=orders_df,
        blendstocks_df=blendstocks_df,
        tanks_df=tanks_df,
    )


def input_snapshot(bundle: InputBundle) -> pd.DataFrame:
    orders = bundle.orders_df.copy()
    market_counts = (
        orders.groupby(["market_type", "sulfur_class"]).size().reset_index(name="count")
        if not orders.empty and {"market_type", "sulfur_class"}.issubset(orders.columns)
        else pd.DataFrame(columns=["market_type", "sulfur_class", "count"])
    )
    rows = [
        {"Item": "Orders", "Value": len(bundle.orders_df)},
        {"Item": "Blendstocks", "Value": len(bundle.blendstocks_df)},
        {"Item": "Product Tanks", "Value": len(bundle.tanks_df)},
        {"Item": "Schedule Start", "Value": pd.Timestamp(bundle.settings["schedule_start"]).strftime("%Y-%m-%d")},
        {"Item": "Schedule End", "Value": pd.Timestamp(bundle.settings["schedule_end"]).strftime("%Y-%m-%d")},
        {"Item": "Horizon Days", "Value": bundle.settings["horizon_days"]},
    ]
    for row in market_counts.itertuples():
        rows.append({"Item": f"{row.market_type}_{row.sulfur_class}", "Value": int(row.count)})
    return pd.DataFrame(rows)


def template_output_path() -> Path:
    return Path(__file__).resolve().parent / "outputs" / "batch_scheduling_input_template.xlsx"


def _read_xlsx_tables(file_like: Any) -> dict[str, pd.DataFrame]:
    if hasattr(file_like, "getvalue"):
        raw = file_like.getvalue()
    elif hasattr(file_like, "read"):
        raw = file_like.read()
    else:
        raw = Path(file_like).read_bytes()

    with ZipFile(BytesIO(raw)) as archive:
        shared_strings = _read_shared_strings(archive)
        sheet_paths = _read_sheet_paths(archive)
        tables: dict[str, pd.DataFrame] = {}
        for sheet_name, sheet_path in sheet_paths.items():
            rows = _read_sheet_rows(archive, sheet_path, shared_strings)
            if not rows:
                tables[sheet_name] = pd.DataFrame()
                continue
            header = [str(value).strip() if value is not None else "" for value in rows[0]]
            data_rows = rows[1:]
            tables[sheet_name] = pd.DataFrame(data_rows, columns=header)
        return tables


def _read_shared_strings(archive: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for item in root.findall("main:si", XML_NS):
        text_parts = [node.text or "" for node in item.findall(".//main:t", XML_NS)]
        strings.append("".join(text_parts))
    return strings


def _read_sheet_paths(archive: ZipFile) -> dict[str, str]:
    workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
    rels_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels_root.findall("pkgrel:Relationship", XML_NS)
    }
    sheet_paths: dict[str, str] = {}
    for sheet in workbook_root.findall("main:sheets/main:sheet", XML_NS):
        rel_id = sheet.attrib.get(f"{{{XML_NS['rel']}}}id")
        target = rel_map.get(rel_id, "")
        if not target:
            continue
        normalized = target.lstrip("/")
        if not normalized.startswith("xl/"):
            normalized = f"xl/{normalized}"
        sheet_paths[sheet.attrib["name"]] = normalized
    return sheet_paths


def _read_sheet_rows(archive: ZipFile, sheet_path: str, shared_strings: list[str]) -> list[list[Any]]:
    root = ET.fromstring(archive.read(sheet_path))
    rows: list[list[Any]] = []
    for row in root.findall(".//main:sheetData/main:row", XML_NS):
        values: dict[int, Any] = {}
        for cell in row.findall("main:c", XML_NS):
            ref = cell.attrib.get("r", "")
            column_index = _column_index_from_ref(ref)
            values[column_index] = _read_cell_value(cell, shared_strings)
        if not values:
            continue
        max_col = max(values)
        rows.append([values.get(idx) for idx in range(max_col + 1)])
    return rows


def _column_index_from_ref(ref: str) -> int:
    letters = "".join(char for char in ref if char.isalpha()).upper()
    index = 0
    for char in letters:
        index = index * 26 + (ord(char) - 64)
    return max(index - 1, 0)


def _read_cell_value(cell: ET.Element, shared_strings: list[str]) -> Any:
    cell_type = cell.attrib.get("t")
    value_node = cell.find("main:v", XML_NS)
    inline_node = cell.find("main:is/main:t", XML_NS)

    if cell_type == "inlineStr" and inline_node is not None:
        return inline_node.text or ""
    if value_node is None:
        return ""

    raw = value_node.text or ""
    if cell_type == "s":
        try:
            return shared_strings[int(raw)]
        except (ValueError, IndexError):
            return raw
    if cell_type == "b":
        return raw == "1"
    numeric = pd.to_numeric(pd.Series([raw]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return raw
    if float(numeric).is_integer():
        return int(numeric)
    return float(numeric)
