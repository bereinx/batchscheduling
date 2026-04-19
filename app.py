from __future__ import annotations

import traceback
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from input_manager import (
    InputBundle,
    bundle_to_session_state,
    input_snapshot,
    load_bundle_from_excel,
    make_example_bundle,
    settings_from_frame,
    settings_to_frame,
    template_output_path,
)
from optimizer import solve_batch_schedule


st.set_page_config(page_title="Batch Scheduling Optimizer", page_icon="⛽", layout="wide")


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(239, 125, 54, 0.12), transparent 24%),
                radial-gradient(circle at top right, rgba(28, 121, 184, 0.14), transparent 28%),
                linear-gradient(180deg, #f6f9fc 0%, #edf3f9 100%);
        }
        section[data-testid="stSidebar"] {
            background: #e4edf2;
            border-right: 1px solid rgba(109, 133, 150, 0.16);
        }
        section[data-testid="stSidebar"] * {
            color: #1c2c3a;
            font-size: 0.90rem;
        }
        section[data-testid="stSidebar"] .stRadio label,
        section[data-testid="stSidebar"] .stButton button,
        section[data-testid="stSidebar"] .stDataFrame,
        section[data-testid="stSidebar"] .stAlert {
            font-size: 0.86rem !important;
        }
        section[data-testid="stSidebar"] .stRadio > div {
            background: rgba(255, 255, 255, 0.28);
            border: 1px solid rgba(115, 140, 160, 0.12);
            border-radius: 10px;
            padding: 4px 6px;
        }
        section[data-testid="stSidebar"] .stButton button {
            background: #c7d7e1;
            color: #173042;
            border: 1px solid rgba(93, 122, 143, 0.14);
            border-radius: 10px;
            box-shadow: none;
            min-height: 2.6rem;
        }
        section[data-testid="stSidebar"] .stButton button[kind="primary"] {
            background: #b8ccd8;
            color: #102536;
            border: 1px solid rgba(79, 110, 132, 0.16);
            box-shadow: none;
        }
        section[data-testid="stSidebar"] [data-testid="stFileUploader"] {
            background: rgba(255, 255, 255, 0.30);
            border: 1px solid rgba(115, 140, 160, 0.10);
            border-radius: 10px;
            padding: 0.35rem 0.45rem 0.25rem 0.45rem;
        }
        section[data-testid="stSidebar"] [data-testid="stFileUploader"] section {
            border: 1px dashed rgba(93, 122, 143, 0.22);
            border-radius: 8px;
            background: rgba(255, 255, 255, 0.14);
        }
        section[data-testid="stSidebar"] .sidebar-block-label {
            font-weight: 700;
            font-size: 0.9rem;
            margin: 0.15rem 0 0.35rem 0;
            color: #173042;
        }
        section[data-testid="stSidebar"] .stButton button:hover {
            background: #c0d2dd;
            border-color: rgba(66, 96, 118, 0.18);
        }
        section[data-testid="stSidebar"] [data-testid="stDataFrame"] {
            background: rgba(255, 255, 255, 0.32);
            border: 1px solid rgba(115, 140, 160, 0.10);
            border-radius: 10px;
            overflow: hidden;
        }
        section[data-testid="stSidebar"] .stAlert {
            background: rgba(255, 255, 255, 0.26);
            border: 1px solid rgba(115, 140, 160, 0.10);
            border-radius: 10px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def init_state() -> None:
    if "orders_df" not in st.session_state or "blendstocks_df" not in st.session_state or "tanks_df" not in st.session_state or "settings" not in st.session_state:
        bundle_to_session_state(make_example_bundle(), st.session_state)
    if "solve_result" not in st.session_state:
        st.session_state["solve_result"] = None
    if "page_name" not in st.session_state:
        st.session_state["page_name"] = "Planning"
    if "selected_batch_id" not in st.session_state:
        st.session_state["selected_batch_id"] = None
    _sync_order_due_dates_with_settings()


def reset_example_case() -> None:
    bundle_to_session_state(make_example_bundle(), st.session_state)
    _sync_order_due_dates_with_settings(force_reset=True)


def _settings_start_ts(settings: dict) -> pd.Timestamp:
    return pd.Timestamp(settings["schedule_start"])


def _sync_order_due_dates_with_settings(force_reset: bool = False) -> None:
    orders_df = st.session_state["orders_df"].copy()
    settings = st.session_state["settings"]
    start_ts = _settings_start_ts(settings)
    if force_reset or "due_at" not in orders_df.columns:
        if "due_day" in orders_df.columns:
            due_hours = pd.to_numeric(orders_df.get("due_hour", 18), errors="coerce").fillna(18.0)
            due_days = pd.to_numeric(orders_df["due_day"], errors="coerce").fillna(1.0)
            orders_df["due_at"] = [
                start_ts + pd.Timedelta(days=float(due_day) - 1.0, hours=float(due_hour))
                for due_day, due_hour in zip(due_days, due_hours)
            ]
    st.session_state["orders_df"] = orders_df


def settings_editor() -> dict:
    settings_frame = settings_to_frame(st.session_state["settings"])[["group", "parameter", "value", "description"]]
    edited_settings = st.data_editor(
        settings_frame,
        width="stretch",
        hide_index=True,
        num_rows="fixed",
        column_config={
            "group": st.column_config.TextColumn("Group", disabled=True),
            "parameter": st.column_config.TextColumn("Parameter", disabled=True),
            "value": st.column_config.TextColumn("Value", required=True),
            "description": st.column_config.TextColumn("Description", disabled=True, width="large"),
        },
    )
    settings = settings_from_frame(edited_settings[["parameter", "value"]], st.session_state["settings"])
    try:
        schedule_start_date = pd.Timestamp(settings["schedule_start"]).date()
        schedule_end_date = pd.Timestamp(settings["schedule_end"]).date()
        settings["horizon_days"] = (schedule_end_date - schedule_start_date).days + 1
        settings["schedule_start"] = datetime.combine(schedule_start_date, time(0, 0)).isoformat()
        settings["schedule_end"] = datetime.combine(schedule_end_date, time(23, 59)).isoformat()
    except Exception:
        pass
    st.session_state["settings"] = settings
    return settings


def _hex_to_rgb(color: str) -> tuple[int, int, int]:
    color = color.lstrip("#")
    return int(color[0:2], 16), int(color[2:4], 16), int(color[4:6], 16)


def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
    return "#{:02x}{:02x}{:02x}".format(*rgb)


def _mix_color(color: str, mix_with: str, ratio: float) -> str:
    base = _hex_to_rgb(color)
    target = _hex_to_rgb(mix_with)
    mixed = tuple(int(round(base[idx] * (1.0 - ratio) + target[idx] * ratio)) for idx in range(3))
    return _rgb_to_hex(mixed)


def _batch_chart_df(batch_schedule_df: pd.DataFrame) -> pd.DataFrame:
    chart_df = batch_schedule_df.copy()
    palette = ["#1f77b4", "#d62728", "#2ca02c", "#ff7f0e", "#8c564b", "#17becf", "#bcbd22", "#e377c2", "#7f7f7f", "#9467bd", "#4c78a8", "#f58518"]
    tank_colors = {
        tank_id: palette[idx % len(palette)]
        for idx, tank_id in enumerate(sorted(chart_df["tank_id"].dropna().unique().tolist()))
    }
    chart_df["tank_color"] = chart_df["tank_id"].map(tank_colors)
    chart_df["tank_tone"] = chart_df["tank_group"].map({"T1": "Tank A", "T2": "Tank B"}).fillna(chart_df["tank_group"])
    chart_df["batch_label"] = chart_df["display_name"] + " " + chart_df["tank_group"] + "-B" + chart_df["stage_in_tank"].astype(str)
    name_parts = chart_df["display_name"].fillna("").str.split("_")
    chart_df["product_short"] = name_parts.apply(
        lambda parts: f"{parts[0]}{parts[1].replace('R', '')}" if len(parts) >= 2 else ""
    )
    return chart_df


def _extract_altair_batch_selection(event: Any) -> str | None:
    if hasattr(event, "selection"):
        selection = event.selection
        if hasattr(selection, "batch_pick"):
            rows = getattr(selection, "batch_pick")
            if rows:
                row = rows[0]
                if isinstance(row, dict):
                    return row.get("batch_id")
    if isinstance(event, dict):
        selection = event.get("selection", {})
        rows = selection.get("batch_pick", [])
        if rows:
            row = rows[0]
            if isinstance(row, dict):
                return row.get("batch_id")
    return None


def draw_campaign_chart(batch_schedule_df: pd.DataFrame) -> alt.Chart:
    chart_df = _batch_chart_df(batch_schedule_df)
    selection = alt.selection_point(fields=["batch_id"], name="batch_pick", on="click", clear=False)
    return (
        alt.Chart(chart_df)
        .mark_bar(size=18, cornerRadius=4)
        .encode(
            x=alt.X("start_at:T", title="Schedule Time", axis=alt.Axis(format="%m-%d %H:%M")),
            x2="finish_at:T",
            y=alt.Y(
                "blender:N",
                sort=["DOMESTIC_LS", "EXPORT_LS", "EXPORT_HS"],
                title="Blender Line",
            ),
            color=alt.Color("tank_color:N", scale=None, legend=None),
            stroke=alt.condition(selection, alt.value("#12263a"), alt.value(None)),
            strokeWidth=alt.condition(selection, alt.value(2), alt.value(0)),
            opacity=alt.condition(selection, alt.value(1.0), alt.value(0.92)),
            tooltip=["product_short", "display_name", "tank_id", "start_label", "finish_label", "due_label"],
        )
        .add_params(selection)
        .properties(height=320, title="Campaign Gantt")
    )


def draw_inventory_chart(inventory_profile_df: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(inventory_profile_df)
        .mark_line(point=False)
        .encode(
            x=alt.X("snapshot_at:T", title="Time", axis=alt.Axis(format="%m-%d %H:%M")),
            y=alt.Y("inventory_m3:Q", title="Inventory (m3)"),
            color=alt.Color("component:N", title="Blendstock"),
            tooltip=["component", "snapshot_label", "inventory_m3"],
        )
        .properties(height=360, title="Blendstock Inventory Profile")
    )


def draw_tank_level_chart(tank_level_profile_df: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(tank_level_profile_df)
        .mark_line(point=False)
        .encode(
            x=alt.X("snapshot_at:T", title="Time", axis=alt.Axis(format="%m-%d %H:%M")),
            y=alt.Y("fill_pct:Q", title="Tank Fill (%)"),
            color=alt.Color("tank_id:N", title="Tank"),
            tooltip=["tank_id", "service", "snapshot_label", "level_m3", "fill_pct"],
        )
        .properties(height=360, title="Tank Level Profile")
    )


def render_sidebar() -> None:
    with st.sidebar:
        st.markdown('<div class="sidebar-block-label">입력값 불러오기</div>', unsafe_allow_html=True)
        uploaded_file = st.file_uploader(
            "입력값 불러오기",
            type=["xlsx", "xls"],
            accept_multiple_files=False,
            help="Settings, Orders, Blendstocks, Tanks 시트가 포함된 엑셀 파일을 불러옵니다.",
            label_visibility="collapsed",
        )
        if uploaded_file is not None:
            try:
                uploaded_bundle = load_bundle_from_excel(uploaded_file)
                bundle_to_session_state(uploaded_bundle, st.session_state)
                _sync_order_due_dates_with_settings(force_reset=True)
                st.success("입력값을 불러왔습니다.")
                st.rerun()
            except Exception as exc:
                st.error(f"입력값 불러오기에 실패했습니다: {exc}")

        st.markdown('<div class="sidebar-block-label">최적화 실행</div>', unsafe_allow_html=True)
        if st.button("최적화 실행", type="primary", use_container_width=True):
            with st.spinner("Batch scheduling MILP를 풀고 있습니다..."):
                try:
                    st.session_state["solve_result"] = solve_batch_schedule(
                        st.session_state["orders_df"],
                        st.session_state["blendstocks_df"],
                        st.session_state["tanks_df"],
                        st.session_state["settings"],
                    )
                    result = st.session_state["solve_result"]
                    if not result.batch_schedule_df.empty:
                        st.session_state["selected_batch_id"] = result.batch_schedule_df.iloc[0]["batch_id"]
                except Exception as exc:
                    st.session_state["solve_result"] = None
                    st.error(f"모델 실행 중 오류가 발생했습니다: {exc}")
                    st.code(traceback.format_exc(), language="text")

        st.markdown("### Final Result")
        result = st.session_state["solve_result"]
        if result is None or result.status not in {"Optimal", "Feasible"}:
            st.info("아직 계산 결과가 없습니다.")
        else:
            summary_df = pd.DataFrame(
                [
                    {"Metric": "Objective", "Value": f"{result.objective_value:,.0f}"},
                    {"Metric": "Blend Cost", "Value": f"{result.blend_cost:,.0f}"},
                    {"Metric": "Demurrage", "Value": f"{result.demurrage_cost:,.0f}"},
                    {"Metric": "Domestic Late", "Value": f"{result.domestic_late_cost:,.0f}"},
                    {"Metric": "Carryover", "Value": f"{result.carryover_penalty_cost:,.0f}"},
                    {"Metric": "Spec Slack", "Value": f"{result.spec_slack_cost:,.0f}"},
                    {"Metric": "Inventory Slack", "Value": f"{result.inventory_slack_cost:,.0f}"},
                    {"Metric": "Tank Slack", "Value": f"{result.tank_capacity_slack_cost:,.0f}"},
                ]
            )
            st.dataframe(summary_df, width="stretch", hide_index=True)
            timing_df = pd.DataFrame(
                [
                    {"Stage": "Total", "Seconds": f"{result.total_solve_sec:.2f}"},
                    {"Stage": "Input sanitize", "Seconds": f"{result.sanitize_sec:.2f}"},
                    {"Stage": "MILP solve", "Seconds": f"{result.milp_sec:.2f}"},
                    {"Stage": "Local resequence", "Seconds": f"{result.resequence_sec:.2f}"},
                    {"Stage": "QC reschedule", "Seconds": f"{result.qc_reschedule_sec:.2f}"},
                    {"Stage": "Deadstock postprocess", "Seconds": f"{result.deadstock_sec:.2f}"},
                ]
            )
            st.dataframe(timing_df, width="stretch", hide_index=True)
            st.caption(result.solver_message)


def render_planning_page() -> None:
    prior_start = st.session_state["settings"].get("schedule_start")
    prior_end = st.session_state["settings"].get("schedule_end")
    bundle = InputBundle(
        settings=st.session_state["settings"].copy(),
        orders_df=st.session_state["orders_df"].copy(),
        blendstocks_df=st.session_state["blendstocks_df"].copy(),
        tanks_df=st.session_state["tanks_df"].copy(),
    )
    template_path = template_output_path()

    input_tab, settings_tab, orders_tab, blendstocks_tab, tanks_tab, notes_tab = st.tabs(["Input Center", "General Settings", "Sales Orders", "Blendstocks", "Product Tanks", "Notes"])

    with input_tab:
        left_col, right_col = st.columns([1.0, 1.1])
        with left_col:
            st.markdown("**Current Input Snapshot**")
            st.dataframe(input_snapshot(bundle), width="stretch", hide_index=True)
            st.markdown("**Settings Table**")
            st.dataframe(settings_to_frame(st.session_state["settings"]), width="stretch", hide_index=True)
        with right_col:
            st.markdown("**Excel Import / Export**")
            if st.button("예시 케이스 다시 불러오기", use_container_width=True):
                reset_example_case()
                st.rerun()
            if template_path.exists():
                st.download_button(
                    "입력 템플릿 다운로드",
                    data=template_path.read_bytes(),
                    file_name=template_path.name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            else:
                st.info("입력 템플릿 생성 중입니다. 잠시 뒤 새로고침해 주세요.")

            uploaded_file = st.file_uploader(
                "엑셀 입력 업로드",
                type=["xlsx", "xls"],
                accept_multiple_files=False,
                help="Settings / Orders / Blendstocks / Tanks 시트를 모두 포함한 파일을 업로드해 주세요.",
            )
            if uploaded_file is not None:
                try:
                    uploaded_bundle = load_bundle_from_excel(uploaded_file)
                    bundle_to_session_state(uploaded_bundle, st.session_state)
                    _sync_order_due_dates_with_settings(force_reset=True)
                    st.success("엑셀 입력값을 불러왔습니다. 아래 탭에서 바로 확인할 수 있습니다.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"엑셀 업로드를 처리하지 못했습니다: {exc}")
            st.caption("엑셀 업로드 후에는 Orders, Blendstocks, Product Tanks 탭의 편집 내용도 그대로 이어서 수정할 수 있습니다.")

    with settings_tab:
        st.markdown("**General Settings**")
        settings_editor()
        if prior_start != st.session_state["settings"].get("schedule_start") or prior_end != st.session_state["settings"].get("schedule_end"):
            _sync_order_due_dates_with_settings(force_reset=True)

    with orders_tab:
        orders_view = st.session_state["orders_df"].copy()
        if "due_day" in orders_view.columns:
            orders_view = orders_view.drop(columns=["due_day"])
        if "due_hour" in orders_view.columns:
            orders_view = orders_view.drop(columns=["due_hour"])
        edited_orders = st.data_editor(
            orders_view,
            width="stretch",
            num_rows="dynamic",
            hide_index=True,
            column_config={
                "market_type": st.column_config.SelectboxColumn(options=["DOMESTIC", "EXPORT"], required=True),
                "sulfur_class": st.column_config.SelectboxColumn(options=["LS", "HS"], required=True),
                "due_at": st.column_config.DatetimeColumn("Due Date", format="MM-DD HH:mm", required=True),
                "volume_m3": st.column_config.NumberColumn(min_value=100.0, step=50.0, required=True),
                "demurrage_per_hour": st.column_config.NumberColumn(min_value=0.0, step=10.0),
            },
        )
        st.session_state["orders_df"] = edited_orders

    with blendstocks_tab:
        st.session_state["blendstocks_df"] = st.data_editor(
            st.session_state["blendstocks_df"],
            width="stretch",
            num_rows="dynamic",
            hide_index=True,
        )

    with tanks_tab:
        st.session_state["tanks_df"] = st.data_editor(
            st.session_state["tanks_df"],
            width="stretch",
            num_rows="dynamic",
            hide_index=True,
            column_config={
                "service": st.column_config.SelectboxColumn(options=["EXPORT_LS", "EXPORT_HS", "DOMESTIC_LS", "DOMESTIC_HS"], required=True),
            },
        )

    with notes_tab:
        st.markdown(
            """
            - 현재 기본 예시 케이스는 2달 horizon 기준입니다.
            - 내수 `LS`는 2일마다 1건, 수출 `HS/LS`는 월 10건씩 들어오도록 예시가 구성되어 있습니다.
            - 최종 탱크 배치(stage 3)에서는 제품 spec을 맞추고, 이전 stage는 기본적으로 10% 완화된 범위를 허용합니다.
            - 제품 탱크에는 deadstock가 남는다고 보고, 현재 구현은 `마스터 스케줄 MILP + 로컬 탱크 재시퀀싱 + deadstock-aware 배합 LP 후처리` 휴리스틱입니다.
            - 재고, 탱크 용량, spec은 모두 slack variable로 soft handling 되어 infeasible을 줄입니다.
            """
        )


def render_batch_details(result, batch_id: str) -> None:
    batch_row = result.batch_schedule_df.loc[result.batch_schedule_df["batch_id"] == batch_id]
    quality_row = result.batch_quality_df.loc[result.batch_quality_df["batch_id"] == batch_id]
    recipe_df = result.batch_recipe_df.loc[result.batch_recipe_df["batch_id"] == batch_id]

    if batch_row.empty or quality_row.empty:
        st.info("배치를 선택하면 상세 정보가 표시됩니다.")
        return

    batch = batch_row.iloc[0]
    quality = quality_row.iloc[0]
    is_final_batch = "Final tank batch" in str(quality.get("rule", ""))

    def _fmt_prop(prop: str, value: object) -> str:
        if pd.isna(value):
            return "-"
        numeric = float(value)
        if prop == "Density":
            return f"{numeric:.3f}"
        return f"{numeric:.1f}"

    def _quality_warning(prop: str) -> bool:
        if prop == "RON":
            actual = float(quality["actual_ron"])
            target = float(quality["target_ron_min"])
            return actual < target or actual <= target * 1.01
        if prop == "RVP":
            actual = float(quality["actual_rvp"])
            target = float(quality["target_rvp_max"])
            return actual > target or actual >= target * 0.99
        if prop == "Density":
            actual = float(quality["actual_density"])
            low = float(quality["target_density_min"])
            high = float(quality["target_density_max"])
            return actual < low or actual > high or actual <= low * 1.01 or actual >= high * 0.99
        if prop == "Sulfur (ppm)":
            actual = float(quality["actual_sulfur_ppm"])
            target = float(quality["target_sulfur_max_ppm"])
            return actual > target or actual >= target * 0.99
        actual = float(quality["actual_olefin_pct"])
        target = float(quality["target_olefin_max_pct"])
        return actual > target or actual >= target * 0.99

    def _quality_breach(prop: str) -> bool:
        if prop == "RON":
            return float(quality["actual_ron"]) < float(quality["target_ron_min"])
        if prop == "RVP":
            return float(quality["actual_rvp"]) > float(quality["target_rvp_max"])
        if prop == "Density":
            actual = float(quality["actual_density"])
            return actual < float(quality["target_density_min"]) or actual > float(quality["target_density_max"])
        if prop == "Sulfur (ppm)":
            return float(quality["actual_sulfur_ppm"]) > float(quality["target_sulfur_max_ppm"])
        return float(quality["actual_olefin_pct"]) > float(quality["target_olefin_max_pct"])

    def _highlight_quality(row: pd.Series) -> list[str]:
        prop = str(row["Property"])
        if is_final_batch and _quality_breach(prop):
            return ["background-color: #f7c8c8"] * len(row)
        if _quality_warning(prop):
            return ["background-color: #fff4cc"] * len(row)
        return [""] * len(row)

    info_col, recipe_col, quality_col = st.columns([0.85, 1.3, 1.0])

    with info_col:
        st.markdown("**Batch Info**")
        info_df = pd.DataFrame(
            [
                {"Item": "Product", "Value": batch["display_name"]},
                {"Item": "Order", "Value": batch["order_id"]},
                {"Item": "Tank", "Value": batch["tank_id"]},
                {"Item": "Stage", "Value": f'{batch["tank_group"]} / Batch {batch["stage_in_tank"]}'},
                {"Item": "Volume", "Value": f'{batch["batch_volume_m3"]:,.1f} m3'},
                {"Item": "Start", "Value": batch["start_label"]},
                {"Item": "Finish", "Value": batch["finish_label"]},
                {"Item": "Due", "Value": batch["due_label"]},
                {"Item": "Rule", "Value": quality["rule"]},
            ]
        )
        st.dataframe(info_df, width="stretch", hide_index=True)

    with recipe_col:
        st.markdown("**Blend Recipe**")
        st.dataframe(
            recipe_df[
                [
                    "component",
                    "volume_m3",
                    "share_pct",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

    with quality_col:
        st.markdown("**Quality vs Target**")
        quality_df = pd.DataFrame(
            [
                {"Property": "RON", "Heel": _fmt_prop("RON", quality["heel_ron"]), "Blend": _fmt_prop("RON", quality["actual_ron"]), "Target": f'>= {_fmt_prop("RON", quality["target_ron_min"])}'},
                {"Property": "RVP", "Heel": _fmt_prop("RVP", quality["heel_rvp"]), "Blend": _fmt_prop("RVP", quality["actual_rvp"]), "Target": f'<= {_fmt_prop("RVP", quality["target_rvp_max"])}'},
                {"Property": "Density", "Heel": _fmt_prop("Density", quality["heel_density"]), "Blend": _fmt_prop("Density", quality["actual_density"]), "Target": f'{_fmt_prop("Density", quality["target_density_min"])} - {_fmt_prop("Density", quality["target_density_max"])}'},
                {"Property": "Sulfur (ppm)", "Heel": _fmt_prop("Sulfur (ppm)", quality["heel_sulfur_ppm"]), "Blend": _fmt_prop("Sulfur (ppm)", quality["actual_sulfur_ppm"]), "Target": f'<= {_fmt_prop("Sulfur (ppm)", quality["target_sulfur_max_ppm"])}'},
                {"Property": "Olefin (%)", "Heel": _fmt_prop("Olefin (%)", quality["heel_olefin_pct"]), "Blend": _fmt_prop("Olefin (%)", quality["actual_olefin_pct"]), "Target": f'<= {_fmt_prop("Olefin (%)", quality["target_olefin_max_pct"])}'},
            ]
        )
        st.dataframe(quality_df.style.apply(_highlight_quality, axis=1), width="stretch", hide_index=True)


def render_results_page() -> None:
    result = st.session_state["solve_result"]
    if result is None:
        st.info("좌측에서 최적화를 실행한 뒤 결과를 확인해 주세요.")
        return
    if result.status not in {"Optimal", "Feasible"}:
        st.error(result.solver_message)
        return

    gantt_tab, batch_tab, tank_tab, inv_tab, summary_tab = st.tabs(["Campaign Gantt", "Batch Detail", "Tank Level", "Inventory", "Summaries"])

    with gantt_tab:
        chart_event = st.altair_chart(draw_campaign_chart(result.batch_schedule_df), use_container_width=True, on_select="rerun")
        picked_batch_id = _extract_altair_batch_selection(chart_event)
        if picked_batch_id:
            st.session_state["selected_batch_id"] = picked_batch_id
        gantt_batch_ids = result.batch_schedule_df["batch_id"].tolist()
        if st.session_state["selected_batch_id"] not in gantt_batch_ids and gantt_batch_ids:
            st.session_state["selected_batch_id"] = gantt_batch_ids[0]

        if st.session_state["selected_batch_id"]:
            render_batch_details(result, st.session_state["selected_batch_id"])

    with batch_tab:
        product_options = ["ALL"] + sorted(result.batch_schedule_df["display_name"].dropna().unique().tolist())
        selected_product = st.selectbox("Product Filter", product_options, index=0)

        filtered_batches = result.batch_schedule_df.copy()
        if selected_product != "ALL":
            filtered_batches = filtered_batches.loc[filtered_batches["display_name"] == selected_product].copy()

        batch_view = filtered_batches.copy()
        batch_view["batch_ref"] = (
            batch_view["display_name"]
            + " | "
            + batch_view["tank_group"]
            + " B"
            + batch_view["stage_in_tank"].astype(str)
            + " | "
            + batch_view["start_label"]
        )
        batch_view = filtered_batches[
            ["display_name", "order_id", "tank_id", "tank_group", "stage_in_tank", "start_label", "finish_label"]
        ].copy()
        batch_view["batch_ref"] = (
            filtered_batches["display_name"]
            + " | "
            + filtered_batches["tank_group"]
            + " B"
            + filtered_batches["stage_in_tank"].astype(str)
            + " | "
            + filtered_batches["start_label"]
        )
        batch_view = batch_view[["batch_ref", "display_name", "order_id", "tank_id", "tank_group", "stage_in_tank", "start_label", "finish_label"]]
        selection = st.dataframe(
            batch_view,
            width="stretch",
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
        )
        if hasattr(selection, "selection") and selection.selection.rows:
            st.session_state["selected_batch_id"] = filtered_batches.iloc[selection.selection.rows[0]]["batch_id"]
        elif isinstance(selection, dict):
            rows = selection.get("selection", {}).get("rows", [])
            if rows:
                st.session_state["selected_batch_id"] = filtered_batches.iloc[rows[0]]["batch_id"]

        if st.session_state["selected_batch_id"] not in filtered_batches["batch_id"].tolist():
            st.session_state["selected_batch_id"] = None

        if st.session_state["selected_batch_id"] is None and not batch_view.empty:
            st.session_state["selected_batch_id"] = batch_view.iloc[0]["batch_id"]

        batch_options = filtered_batches["batch_id"].tolist()
        if batch_options:
            render_batch_details(result, st.session_state["selected_batch_id"])
        else:
            st.info("선택한 제품에 해당하는 배치가 없습니다.")

    with tank_tab:
        st.altair_chart(draw_tank_level_chart(result.tank_level_profile_df), use_container_width=True)
        st.dataframe(result.tank_level_profile_df[["tank_id", "service", "snapshot_label", "level_m3", "fill_pct"]], width="stretch", hide_index=True)

    with inv_tab:
        st.altair_chart(draw_inventory_chart(result.inventory_profile_df), use_container_width=True)
        st.dataframe(result.inventory_profile_df[["component", "snapshot_label", "inventory_m3"]], width="stretch", hide_index=True)

    with summary_tab:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Order Summary")
            st.dataframe(result.order_summary_df, width="stretch", hide_index=True)
        with c2:
            st.subheader("Slack Summary")
            st.dataframe(result.slack_summary_df, width="stretch", hide_index=True)
        st.subheader("Runtime Profile")
        runtime_df = pd.DataFrame(
            [
                {"Stage": "Total", "Seconds": round(result.total_solve_sec, 2)},
                {"Stage": "Input sanitize", "Seconds": round(result.sanitize_sec, 2)},
                {"Stage": "MILP solve", "Seconds": round(result.milp_sec, 2)},
                {"Stage": "Local resequence", "Seconds": round(result.resequence_sec, 2)},
                {"Stage": "QC reschedule", "Seconds": round(result.qc_reschedule_sec, 2)},
                {"Stage": "Deadstock postprocess", "Seconds": round(result.deadstock_sec, 2)},
            ]
        )
        st.dataframe(runtime_df, width="stretch", hide_index=True)
        st.subheader("Order Blend Plan")
        st.dataframe(result.blend_df, width="stretch", hide_index=True)
        st.subheader("Blendstock Consumption")
        st.dataframe(result.component_summary_df, width="stretch", hide_index=True)


inject_styles()
init_state()
render_sidebar()
planning_tab, results_tab = st.tabs(["Planning", "Results"])

with planning_tab:
    render_planning_page()

with results_tab:
    render_results_page()
