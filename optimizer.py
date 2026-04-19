from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Dict, List

import pandas as pd
import pulp

try:
    from .deadstock_heuristic import refine_deadstock_plan
except ImportError:
    from deadstock_heuristic import refine_deadstock_plan


@dataclass
class SolveArtifacts:
    status: str
    objective_value: float | None
    blend_cost: float
    demurrage_cost: float
    domestic_late_cost: float
    spec_slack_cost: float
    inventory_slack_cost: float
    tank_capacity_slack_cost: float
    carryover_penalty_cost: float
    schedule_df: pd.DataFrame
    batch_schedule_df: pd.DataFrame
    batch_recipe_df: pd.DataFrame
    batch_quality_df: pd.DataFrame
    tank_usage_df: pd.DataFrame
    inventory_profile_df: pd.DataFrame
    tank_level_profile_df: pd.DataFrame
    blend_df: pd.DataFrame
    component_summary_df: pd.DataFrame
    order_summary_df: pd.DataFrame
    slack_summary_df: pd.DataFrame
    solver_message: str
    sanitize_sec: float
    milp_sec: float
    resequence_sec: float
    qc_reschedule_sec: float
    deadstock_sec: float
    total_solve_sec: float


def _normalize_text(value: object) -> str:
    return str(value).strip().upper().replace(" ", "_")


def _coerce_numeric(df: pd.DataFrame, columns: List[str], default: float = 0.0) -> pd.DataFrame:
    for column in columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(default)
    return df


def _product_code(order) -> str:
    export_map = {
        "ELS_AUS": "AUS",
        "ELS_JPN": "JPN",
        "ELS_SGP": "SGP",
        "EHS_IDN": "IDN",
        "EHS_VNM": "VNM",
        "EHS_THA": "THA",
        "EHS_PHL": "PHL",
        "EHS_SEA": "SEA",
    }
    for prefix, code in export_map.items():
        if str(order.order_id).startswith(prefix):
            return code
    return "KOR" if order.market_type == "DOMESTIC" else "UNK"


def _display_name(order) -> str:
    return f"{_product_code(order)}_{int(round(order.ron_min))}R_{order.sulfur_class}"


def _product_signature_from_order(order_dict: Dict[str, float]) -> Dict[str, float]:
    return {
        "ron": float(order_dict["ron_min"]),
        "rvp": float(order_dict["rvp_max"]),
        "density": (float(order_dict["density_min"]) + float(order_dict["density_max"])) / 2.0,
        "sulfur_ppm": float(order_dict["sulfur_max_ppm"]),
        "olefin_pct": float(order_dict["olefin_max_pct"]),
    }


def _carryover_penalty(prev_sig: Dict[str, float] | None, next_sig: Dict[str, float]) -> float:
    if prev_sig is None:
        return 0.0
    return (
        6.0 * abs(prev_sig["sulfur_ppm"] - next_sig["sulfur_ppm"]) / 200.0
        + 4.0 * abs(prev_sig["ron"] - next_sig["ron"]) / 10.0
        + 4.0 * abs(prev_sig["rvp"] - next_sig["rvp"]) / 15.0
        + 2.0 * abs(prev_sig["olefin_pct"] - next_sig["olefin_pct"]) / 20.0
        + 2.0 * abs(prev_sig["density"] - next_sig["density"]) / 0.05
    )


def _simulate_service_sequence(
    service_orders: pd.DataFrame,
    service_tanks: List[str],
    orders_lookup: Dict[str, Dict[str, float]],
    tank_signature_seed: Dict[str, Dict[str, float] | None],
) -> tuple[List[dict], float]:
    tank_available = {tank_id: 0.0 for tank_id in service_tanks}
    tank_signature = {tank_id: tank_signature_seed.get(tank_id) for tank_id in service_tanks}
    service_clock = 0.0
    assignments: List[dict] = []
    total_cost = 0.0

    for row in service_orders.itertuples():
        duration = float(row.finish_day) - float(row.start_day)
        desired_start = max(float(row.start_day), service_clock)
        order_meta = orders_lookup[row.order_id]
        order_sig = _product_signature_from_order(order_meta)

        best = None
        for i, tank_a in enumerate(service_tanks):
            for tank_b in service_tanks[i + 1 :]:
                start_time = max(desired_start, tank_available[tank_a], tank_available[tank_b])
                wait_days = max(0.0, start_time - desired_start)
                contamination = _carryover_penalty(tank_signature[tank_a], order_sig) + _carryover_penalty(
                    tank_signature[tank_b], order_sig
                )

                late_hours = max(0.0, (start_time + duration - float(row.due_day)) * 24.0)
                lateness_cost = (
                    float(order_meta["demurrage_per_hour"]) * late_hours
                    if str(order_meta["market_type"]) == "EXPORT"
                    else 5000.0 * late_hours
                )
                score = contamination * 50.0 + wait_days * 100.0 + lateness_cost
                candidate = (score, contamination, start_time, tank_a, tank_b, late_hours)
                if best is None or candidate[0] < best[0]:
                    best = candidate

        assert best is not None
        score, contamination, actual_start, tank_a, tank_b, late_hours = best
        actual_finish = actual_start + duration
        service_clock = actual_finish
        tank_available[tank_a] = actual_finish
        tank_available[tank_b] = actual_finish
        tank_signature[tank_a] = order_sig
        tank_signature[tank_b] = order_sig
        total_cost += score
        assignments.append(
            {
                "order_id": row.order_id,
                "start_day": round(actual_start, 2),
                "finish_day": round(actual_finish, 2),
                "late_hours": round(late_hours, 2),
                "assigned_tanks": (tank_a, tank_b),
                "carryover_penalty": contamination * 50.0,
            }
        )

    return assignments, total_cost


def _improve_service_ordering(
    service_orders: pd.DataFrame,
    service_tanks: List[str],
    orders_lookup: Dict[str, Dict[str, float]],
    tank_signature_seed: Dict[str, Dict[str, float] | None],
) -> tuple[pd.DataFrame, List[dict], float]:
    working = service_orders.sort_values(["sequence_rank", "start_day", "due_day", "order_id"]).reset_index(drop=True)
    best_assignments, best_cost = _simulate_service_sequence(working, service_tanks, orders_lookup, tank_signature_seed)

    improved = True
    while improved and len(working) > 1:
        improved = False
        for idx in range(len(working) - 1):
            trial = working.copy()
            swap_row = trial.iloc[idx].copy()
            trial.iloc[idx] = trial.iloc[idx + 1]
            trial.iloc[idx + 1] = swap_row
            trial = trial.reset_index(drop=True)
            trial_assignments, trial_cost = _simulate_service_sequence(trial, service_tanks, orders_lookup, tank_signature_seed)
            if trial_cost + 1e-6 < best_cost:
                working = trial
                best_assignments = trial_assignments
                best_cost = trial_cost
                improved = True
                break

    working["sequence_rank"] = range(len(working))
    return working, best_assignments, best_cost


def _apply_local_tank_resequence(
    schedule_df: pd.DataFrame,
    batch_schedule_df: pd.DataFrame,
    tanks_df: pd.DataFrame,
    orders_lookup: Dict[str, Dict[str, float]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, float]:
    updated_schedule = schedule_df.copy()
    updated_batch = batch_schedule_df.copy()
    if "sequence_rank" not in updated_schedule.columns:
        updated_schedule["sequence_rank"] = (
            updated_schedule.sort_values(["start_day", "blender", "due_day", "order_id"])
            .groupby("blender")
            .cumcount()
        )
    tank_usage_rows = []
    carryover_cost = 0.0
    tank_signature_seed = {
        row.tank_id: (
            {
                "ron": float(row.initial_ron),
                "rvp": float(row.initial_rvp),
                "density": float(row.initial_density),
                "sulfur_ppm": float(row.initial_sulfur_ppm),
                "olefin_pct": float(row.initial_olefin_pct),
            }
            if float(row.initial_volume_m3) > 1e-6
            else None
        )
        for row in tanks_df.itertuples()
    }

    for service, service_orders in updated_schedule.groupby("blender", sort=False):
        service_tanks = tanks_df.loc[tanks_df["service"] == service, "tank_id"].tolist()
        service_orders = service_orders.sort_values(["sequence_rank", "start_day", "due_day", "order_id"]).copy()
        service_seed = {tank_id: tank_signature_seed.get(tank_id) for tank_id in service_tanks}
        improved_orders, assignments, service_cost = _improve_service_ordering(service_orders, service_tanks, orders_lookup, service_seed)
        carryover_cost += service_cost
        assignment_map = {item["order_id"]: item for item in assignments}

        for row in improved_orders.itertuples():
            chosen = assignment_map[row.order_id]
            actual_start = chosen["start_day"]
            actual_finish = chosen["finish_day"]
            tank_a, tank_b = chosen["assigned_tanks"]
            idx = updated_schedule.index[updated_schedule["order_id"] == row.order_id][0]
            updated_schedule.loc[idx, "start_day"] = round(actual_start, 2)
            updated_schedule.loc[idx, "finish_day"] = round(actual_finish, 2)
            updated_schedule.loc[idx, "late_hours"] = chosen["late_hours"]
            updated_schedule.loc[idx, "assigned_tanks"] = f"{tank_a}, {tank_b}"
            updated_schedule.loc[idx, "sequence_rank"] = int(row.sequence_rank)

            order_batches = updated_batch.loc[updated_batch["order_id"] == row.order_id].copy()
            old_start = float(row.start_day)
            for batch_idx in order_batches.index:
                rel_start = float(updated_batch.loc[batch_idx, "start_day"]) - old_start
                rel_finish = float(updated_batch.loc[batch_idx, "finish_day"]) - old_start
                updated_batch.loc[batch_idx, "start_day"] = round(actual_start + rel_start, 2)
                updated_batch.loc[batch_idx, "finish_day"] = round(actual_start + rel_finish, 2)
                updated_batch.loc[batch_idx, "late_hours"] = chosen["late_hours"]
                updated_batch.loc[batch_idx, "tank_id"] = tank_a if updated_batch.loc[batch_idx, "tank_group"] == "T1" else tank_b

            tank_usage_rows.append(
                {"tank_id": tank_a, "order_id": row.order_id, "service": service, "start_day": round(actual_start, 2), "finish_day": round(actual_finish, 2)}
            )
            tank_usage_rows.append(
                {"tank_id": tank_b, "order_id": row.order_id, "service": service, "start_day": round(actual_start, 2), "finish_day": round(actual_finish, 2)}
            )

    tank_usage_df = pd.DataFrame(tank_usage_rows).sort_values(["tank_id", "start_day"]).reset_index(drop=True)
    updated_schedule = updated_schedule.sort_values(["start_day", "blender", "sequence_rank", "order_id"]).reset_index(drop=True)
    updated_batch = updated_batch.sort_values(["start_day", "blender", "batch_id"]).reset_index(drop=True)
    return updated_schedule, updated_batch, tank_usage_df, carryover_cost


def _reschedule_batches_with_qc(
    schedule_df: pd.DataFrame,
    batch_schedule_df: pd.DataFrame,
    qc_hours: float,
    batch_hours: float,
    blender_gap_hours: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if schedule_df.empty or batch_schedule_df.empty:
        return schedule_df.copy(), batch_schedule_df.copy()

    updated_schedule = schedule_df.copy()
    updated_batch = batch_schedule_df.copy()
    batch_duration_days = batch_hours / 24.0
    qc_duration_days = qc_hours / 24.0
    blender_gap_days = blender_gap_hours / 24.0

    for service, service_orders in updated_schedule.groupby("blender", sort=False):
        service_batches = updated_batch.loc[updated_batch["blender"] == service].copy()
        if service_batches.empty:
            continue

        order_sequence = service_orders.sort_values(["sequence_rank", "start_day", "due_day", "order_id"])
        order_state: Dict[str, dict] = {}
        for row in order_sequence.itertuples():
            order_batches = service_batches.loc[service_batches["order_id"] == row.order_id].copy()
            if order_batches.empty:
                continue
            order_batches["tank_rank"] = order_batches["tank_group"].astype(str).str.extract(r"(\d+)").astype(int)
            order_batches = order_batches.sort_values(["stage_in_tank", "tank_rank", "batch_id"]).reset_index(drop=True)
            tank_ready = {tank_group: float(row.start_day) for tank_group in order_batches["tank_group"].unique()}
            order_state[row.order_id] = {
                "queue": order_batches.to_dict("records"),
                "index": 0,
                "order_ready": float(row.start_day),
                "tank_ready": tank_ready,
                "sequence_rank": int(row.sequence_rank),
            }

        cursor = min(float(row.start_day) for row in order_sequence.itertuples())
        while True:
            candidates = []
            for order_id, state in order_state.items():
                if state["index"] >= len(state["queue"]):
                    continue
                task = state["queue"][state["index"]]
                release = max(
                    float(state["order_ready"]),
                    float(state["tank_ready"].get(task["tank_group"], 0.0)),
                )
                candidates.append((max(cursor, release), state["sequence_rank"], release, order_id, task))

            if not candidates:
                break

            actual_start, _, release, order_id, task = min(candidates, key=lambda item: (item[0], item[1], item[2], item[3]))
            actual_finish = actual_start + batch_duration_days
            batch_mask = updated_batch["batch_id"] == task["batch_id"]
            updated_batch.loc[batch_mask, "start_day"] = round(actual_start, 4)
            updated_batch.loc[batch_mask, "finish_day"] = round(actual_finish, 4)

            state = order_state[order_id]
            state["order_ready"] = actual_finish
            state["tank_ready"][task["tank_group"]] = actual_finish + qc_duration_days
            state["index"] += 1
            cursor = actual_finish + blender_gap_days

        for row in order_sequence.itertuples():
            order_batches = updated_batch.loc[updated_batch["order_id"] == row.order_id]
            if order_batches.empty:
                continue
            order_start = float(order_batches["start_day"].min())
            blend_finish = float(order_batches["finish_day"].max())
            campaign_release = blend_finish + qc_duration_days
            idx = updated_schedule.index[updated_schedule["order_id"] == row.order_id][0]
            updated_schedule.loc[idx, "start_day"] = round(order_start, 4)
            updated_schedule.loc[idx, "finish_day"] = round(campaign_release, 4)
            updated_schedule.loc[idx, "late_hours"] = round(max(0.0, (campaign_release - float(updated_schedule.loc[idx, "due_day"])) * 24.0), 2)

    updated_schedule = updated_schedule.sort_values(["start_day", "blender", "sequence_rank", "order_id"]).reset_index(drop=True)
    updated_batch = updated_batch.sort_values(["start_day", "blender", "batch_id"]).reset_index(drop=True)
    return updated_schedule, updated_batch


def _repair_inventory_shortages(
    schedule_df: pd.DataFrame,
    batch_schedule_df: pd.DataFrame,
    blend_df: pd.DataFrame,
    inventory_profile_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    qc_hours: float,
    batch_hours: float,
    blender_gap_hours: float,
    max_iters: int = 4,
    shift_days: float = 1.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    repaired_schedule = schedule_df.copy()
    repaired_batch = batch_schedule_df.copy()
    order_meta = orders_df.set_index("order_id").to_dict("index")

    for _ in range(max_iters):
        negative_rows = inventory_profile_df.loc[inventory_profile_df["inventory_m3"] < -1e-6].sort_values(["day", "inventory_m3"])
        if negative_rows.empty:
            break

        shortage = negative_rows.iloc[0]
        component = shortage["component"]
        shortage_day = float(shortage["day"])

        candidate_usage = (
            blend_df.loc[blend_df["component"] == component]
            .groupby("order_id", as_index=False)["volume_m3"]
            .sum()
            .rename(columns={"volume_m3": "component_use_m3"})
            .sort_values("component_use_m3", ascending=False)
        )
        if candidate_usage.empty:
            break

        candidates = repaired_schedule.merge(candidate_usage, on="order_id", how="inner")
        candidates = candidates.loc[candidates["start_day"] <= shortage_day].copy()
        if candidates.empty:
            break

        candidates["market_priority"] = candidates["market_type"].map({"EXPORT": 0, "DOMESTIC": 1}).fillna(2)
        candidates = candidates.sort_values(["market_priority", "start_day", "component_use_m3"], ascending=[True, False, False])
        chosen = candidates.iloc[0]
        order_id = chosen["order_id"]

        repaired_schedule.loc[repaired_schedule["order_id"] == order_id, "start_day"] = (
            repaired_schedule.loc[repaired_schedule["order_id"] == order_id, "start_day"] + shift_days
        )
        repaired_batch.loc[repaired_batch["order_id"] == order_id, "start_day"] = (
            repaired_batch.loc[repaired_batch["order_id"] == order_id, "start_day"] + shift_days
        )
        repaired_batch.loc[repaired_batch["order_id"] == order_id, "finish_day"] = (
            repaired_batch.loc[repaired_batch["order_id"] == order_id, "finish_day"] + shift_days
        )

        repaired_schedule = repaired_schedule.sort_values(["blender", "start_day", "order_id"]).reset_index(drop=True)
        repaired_schedule["sequence_rank"] = repaired_schedule.groupby("blender").cumcount()
        repaired_schedule, repaired_batch = _reschedule_batches_with_qc(
            schedule_df=repaired_schedule,
            batch_schedule_df=repaired_batch,
            qc_hours=qc_hours,
            batch_hours=batch_hours,
            blender_gap_hours=blender_gap_hours,
        )

        repaired_schedule["late_hours"] = repaired_schedule.apply(
            lambda row: round(max(0.0, (float(row["finish_day"]) - float(order_meta[row["order_id"]]["due_day"])) * 24.0), 2),
            axis=1,
        )
        repaired_batch["late_hours"] = repaired_batch["order_id"].map(
            repaired_schedule.set_index("order_id")["late_hours"].to_dict()
        )

    return repaired_schedule, repaired_batch


def sanitize_inputs(
    orders_df: pd.DataFrame,
    blendstocks_df: pd.DataFrame,
    tanks_df: pd.DataFrame,
    settings: Dict[str, float],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, float]]:
    orders = orders_df.copy().dropna(how="all")
    blendstocks = blendstocks_df.copy().dropna(how="all")
    tanks = tanks_df.copy().dropna(how="all")

    orders["order_id"] = orders["order_id"].fillna("").astype(str).str.strip()
    blendstocks["component"] = blendstocks["component"].fillna("").astype(str).str.strip()
    tanks["tank_id"] = tanks["tank_id"].fillna("").astype(str).str.strip()

    orders = orders.loc[orders["order_id"] != ""].copy()
    blendstocks = blendstocks.loc[blendstocks["component"] != ""].copy()
    tanks = tanks.loc[tanks["tank_id"] != ""].copy()

    if orders.empty:
        raise ValueError("Sales Orders에 최소 1개 이상의 유효한 주문이 필요합니다.")
    if blendstocks.empty:
        raise ValueError("Blendstocks에 최소 1개 이상의 유효한 반제품이 필요합니다.")
    if tanks.empty:
        raise ValueError("Product Tanks에 최소 1개 이상의 유효한 탱크가 필요합니다.")

    orders["market_type"] = orders["market_type"].map(_normalize_text)
    orders["sulfur_class"] = orders["sulfur_class"].map(_normalize_text)
    orders["service"] = orders["market_type"] + "_" + orders["sulfur_class"]
    valid_services = {"EXPORT_LS", "EXPORT_HS", "DOMESTIC_LS", "DOMESTIC_HS"}
    orders = orders.loc[orders["service"].isin(valid_services)].copy()

    schedule_start = pd.to_datetime(settings.get("schedule_start"))
    schedule_end = pd.to_datetime(settings.get("schedule_end"))
    if pd.isna(schedule_start) or pd.isna(schedule_end):
        raise ValueError("Planning Settings에 유효한 스케줄 시작일/종료일이 필요합니다.")
    if schedule_end < schedule_start:
        raise ValueError("스케줄 종료일은 시작일보다 빠를 수 없습니다.")
    horizon_days = int((schedule_end.normalize() - schedule_start.normalize()).days) + 1
    if "due_at" in orders.columns:
        orders["due_at"] = pd.to_datetime(orders["due_at"], errors="coerce")
        orders["due_day"] = ((orders["due_at"] - schedule_start).dt.total_seconds() / 86400.0) + 1.0
    orders = _coerce_numeric(
        orders,
        [
            "due_day",
            "volume_m3",
            "ron_min",
            "rvp_max",
            "density_min",
            "density_max",
            "sulfur_max_ppm",
            "olefin_max_pct",
            "demurrage_per_hour",
        ],
    )
    orders = orders.loc[(orders["due_day"] > 0) & (orders["volume_m3"] > 0)].copy()

    blendstocks = _coerce_numeric(
        blendstocks,
        [
            "ron",
            "rvp",
            "density",
            "sulfur_ppm",
            "olefin_pct",
            "cost_per_m3",
            "initial_inventory",
            "rundown_per_hour",
        ],
    )
    tanks["service"] = tanks["service"].map(_normalize_text)
    tanks = tanks.loc[tanks["service"].isin(valid_services)].copy()
    for column in [
        "initial_volume_m3",
        "initial_ron",
        "initial_rvp",
        "initial_density",
        "initial_sulfur_ppm",
        "initial_olefin_pct",
    ]:
        if column not in tanks.columns:
            tanks[column] = 0.0
    tanks = _coerce_numeric(
        tanks,
        [
            "capacity_m3",
            "initial_volume_m3",
            "initial_ron",
            "initial_rvp",
            "initial_density",
            "initial_sulfur_ppm",
            "initial_olefin_pct",
        ],
    )
    tanks = tanks.loc[tanks["capacity_m3"] > 0].copy()
    tanks["deadstock_min_m3"] = tanks["capacity_m3"] * float(settings.get("deadstock_fraction", 0.20))
    tanks = tanks.loc[tanks["initial_volume_m3"] >= 0].copy()

    if orders["order_id"].duplicated().any():
        raise ValueError("Sales Orders의 order_id는 중복되면 안 됩니다.")
    if blendstocks["component"].duplicated().any():
        raise ValueError("Blendstocks의 component는 중복되면 안 됩니다.")
    if tanks["tank_id"].duplicated().any():
        raise ValueError("Product Tanks의 tank_id는 중복되면 안 됩니다.")
    over_capacity = tanks.loc[tanks["initial_volume_m3"] > tanks["capacity_m3"]]
    if not over_capacity.empty:
        raise ValueError("Product Tanks의 initial_volume_m3는 capacity_m3를 초과할 수 없습니다.")

    batch_hours = int(settings["batch_hours"])
    batches_per_order = int(settings["batches_per_order"])
    tanks_per_order = int(settings["tanks_per_order"])
    qc_hours = int(settings["qc_hours"])
    solver_time_limit = int(settings["solver_time_limit_sec"])
    mip_gap_rel = float(settings.get("mip_gap_rel", 0.20))
    base_spec_penalty = float(settings.get("base_spec_penalty", 100.0))
    hard_slack_penalty = float(settings.get("hard_slack_penalty", 1000.0))
    inventory_slack_penalty = float(settings.get("inventory_slack_penalty", hard_slack_penalty))
    key_spec_multiplier = float(settings.get("key_spec_multiplier", 2.0))
    domestic_late_penalty = float(settings.get("domestic_late_penalty", 5000.0))
    interim_relax_pct = float(settings.get("interim_relax_pct", 0.10))

    if batches_per_order % tanks_per_order != 0:
        raise ValueError("Batches / order는 Tanks / order로 나누어 떨어져야 합니다.")

    return orders, blendstocks, tanks, {
        "batch_hours": batch_hours,
        "horizon_days": horizon_days,
        "schedule_start": schedule_start.isoformat(),
        "schedule_end": schedule_end.isoformat(),
        "batches_per_order": batches_per_order,
        "tanks_per_order": tanks_per_order,
        "qc_hours": qc_hours,
        "solver_time_limit_sec": solver_time_limit,
        "mip_gap_rel": mip_gap_rel,
        "base_spec_penalty": base_spec_penalty,
        "hard_slack_penalty": hard_slack_penalty,
        "inventory_slack_penalty": inventory_slack_penalty,
        "key_spec_multiplier": key_spec_multiplier,
        "domestic_late_penalty": domestic_late_penalty,
        "interim_relax_pct": interim_relax_pct,
    }


def _attach_calendar_columns(df: pd.DataFrame, schedule_start: str, mappings: Dict[str, str]) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    enriched = df.copy()
    base = pd.Timestamp(schedule_start)
    for numeric_col, prefix in mappings.items():
        if numeric_col not in enriched.columns:
            continue
        ts_col = f"{prefix}_at"
        label_col = f"{prefix}_label"
        minutes = (pd.to_numeric(enriched[numeric_col], errors="coerce").fillna(0.0) * 24.0 * 60.0).round().astype(int)
        enriched[ts_col] = base + pd.to_timedelta(minutes, unit="m")
        enriched[label_col] = pd.to_datetime(enriched[ts_col]).dt.strftime("%m-%d %H:%M")
    return enriched


def solve_batch_schedule(
    orders_df: pd.DataFrame,
    blendstocks_df: pd.DataFrame,
    tanks_df: pd.DataFrame,
    settings: Dict[str, float],
) -> SolveArtifacts:
    total_t0 = perf_counter()
    stage_t0 = perf_counter()
    orders, blendstocks, tanks, cfg = sanitize_inputs(orders_df, blendstocks_df, tanks_df, settings)
    sanitize_sec = perf_counter() - stage_t0

    slot_hours = cfg["batch_hours"]
    horizon_slots = int(cfg["horizon_days"] * 24 / slot_hours)
    process_slots = cfg["batches_per_order"]
    qc_slots = int(math.ceil(cfg["qc_hours"] / slot_hours))
    occupancy_slots = process_slots + qc_slots
    last_start_slot = horizon_slots - occupancy_slots
    stages_per_tank = cfg["batches_per_order"] // cfg["tanks_per_order"]

    if last_start_slot < 0:
        raise ValueError("Time horizon is shorter than one order campaign.")

    orders = orders.copy()
    orders["due_hour"] = orders["due_day"] * 24.0
    orders["half_volume_m3"] = orders["volume_m3"] / cfg["tanks_per_order"]
    orders["batch_volume_m3"] = orders["volume_m3"] / cfg["batches_per_order"]

    component_names = blendstocks["component"].tolist()
    component_props = blendstocks.set_index("component").to_dict("index")
    tank_capacity = tanks.set_index("tank_id")["capacity_m3"].to_dict()
    compatible_tanks = {
        service: tanks.loc[tanks["service"] == service, "tank_id"].tolist()
        for service in tanks["service"].unique()
    }
    spec_weights = {
        "ron_short": cfg["base_spec_penalty"] * cfg["key_spec_multiplier"],
        "rvp_excess": cfg["base_spec_penalty"] * cfg["key_spec_multiplier"],
        "density_low": cfg["base_spec_penalty"],
        "density_high": cfg["base_spec_penalty"],
        "sulfur_excess": cfg["base_spec_penalty"] * cfg["key_spec_multiplier"],
        "olefin_excess": cfg["base_spec_penalty"],
    }

    candidate_starts = {order.order_id: list(range(last_start_slot + 1)) for order in orders.itertuples()}
    tank_groups = list(range(1, cfg["tanks_per_order"] + 1))
    stages = list(range(1, stages_per_tank + 1))

    model = pulp.LpProblem("refinery_batch_scheduling", pulp.LpMinimize)

    start = {
        (order_id, slot): pulp.LpVariable(f"start__{order_id}__{slot}", cat="Binary")
        for order_id, slots in candidate_starts.items()
        for slot in slots
    }
    use_tank = {
        (order.order_id, tank_id, slot): pulp.LpVariable(f"use_tank__{order.order_id}__{tank_id}__{slot}", cat="Binary")
        for order in orders.itertuples()
        for tank_id in compatible_tanks[order.service]
        for slot in candidate_starts[order.order_id]
    }
    batch_qty = {
        (order.order_id, tank_group, stage, component): pulp.LpVariable(
            f"batch_qty__{order.order_id}__T{tank_group}__B{stage}__{component}",
            lowBound=0,
            cat="Continuous",
        )
        for order in orders.itertuples()
        for tank_group in tank_groups
        for stage in stages
        for component in component_names
    }
    spec_slack = {
        (order.order_id, tank_group, stage, name): pulp.LpVariable(
            f"spec_slack__{order.order_id}__T{tank_group}__B{stage}__{name}",
            lowBound=0,
            cat="Continuous",
        )
        for order in orders.itertuples()
        for tank_group in tank_groups
        for stage in stages
        for name in spec_weights
    }
    inventory_slack = {
        component: pulp.LpVariable(f"inventory_slack__{component}", lowBound=0, cat="Continuous")
        for component in component_names
    }
    tank_capacity_slack = {
        (order.order_id, slot): pulp.LpVariable(f"tank_cap_slack__{order.order_id}__{slot}", lowBound=0, cat="Continuous")
        for order in orders.itertuples()
        for slot in candidate_starts[order.order_id]
    }
    tardy_hours = {
        order.order_id: pulp.LpVariable(f"tardy__{order.order_id}", lowBound=0, cat="Continuous")
        for order in orders.itertuples()
    }

    for order in orders.itertuples():
        slots = candidate_starts[order.order_id]
        model += pulp.lpSum(start[(order.order_id, slot)] for slot in slots) == 1

        for slot in slots:
            model += (
                pulp.lpSum(use_tank[(order.order_id, tank_id, slot)] for tank_id in compatible_tanks[order.service])
                == cfg["tanks_per_order"] * start[(order.order_id, slot)]
            )
            model += (
                pulp.lpSum(tank_capacity[tank_id] * use_tank[(order.order_id, tank_id, slot)] for tank_id in compatible_tanks[order.service])
                + tank_capacity_slack[(order.order_id, slot)]
                >= order.volume_m3 * start[(order.order_id, slot)]
            )

        for tank_id in compatible_tanks[order.service]:
            for slot in slots:
                model += use_tank[(order.order_id, tank_id, slot)] <= start[(order.order_id, slot)]

        for tank_group in tank_groups:
            for stage in stages:
                model += (
                    pulp.lpSum(batch_qty[(order.order_id, tank_group, stage, component)] for component in component_names)
                    == order.batch_volume_m3
                )

            cumulative_volume = 0.0
            for stage in stages:
                cumulative_volume += order.batch_volume_m3
                cumulative = {
                    component: pulp.lpSum(
                        batch_qty[(order.order_id, tank_group, stage_idx, component)]
                        for stage_idx in range(1, stage + 1)
                    )
                    for component in component_names
                }
                if stage == stages_per_tank:
                    ron_target = order.ron_min
                    rvp_target = order.rvp_max
                    density_min_target = order.density_min
                    density_max_target = order.density_max
                    sulfur_target = order.sulfur_max_ppm
                    olefin_target = order.olefin_max_pct
                else:
                    relax = cfg["interim_relax_pct"]
                    ron_target = order.ron_min * (1.0 - relax)
                    rvp_target = order.rvp_max * (1.0 + relax)
                    density_min_target = order.density_min * (1.0 - relax)
                    density_max_target = order.density_max * (1.0 + relax)
                    sulfur_target = order.sulfur_max_ppm * (1.0 + relax)
                    olefin_target = order.olefin_max_pct * (1.0 + relax)

                ron_expr = pulp.lpSum(component_props[c]["ron"] * cumulative[c] for c in component_names)
                rvp_expr = pulp.lpSum(component_props[c]["rvp"] * cumulative[c] for c in component_names)
                density_expr = pulp.lpSum(component_props[c]["density"] * cumulative[c] for c in component_names)
                sulfur_expr = pulp.lpSum(component_props[c]["sulfur_ppm"] * cumulative[c] for c in component_names)
                olefin_expr = pulp.lpSum(component_props[c]["olefin_pct"] * cumulative[c] for c in component_names)

                model += ron_expr + spec_slack[(order.order_id, tank_group, stage, "ron_short")] * cumulative_volume >= ron_target * cumulative_volume
                model += rvp_expr <= (rvp_target + spec_slack[(order.order_id, tank_group, stage, "rvp_excess")]) * cumulative_volume
                model += density_expr + spec_slack[(order.order_id, tank_group, stage, "density_low")] * cumulative_volume >= density_min_target * cumulative_volume
                model += density_expr <= (density_max_target + spec_slack[(order.order_id, tank_group, stage, "density_high")]) * cumulative_volume
                model += sulfur_expr <= (sulfur_target + spec_slack[(order.order_id, tank_group, stage, "sulfur_excess")]) * cumulative_volume
                model += olefin_expr <= (olefin_target + spec_slack[(order.order_id, tank_group, stage, "olefin_excess")]) * cumulative_volume

        lateness_expr = pulp.lpSum(
            max(0.0, ((slot + occupancy_slots) * slot_hours) - order.due_hour) * start[(order.order_id, slot)]
            for slot in slots
        )
        model += tardy_hours[order.order_id] >= lateness_expr

    for service in orders["service"].unique():
        service_orders = orders.loc[orders["service"] == service, "order_id"].tolist()
        for slot in range(horizon_slots):
            active_orders = []
            for order_id in service_orders:
                for start_slot in candidate_starts[order_id]:
                    if start_slot <= slot < start_slot + process_slots:
                        active_orders.append(start[(order_id, start_slot)])
            model += pulp.lpSum(active_orders) <= 1

    for tank in tanks.itertuples():
        relevant_orders = orders.loc[orders["service"] == tank.service, "order_id"].tolist()
        for slot in range(horizon_slots):
            active_uses = []
            for order_id in relevant_orders:
                for start_slot in candidate_starts[order_id]:
                    if start_slot <= slot < start_slot + occupancy_slots:
                        active_uses.append(use_tank[(order_id, tank.tank_id, start_slot)])
            model += pulp.lpSum(active_uses) <= 1

    for component in component_names:
        initial_inventory = component_props[component]["initial_inventory"]
        rundown_per_hour = component_props[component]["rundown_per_hour"]
        available = initial_inventory + rundown_per_hour * (cfg["horizon_days"] * 24)
        usage = pulp.lpSum(
            batch_qty[(order.order_id, tank_group, stage, component)]
            for order in orders.itertuples()
            for tank_group in tank_groups
            for stage in stages
        )
        model += usage <= available + inventory_slack[component]

    blend_cost_expr = pulp.lpSum(
        component_props[component]["cost_per_m3"] * batch_qty[(order.order_id, tank_group, stage, component)]
        for order in orders.itertuples()
        for tank_group in tank_groups
        for stage in stages
        for component in component_names
    )
    demurrage_expr = pulp.lpSum(
        order.demurrage_per_hour * tardy_hours[order.order_id]
        for order in orders.itertuples()
        if order.market_type == "EXPORT"
    )
    domestic_late_expr = pulp.lpSum(
        cfg["domestic_late_penalty"] * tardy_hours[order.order_id]
        for order in orders.itertuples()
        if order.market_type == "DOMESTIC"
    )
    spec_slack_expr = pulp.lpSum(
        spec_weights[name] * spec_slack[(order.order_id, tank_group, stage, name)] * (stage * order.batch_volume_m3)
        for order in orders.itertuples()
        for tank_group in tank_groups
        for stage in stages
        for name in spec_weights
    )
    inventory_slack_expr = cfg["inventory_slack_penalty"] * pulp.lpSum(inventory_slack.values())
    tank_capacity_slack_expr = cfg["hard_slack_penalty"] * pulp.lpSum(tank_capacity_slack.values())

    model += (
        blend_cost_expr
        + demurrage_expr
        + domestic_late_expr
        + spec_slack_expr
        + inventory_slack_expr
        + tank_capacity_slack_expr
    )

    solver = pulp.HiGHS(msg=False, timeLimit=cfg["solver_time_limit_sec"], gapRel=cfg["mip_gap_rel"])
    milp_t0 = perf_counter()
    model.solve(solver)
    milp_sec = perf_counter() - milp_t0

    status = pulp.LpStatus[model.status]
    if status not in {"Optimal", "Feasible"}:
        return SolveArtifacts(
            status=status,
            objective_value=None,
            blend_cost=0.0,
            demurrage_cost=0.0,
            domestic_late_cost=0.0,
            spec_slack_cost=0.0,
            inventory_slack_cost=0.0,
            tank_capacity_slack_cost=0.0,
            schedule_df=pd.DataFrame(),
            batch_schedule_df=pd.DataFrame(),
            batch_recipe_df=pd.DataFrame(),
            batch_quality_df=pd.DataFrame(),
            tank_usage_df=pd.DataFrame(),
            inventory_profile_df=pd.DataFrame(),
            tank_level_profile_df=pd.DataFrame(),
            blend_df=pd.DataFrame(),
            component_summary_df=pd.DataFrame(),
            order_summary_df=pd.DataFrame(),
            slack_summary_df=pd.DataFrame(),
            solver_message="Solver could not find a feasible plan with the current inputs.",
            sanitize_sec=sanitize_sec,
            milp_sec=milp_sec,
            resequence_sec=0.0,
            qc_reschedule_sec=0.0,
            deadstock_sec=0.0,
            total_solve_sec=perf_counter() - total_t0,
        )

    schedule_rows = []
    batch_schedule_rows = []
    batch_recipe_rows = []
    batch_quality_rows = []
    tank_rows = []
    blend_rows = []
    component_summary = {component: 0.0 for component in component_names}
    slack_rows = []

    for order in orders.itertuples():
        chosen_slot = next(slot for slot in candidate_starts[order.order_id] if pulp.value(start[(order.order_id, slot)]) > 0.5)
        assigned_tanks = [
            tank_id
            for tank_id in compatible_tanks[order.service]
            if pulp.value(use_tank[(order.order_id, tank_id, chosen_slot)]) > 0.5
        ]
        completion_hour = (chosen_slot + occupancy_slots) * slot_hours
        late_hours_value = float(pulp.value(tardy_hours[order.order_id]) or 0.0)
        display_name = _display_name(order)

        schedule_rows.append(
            {
                "order_id": order.order_id,
                "display_name": display_name,
                "market_type": order.market_type,
                "sulfur_class": order.sulfur_class,
                "grade_name": order.grade_name,
                "region": order.region,
                "start_day": round(chosen_slot * slot_hours / 24.0, 2),
                "finish_day": round(completion_hour / 24.0, 2),
                "due_day": order.due_day,
                "late_hours": round(late_hours_value, 2),
                "volume_m3": order.volume_m3,
                "batch_volume_m3": round(order.batch_volume_m3, 2),
                "blender": order.service,
                "assigned_tanks": ", ".join(assigned_tanks),
            }
        )

        order_component_totals = {component: 0.0 for component in component_names}
        for tank_group in tank_groups:
            tank_id = assigned_tanks[tank_group - 1] if len(assigned_tanks) >= tank_group else f"T{tank_group}"
            cumulative_component_qty = {component: 0.0 for component in component_names}
            for stage in stages:
                batch_id = f"{order.order_id}_T{tank_group}_B{stage}"
                batch_start_hour = (chosen_slot + (tank_group - 1) * 2 + (stage - 1)) * slot_hours / max(1, cfg["tanks_per_order"])
                batch_finish_hour = batch_start_hour + slot_hours

                for component in component_names:
                    qty_value = float(pulp.value(batch_qty[(order.order_id, tank_group, stage, component)]) or 0.0)
                    if qty_value > 1e-6:
                        cumulative_component_qty[component] += qty_value
                        order_component_totals[component] += qty_value
                        component_summary[component] += qty_value
                        batch_recipe_rows.append(
                            {
                                "batch_id": batch_id,
                                "order_id": order.order_id,
                                "display_name": display_name,
                                "tank_group": tank_group,
                                "stage_in_tank": stage,
                                "component": component,
                                "volume_m3": round(qty_value, 2),
                                "share_pct": round((qty_value / order.batch_volume_m3) * 100.0, 2),
                                "component_ron": component_props[component]["ron"],
                                "component_rvp": component_props[component]["rvp"],
                                "component_density": component_props[component]["density"],
                                "component_sulfur_ppm": component_props[component]["sulfur_ppm"],
                                "component_olefin_pct": component_props[component]["olefin_pct"],
                            }
                        )

                cumulative_volume = stage * order.batch_volume_m3
                actual_ron = sum(component_props[c]["ron"] * cumulative_component_qty[c] for c in component_names) / cumulative_volume
                actual_rvp = sum(component_props[c]["rvp"] * cumulative_component_qty[c] for c in component_names) / cumulative_volume
                actual_density = sum(component_props[c]["density"] * cumulative_component_qty[c] for c in component_names) / cumulative_volume
                actual_sulfur = sum(component_props[c]["sulfur_ppm"] * cumulative_component_qty[c] for c in component_names) / cumulative_volume
                actual_olefin = sum(component_props[c]["olefin_pct"] * cumulative_component_qty[c] for c in component_names) / cumulative_volume

                if stage == stages_per_tank:
                    ron_target = order.ron_min
                    rvp_target = order.rvp_max
                    density_min_target = order.density_min
                    density_max_target = order.density_max
                    sulfur_target = order.sulfur_max_ppm
                    olefin_target = order.olefin_max_pct
                    spec_rule = "Final tank batch must satisfy product spec"
                else:
                    relax = cfg["interim_relax_pct"]
                    ron_target = order.ron_min * (1.0 - relax)
                    rvp_target = order.rvp_max * (1.0 + relax)
                    density_min_target = order.density_min * (1.0 - relax)
                    density_max_target = order.density_max * (1.0 + relax)
                    sulfur_target = order.sulfur_max_ppm * (1.0 + relax)
                    olefin_target = order.olefin_max_pct * (1.0 + relax)
                    spec_rule = "Intermediate batch limited to 10% off-spec buffer"

                batch_schedule_rows.append(
                    {
                        "batch_id": batch_id,
                        "order_id": order.order_id,
                        "display_name": display_name,
                        "grade_name": order.grade_name,
                        "market_type": order.market_type,
                        "sulfur_class": order.sulfur_class,
                        "region": order.region,
                        "blender": order.service,
                        "tank_id": tank_id,
                        "tank_group": f"T{tank_group}",
                        "stage_in_tank": stage,
                        "batch_volume_m3": round(order.batch_volume_m3, 2),
                        "start_day": round(batch_start_hour / 24.0, 2),
                        "finish_day": round(batch_finish_hour / 24.0, 2),
                        "due_day": order.due_day,
                        "late_hours": round(late_hours_value, 2),
                    }
                )
                batch_quality_rows.append(
                    {
                        "batch_id": batch_id,
                        "order_id": order.order_id,
                        "display_name": display_name,
                        "tank_group": f"T{tank_group}",
                        "stage_in_tank": stage,
                        "rule": spec_rule,
                        "actual_ron": round(actual_ron, 2),
                        "target_ron_min": round(ron_target, 2),
                        "actual_rvp": round(actual_rvp, 2),
                        "target_rvp_max": round(rvp_target, 2),
                        "actual_density": round(actual_density, 4),
                        "target_density_min": round(density_min_target, 4),
                        "target_density_max": round(density_max_target, 4),
                        "actual_sulfur_ppm": round(actual_sulfur, 2),
                        "target_sulfur_max_ppm": round(sulfur_target, 2),
                        "actual_olefin_pct": round(actual_olefin, 2),
                        "target_olefin_max_pct": round(olefin_target, 2),
                    }
                )

                for name in spec_weights:
                    slack_val = float(pulp.value(spec_slack[(order.order_id, tank_group, stage, name)]) or 0.0)
                    if slack_val > 1e-8:
                        slack_rows.append(
                            {
                                "category": "Spec",
                                "entity": batch_id,
                                "name": name,
                                "value": round(slack_val, 4),
                            }
                        )

        for component, qty in order_component_totals.items():
            if qty > 1e-6:
                blend_rows.append(
                    {
                        "order_id": order.order_id,
                        "display_name": display_name,
                        "component": component,
                        "volume_m3": round(qty, 2),
                        "share_pct": round((qty / order.volume_m3) * 100.0, 2),
                    }
                )

        tank_slack_val = float(pulp.value(tank_capacity_slack[(order.order_id, chosen_slot)]) or 0.0)
        if tank_slack_val > 1e-8:
            slack_rows.append({"category": "Tank capacity", "entity": order.order_id, "name": "tank_capacity_slack", "value": round(tank_slack_val, 2)})

    for component in component_names:
        slack_val = float(pulp.value(inventory_slack[component]) or 0.0)
        if slack_val > 1e-8:
            slack_rows.append({"category": "Inventory", "entity": component, "name": "horizon_total", "value": round(slack_val, 2)})

    schedule_df = pd.DataFrame(schedule_rows).sort_values(["start_day", "blender", "order_id"]).reset_index(drop=True)
    schedule_df["sequence_rank"] = schedule_df.groupby("blender").cumcount()
    batch_schedule_df = pd.DataFrame(batch_schedule_rows).sort_values(["start_day", "blender", "batch_id"]).reset_index(drop=True)
    batch_recipe_df = pd.DataFrame(batch_recipe_rows).sort_values(["batch_id", "share_pct"], ascending=[True, False]).reset_index(drop=True)
    batch_quality_df = pd.DataFrame(batch_quality_rows).sort_values(["batch_id"]).reset_index(drop=True)
    orders_lookup = orders.set_index("order_id").to_dict("index")
    resequence_t0 = perf_counter()
    schedule_df, batch_schedule_df, tank_usage_df, carryover_penalty_cost = _apply_local_tank_resequence(
        schedule_df=schedule_df,
        batch_schedule_df=batch_schedule_df,
        tanks_df=tanks,
        orders_lookup=orders_lookup,
    )
    resequence_sec = perf_counter() - resequence_t0
    qc_t0 = perf_counter()
    schedule_df, batch_schedule_df = _reschedule_batches_with_qc(
        schedule_df=schedule_df,
        batch_schedule_df=batch_schedule_df,
        qc_hours=cfg["qc_hours"],
        batch_hours=cfg["batch_hours"],
        blender_gap_hours=2.0,
    )
    qc_reschedule_sec = perf_counter() - qc_t0
    blend_df = pd.DataFrame(blend_rows).sort_values(["order_id", "share_pct"], ascending=[True, False]).reset_index(drop=True)
    component_summary_df = pd.DataFrame(
        [{"component": component, "consumed_m3": round(volume, 2)} for component, volume in component_summary.items()]
    ).sort_values("consumed_m3", ascending=False).reset_index(drop=True)
    order_summary_df = (
        schedule_df[["order_id", "display_name", "blender", "market_type", "sulfur_class", "grade_name", "volume_m3", "due_day", "finish_day", "late_hours"]]
        .sort_values(["blender", "due_day", "order_id"])
        .reset_index(drop=True)
    )
    slack_summary_df = pd.DataFrame(slack_rows).sort_values(["category", "entity", "name"]).reset_index(drop=True) if slack_rows else pd.DataFrame(columns=["category", "entity", "name", "value"])

    deadstock_t0 = perf_counter()
    deadstock_result = refine_deadstock_plan(
        schedule_df=schedule_df,
        batch_schedule_df=batch_schedule_df,
        orders_df=orders,
        blendstocks_df=blendstocks,
        tanks_df=tanks,
        settings=cfg,
    )
    repair_iters = 0
    while (
        repair_iters < 4
        and not deadstock_result.inventory_profile_df.empty
        and float(deadstock_result.inventory_profile_df["inventory_m3"].min()) < -1e-6
    ):
        repaired_schedule_df, repaired_batch_df = _repair_inventory_shortages(
            schedule_df=schedule_df,
            batch_schedule_df=batch_schedule_df,
            blend_df=deadstock_result.blend_df,
            inventory_profile_df=deadstock_result.inventory_profile_df,
            orders_df=orders,
            qc_hours=cfg["qc_hours"],
            batch_hours=cfg["batch_hours"],
            blender_gap_hours=2.0,
            max_iters=1,
            shift_days=1.0,
        )
        if repaired_schedule_df[["order_id", "start_day", "finish_day"]].equals(schedule_df[["order_id", "start_day", "finish_day"]]):
            break
        schedule_df = repaired_schedule_df
        batch_schedule_df = repaired_batch_df
        deadstock_result = refine_deadstock_plan(
            schedule_df=schedule_df,
            batch_schedule_df=batch_schedule_df,
            orders_df=orders,
            blendstocks_df=blendstocks,
            tanks_df=tanks,
            settings=cfg,
        )
        repair_iters += 1
    deadstock_sec = perf_counter() - deadstock_t0

    blend_df = deadstock_result.blend_df
    batch_recipe_df = deadstock_result.batch_recipe_df
    batch_quality_df = deadstock_result.batch_quality_df
    component_summary_df = deadstock_result.component_summary_df
    inventory_profile_df = deadstock_result.inventory_profile_df
    tank_level_profile_df = deadstock_result.tank_level_profile_df
    if deadstock_result.slack_summary_df.empty:
        slack_summary_df = slack_summary_df.copy()
    elif slack_summary_df.empty:
        slack_summary_df = deadstock_result.slack_summary_df.copy()
    else:
        slack_summary_df = (
            pd.concat([slack_summary_df, deadstock_result.slack_summary_df], ignore_index=True)
            .sort_values(["category", "entity", "name"])
            .reset_index(drop=True)
        )

    blend_cost_total = deadstock_result.blend_cost
    spec_slack_total = float(pulp.value(spec_slack_expr) or 0.0) + deadstock_result.spec_slack_cost
    inventory_slack_total = float(pulp.value(inventory_slack_expr) or 0.0) + deadstock_result.inventory_slack_cost
    objective_total = (
        blend_cost_total
        + float(pulp.value(demurrage_expr) or 0.0)
        + float(pulp.value(domestic_late_expr) or 0.0)
        + spec_slack_total
        + inventory_slack_total
        + float(pulp.value(tank_capacity_slack_expr) or 0.0)
        + carryover_penalty_cost
    )

    schedule_df = _attach_calendar_columns(schedule_df, cfg["schedule_start"], {"start_day": "start", "finish_day": "finish", "due_day": "due"})
    batch_schedule_df = _attach_calendar_columns(batch_schedule_df, cfg["schedule_start"], {"start_day": "start", "finish_day": "finish", "due_day": "due"})
    tank_usage_df = _attach_calendar_columns(tank_usage_df, cfg["schedule_start"], {"start_day": "start", "finish_day": "finish"})
    inventory_profile_df = _attach_calendar_columns(inventory_profile_df, cfg["schedule_start"], {"day": "snapshot"})
    tank_level_profile_df = _attach_calendar_columns(tank_level_profile_df, cfg["schedule_start"], {"day": "snapshot"})
    order_summary_df = _attach_calendar_columns(order_summary_df, cfg["schedule_start"], {"finish_day": "finish", "due_day": "due"})

    total_solve_sec = perf_counter() - total_t0

    return SolveArtifacts(
        status=status,
        objective_value=objective_total,
        blend_cost=blend_cost_total,
        demurrage_cost=float(pulp.value(demurrage_expr) or 0.0),
        domestic_late_cost=float(pulp.value(domestic_late_expr) or 0.0),
        spec_slack_cost=spec_slack_total,
        inventory_slack_cost=inventory_slack_total,
        tank_capacity_slack_cost=float(pulp.value(tank_capacity_slack_expr) or 0.0),
        carryover_penalty_cost=carryover_penalty_cost,
        schedule_df=schedule_df,
        batch_schedule_df=batch_schedule_df,
        batch_recipe_df=batch_recipe_df,
        batch_quality_df=batch_quality_df,
        tank_usage_df=tank_usage_df,
        inventory_profile_df=inventory_profile_df,
        tank_level_profile_df=tank_level_profile_df,
        blend_df=blend_df,
        component_summary_df=component_summary_df,
        order_summary_df=order_summary_df,
        slack_summary_df=slack_summary_df,
        solver_message=f"{status} solution found with {len(schedule_df)} campaigns and {len(batch_schedule_df)} scheduled batches.",
        sanitize_sec=sanitize_sec,
        milp_sec=milp_sec,
        resequence_sec=resequence_sec,
        qc_reschedule_sec=qc_reschedule_sec,
        deadstock_sec=deadstock_sec,
        total_solve_sec=total_solve_sec,
    )
