from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd
import pulp


@dataclass
class DeadstockRefineResult:
    blend_cost: float
    spec_slack_cost: float
    inventory_slack_cost: float
    batch_recipe_df: pd.DataFrame
    batch_quality_df: pd.DataFrame
    blend_df: pd.DataFrame
    component_summary_df: pd.DataFrame
    slack_summary_df: pd.DataFrame
    inventory_profile_df: pd.DataFrame
    tank_level_profile_df: pd.DataFrame


def refine_deadstock_plan(
    schedule_df: pd.DataFrame,
    batch_schedule_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    blendstocks_df: pd.DataFrame,
    tanks_df: pd.DataFrame,
    settings: Dict[str, float],
) -> DeadstockRefineResult:
    component_names = blendstocks_df["component"].tolist()
    component_props = blendstocks_df.set_index("component").to_dict("index")
    tank_capacity = tanks_df.set_index("tank_id")["capacity_m3"].to_dict()
    order_map = orders_df.set_index("order_id").to_dict("index")
    initial_tank_volume = tanks_df.set_index("tank_id")["initial_volume_m3"].to_dict() if "initial_volume_m3" in tanks_df.columns else {}

    deadstock_fraction = float(settings.get("deadstock_fraction", 0.20))
    relax = float(settings.get("interim_relax_pct", 0.10))
    base_spec_penalty = float(settings.get("base_spec_penalty", 100.0))
    hard_slack_penalty = float(settings.get("hard_slack_penalty", 1000.0))
    inventory_slack_penalty = float(settings.get("inventory_slack_penalty", hard_slack_penalty))
    key_mult = float(settings.get("key_spec_multiplier", 2.0))

    spec_weights = {
        "ron_short": base_spec_penalty * key_mult,
        "rvp_excess": base_spec_penalty * key_mult,
        "density_low": base_spec_penalty,
        "density_high": base_spec_penalty,
        "sulfur_excess": base_spec_penalty * key_mult,
        "olefin_excess": base_spec_penalty,
    }

    initial_inventory = {row.component: float(row.initial_inventory) for row in blendstocks_df.itertuples()}
    rundown_per_hour = {row.component: float(row.rundown_per_hour) for row in blendstocks_df.itertuples()}
    max_inventory = {
        row.component: float(getattr(row, "max_inventory_m3", 4000.0) or 4000.0)
        for row in blendstocks_df.itertuples()
    }
    consumed_so_far = {component: 0.0 for component in component_names}

    batch_recipe_rows = []
    batch_quality_rows = []
    blend_rows = []
    slack_rows = []
    component_summary = {component: 0.0 for component in component_names}
    tank_state: Dict[str, Dict[str, float]] = {}
    for tank in tanks_df.itertuples():
        initial_volume = float(getattr(tank, "initial_volume_m3", 0.0) or 0.0)
        if initial_volume > 1e-6:
            tank_state[tank.tank_id] = {
                "volume_m3": initial_volume,
                "ron": float(getattr(tank, "initial_ron", 0.0) or 0.0),
                "rvp": float(getattr(tank, "initial_rvp", 0.0) or 0.0),
                "density": float(getattr(tank, "initial_density", 0.0) or 0.0),
                "sulfur_ppm": float(getattr(tank, "initial_sulfur_ppm", 0.0) or 0.0),
                "olefin_pct": float(getattr(tank, "initial_olefin_pct", 0.0) or 0.0),
            }

    tank_groups = (
        batch_schedule_df.sort_values(["tank_id", "start_day"])
        .groupby(["tank_id", "order_id"], as_index=False)
        .agg(
            display_name=("display_name", "first"),
            stages=("stage_in_tank", "count"),
            batch_volume_m3=("batch_volume_m3", "first"),
            first_start=("start_day", "min"),
            campaign_finish=("finish_day", "max"),
        )
        .sort_values(["first_start", "tank_id"])
    )

    for group in tank_groups.itertuples():
        order = order_map[group.order_id]
        tank_id = group.tank_id
        carryover = tank_state.get(tank_id, {})
        carryover_volume = float(carryover.get("volume_m3", 0.0)) if carryover else 0.0
        carryover_props = carryover if carryover else {
            "ron": 0.0,
            "rvp": 0.0,
            "density": 0.0,
            "sulfur_ppm": 0.0,
            "olefin_pct": 0.0,
        }

        lp = pulp.LpProblem(f"deadstock_refine_{group.order_id}_{tank_id}", pulp.LpMinimize)
        current_hour = float(group.first_start) * 24.0
        available_now = {
            component: initial_inventory[component] + rundown_per_hour[component] * current_hour - consumed_so_far[component]
            for component in component_names
        }
        overflow_before = {
            component: max(0.0, available_now[component] - max_inventory[component])
            for component in component_names
        }
        x = {
            (stage, component): pulp.LpVariable(f"x__{group.order_id}__{tank_id}__{stage}__{component}", lowBound=0)
            for stage in range(1, group.stages + 1)
            for component in component_names
        }
        spec_slack = {
            (stage, name): pulp.LpVariable(f"slack__{group.order_id}__{tank_id}__{stage}__{name}", lowBound=0)
            for stage in range(1, group.stages + 1)
            for name in spec_weights
        }
        inv_slack = {
            component: pulp.LpVariable(f"inv__{group.order_id}__{tank_id}__{component}", lowBound=0)
            for component in component_names
        }

        for stage in range(1, group.stages + 1):
            lp += pulp.lpSum(x[(stage, component)] for component in component_names) == group.batch_volume_m3

            cumulative_volume = carryover_volume + stage * group.batch_volume_m3
            cumulative = {
                component: pulp.lpSum(x[(stage_idx, component)] for stage_idx in range(1, stage + 1))
                for component in component_names
            }

            if stage == group.stages:
                ron_target = float(order["ron_min"])
                rvp_target = float(order["rvp_max"])
                density_min_target = float(order["density_min"])
                density_max_target = float(order["density_max"])
                sulfur_target = float(order["sulfur_max_ppm"])
                olefin_target = float(order["olefin_max_pct"])
            else:
                ron_target = float(order["ron_min"]) * (1.0 - relax)
                rvp_target = float(order["rvp_max"]) * (1.0 + relax)
                density_min_target = float(order["density_min"]) * (1.0 - relax)
                density_max_target = float(order["density_max"]) * (1.0 + relax)
                sulfur_target = float(order["sulfur_max_ppm"]) * (1.0 + relax)
                olefin_target = float(order["olefin_max_pct"]) * (1.0 + relax)

            ron_expr = carryover_props["ron"] * carryover_volume + pulp.lpSum(component_props[c]["ron"] * cumulative[c] for c in component_names)
            rvp_expr = carryover_props["rvp"] * carryover_volume + pulp.lpSum(component_props[c]["rvp"] * cumulative[c] for c in component_names)
            density_expr = carryover_props["density"] * carryover_volume + pulp.lpSum(component_props[c]["density"] * cumulative[c] for c in component_names)
            sulfur_expr = carryover_props["sulfur_ppm"] * carryover_volume + pulp.lpSum(component_props[c]["sulfur_ppm"] * cumulative[c] for c in component_names)
            olefin_expr = carryover_props["olefin_pct"] * carryover_volume + pulp.lpSum(component_props[c]["olefin_pct"] * cumulative[c] for c in component_names)

            lp += ron_expr + spec_slack[(stage, "ron_short")] * cumulative_volume >= ron_target * cumulative_volume
            lp += rvp_expr <= (rvp_target + spec_slack[(stage, "rvp_excess")]) * cumulative_volume
            lp += density_expr + spec_slack[(stage, "density_low")] * cumulative_volume >= density_min_target * cumulative_volume
            lp += density_expr <= (density_max_target + spec_slack[(stage, "density_high")]) * cumulative_volume
            lp += sulfur_expr <= (sulfur_target + spec_slack[(stage, "sulfur_excess")]) * cumulative_volume
            lp += olefin_expr <= (olefin_target + spec_slack[(stage, "olefin_excess")]) * cumulative_volume

        for component in component_names:
            lp += pulp.lpSum(x[(stage, component)] for stage in range(1, group.stages + 1)) <= max(0.0, available_now[component]) + inv_slack[component]

        lp += (
            pulp.lpSum(component_props[component]["cost_per_m3"] * x[(stage, component)] for stage in range(1, group.stages + 1) for component in component_names)
            + pulp.lpSum(spec_weights[name] * spec_slack[(stage, name)] * (carryover_volume + stage * group.batch_volume_m3) for stage in range(1, group.stages + 1) for name in spec_weights)
            + inventory_slack_penalty * pulp.lpSum(inv_slack.values())
            - pulp.lpSum(0.15 * overflow_before[component] * pulp.lpSum(x[(stage, component)] for stage in range(1, group.stages + 1)) for component in component_names)
        )

        solver = pulp.HiGHS(msg=False, timeLimit=3, gapRel=0.05)
        lp.solve(solver)

        used_totals = {component: 0.0 for component in component_names}
        cumulative = {component: 0.0 for component in component_names}
        tank_batches = batch_schedule_df.loc[(batch_schedule_df["tank_id"] == tank_id) & (batch_schedule_df["order_id"] == group.order_id)].sort_values("stage_in_tank")

        for batch in tank_batches.itertuples():
            heel_volume = carryover_volume + sum(cumulative.values())
            if heel_volume > 1e-6:
                heel_ron = (carryover_props["ron"] * carryover_volume + sum(component_props[c]["ron"] * cumulative[c] for c in component_names)) / heel_volume
                heel_rvp = (carryover_props["rvp"] * carryover_volume + sum(component_props[c]["rvp"] * cumulative[c] for c in component_names)) / heel_volume
                heel_density = (carryover_props["density"] * carryover_volume + sum(component_props[c]["density"] * cumulative[c] for c in component_names)) / heel_volume
                heel_sulfur = (carryover_props["sulfur_ppm"] * carryover_volume + sum(component_props[c]["sulfur_ppm"] * cumulative[c] for c in component_names)) / heel_volume
                heel_olefin = (carryover_props["olefin_pct"] * carryover_volume + sum(component_props[c]["olefin_pct"] * cumulative[c] for c in component_names)) / heel_volume
            else:
                heel_ron = 0.0
                heel_rvp = 0.0
                heel_density = 0.0
                heel_sulfur = 0.0
                heel_olefin = 0.0

            if heel_volume > 1e-6:
                batch_recipe_rows.append(
                    {
                        "batch_id": batch.batch_id,
                        "order_id": batch.order_id,
                        "display_name": batch.display_name,
                        "tank_group": batch.tank_group,
                        "stage_in_tank": batch.stage_in_tank,
                        "component": "HEEL",
                        "volume_m3": round(heel_volume, 2),
                        "share_pct": None,
                        "component_ron": None,
                        "component_rvp": None,
                        "component_density": None,
                        "component_sulfur_ppm": None,
                        "component_olefin_pct": None,
                    }
                    )
            for component in component_names:
                qty = float(pulp.value(x[(int(batch.stage_in_tank), component)]) or 0.0)
                if qty > 1e-6:
                    cumulative[component] += qty
                    used_totals[component] += qty
                    component_summary[component] += qty
                    batch_recipe_rows.append(
                        {
                            "batch_id": batch.batch_id,
                            "order_id": batch.order_id,
                            "display_name": batch.display_name,
                            "tank_group": batch.tank_group,
                            "stage_in_tank": batch.stage_in_tank,
                            "component": component,
                            "volume_m3": round(qty, 2),
                            "share_pct": round((qty / batch.batch_volume_m3) * 100.0, 2),
                            "component_ron": component_props[component]["ron"],
                            "component_rvp": component_props[component]["rvp"],
                            "component_density": component_props[component]["density"],
                            "component_sulfur_ppm": component_props[component]["sulfur_ppm"],
                            "component_olefin_pct": component_props[component]["olefin_pct"],
                        }
                    )

            total_volume = carryover_volume + sum(cumulative.values())
            actual_ron = (carryover_props["ron"] * carryover_volume + sum(component_props[c]["ron"] * cumulative[c] for c in component_names)) / total_volume
            actual_rvp = (carryover_props["rvp"] * carryover_volume + sum(component_props[c]["rvp"] * cumulative[c] for c in component_names)) / total_volume
            actual_density = (carryover_props["density"] * carryover_volume + sum(component_props[c]["density"] * cumulative[c] for c in component_names)) / total_volume
            actual_sulfur = (carryover_props["sulfur_ppm"] * carryover_volume + sum(component_props[c]["sulfur_ppm"] * cumulative[c] for c in component_names)) / total_volume
            actual_olefin = (carryover_props["olefin_pct"] * carryover_volume + sum(component_props[c]["olefin_pct"] * cumulative[c] for c in component_names)) / total_volume

            if batch.stage_in_tank == group.stages:
                rule = "Final tank batch with deadstock carryover"
                ron_target = float(order["ron_min"])
                rvp_target = float(order["rvp_max"])
                density_min_target = float(order["density_min"])
                density_max_target = float(order["density_max"])
                sulfur_target = float(order["sulfur_max_ppm"])
                olefin_target = float(order["olefin_max_pct"])
            else:
                rule = "Intermediate batch with 10% off-spec buffer and deadstock carryover"
                ron_target = float(order["ron_min"]) * (1.0 - relax)
                rvp_target = float(order["rvp_max"]) * (1.0 + relax)
                density_min_target = float(order["density_min"]) * (1.0 - relax)
                density_max_target = float(order["density_max"]) * (1.0 + relax)
                sulfur_target = float(order["sulfur_max_ppm"]) * (1.0 + relax)
                olefin_target = float(order["olefin_max_pct"]) * (1.0 + relax)

            batch_quality_rows.append(
                {
                    "batch_id": batch.batch_id,
                    "order_id": batch.order_id,
                    "display_name": batch.display_name,
                    "tank_group": batch.tank_group,
                    "stage_in_tank": batch.stage_in_tank,
                    "rule": rule,
                    "heel_volume_m3": round(heel_volume, 2),
                    "heel_ron": round(heel_ron, 2),
                    "heel_rvp": round(heel_rvp, 2),
                    "heel_density": round(heel_density, 4),
                    "heel_sulfur_ppm": round(heel_sulfur, 2),
                    "heel_olefin_pct": round(heel_olefin, 2),
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
                slack_val = float(pulp.value(spec_slack[(int(batch.stage_in_tank), name)]) or 0.0)
                if slack_val > 1e-8:
                    slack_rows.append({"category": "Spec", "entity": batch.batch_id, "name": f"deadstock_{name}", "value": round(slack_val, 4)})

        for component in component_names:
            used = used_totals[component]
            consumed_so_far[component] += used
            inv_val = float(pulp.value(inv_slack[component]) or 0.0)
            if inv_val > 1e-8:
                slack_rows.append({"category": "Inventory", "entity": component, "name": f"{tank_id}_{group.order_id}", "value": round(inv_val, 2)})

        total_final = carryover_volume + group.stages * group.batch_volume_m3
        residual_volume = min(tank_capacity[tank_id] * deadstock_fraction, total_final)
        tank_state[tank_id] = {
            "volume_m3": residual_volume,
            "ron": (carryover_props["ron"] * carryover_volume + sum(component_props[c]["ron"] * used_totals[c] for c in component_names)) / total_final,
            "rvp": (carryover_props["rvp"] * carryover_volume + sum(component_props[c]["rvp"] * used_totals[c] for c in component_names)) / total_final,
            "density": (carryover_props["density"] * carryover_volume + sum(component_props[c]["density"] * used_totals[c] for c in component_names)) / total_final,
            "sulfur_ppm": (carryover_props["sulfur_ppm"] * carryover_volume + sum(component_props[c]["sulfur_ppm"] * used_totals[c] for c in component_names)) / total_final,
            "olefin_pct": (carryover_props["olefin_pct"] * carryover_volume + sum(component_props[c]["olefin_pct"] * used_totals[c] for c in component_names)) / total_final,
        }

        for component, qty in used_totals.items():
            if qty > 1e-6:
                blend_rows.append(
                    {
                        "order_id": group.order_id,
                        "display_name": group.display_name,
                        "component": component,
                        "volume_m3": round(qty, 2),
                        "share_pct": round(qty / (group.batch_volume_m3 * group.stages) * 100.0, 2),
                    }
                )

    batch_recipe_df = pd.DataFrame(batch_recipe_rows).sort_values(["batch_id", "share_pct"], ascending=[True, False]).reset_index(drop=True)
    batch_quality_df = pd.DataFrame(batch_quality_rows).sort_values(["batch_id"]).reset_index(drop=True)
    blend_df = pd.DataFrame(blend_rows).sort_values(["order_id", "share_pct"], ascending=[True, False]).reset_index(drop=True)
    component_summary_df = (
        pd.DataFrame([{"component": c, "consumed_m3": round(v, 2)} for c, v in component_summary.items()])
        .sort_values("consumed_m3", ascending=False)
        .reset_index(drop=True)
    )
    slack_summary_df = pd.DataFrame(slack_rows).sort_values(["category", "entity", "name"]).reset_index(drop=True) if slack_rows else pd.DataFrame(columns=["category", "entity", "name", "value"])

    profile_points = [round(x * 0.25, 2) for x in range(0, int(settings["horizon_days"] * 4) + 1)]
    inventory_profile_rows = []
    initial_map = blendstocks_df.set_index("component")["initial_inventory"].to_dict()
    rundown_map = blendstocks_df.set_index("component")["rundown_per_hour"].to_dict()
    inv_events = (
        batch_recipe_df.groupby(["component", "batch_id"], as_index=False)["volume_m3"].sum()
        .merge(batch_schedule_df[["batch_id", "finish_day"]], on="batch_id", how="left")
        .sort_values(["component", "finish_day"])
    )
    for component in component_names:
        comp_events = inv_events.loc[inv_events["component"] == component]
        for day in profile_points:
            consumed = comp_events.loc[comp_events["finish_day"] <= day, "volume_m3"].sum()
            inventory_profile_rows.append(
                {
                    "component": component,
                    "day": day,
                    "inventory_m3": round(initial_map[component] + rundown_map[component] * day * 24 - consumed, 2),
                }
            )
    inventory_profile_df = pd.DataFrame(inventory_profile_rows)

    tank_level_rows = []
    tank_campaigns = (
        batch_schedule_df.groupby(["tank_id", "order_id"], as_index=False)
        .agg(start_day=("start_day", "min"), finish_day=("finish_day", "max"), filled_m3=("batch_volume_m3", "sum"))
        .sort_values(["tank_id", "start_day", "finish_day"])
    )
    due_map = orders_df.set_index("order_id")["due_day"].to_dict()
    for tank in tanks_df.itertuples():
        deadstock_volume = tank.capacity_m3 * deadstock_fraction
        current_level = float(initial_tank_volume.get(tank.tank_id, 0.0) or 0.0)
        events = []
        tank_campaign_rows = tank_campaigns.loc[tank_campaigns["tank_id"] == tank.tank_id].reset_index(drop=True)
        for idx, campaign in enumerate(tank_campaign_rows.itertuples()):
            events.append((float(campaign.finish_day), "fill", float(campaign.filled_m3)))
            next_start = float(tank_campaign_rows.iloc[idx + 1]["start_day"]) if idx + 1 < len(tank_campaign_rows) else float(settings["horizon_days"])
            due_day = float(due_map.get(campaign.order_id, campaign.finish_day))
            discharge_day = min(next_start, max(float(campaign.finish_day), due_day))
            discharge_amount = max(0.0, current_level + float(campaign.filled_m3) - deadstock_volume)
            current_level = current_level + float(campaign.filled_m3) - discharge_amount
            events.append((discharge_day, "discharge", discharge_amount))
        events.sort(key=lambda item: (item[0], 0 if item[1] == "fill" else 1))
        for day in profile_points:
            level = float(initial_tank_volume.get(tank.tank_id, 0.0) or 0.0)
            for event_day, action, amount in events:
                if event_day <= day:
                    level = level + amount if action == "fill" else max(0.0, level - amount)
            tank_level_rows.append(
                {
                    "tank_id": tank.tank_id,
                    "service": tank.service,
                    "day": day,
                    "level_m3": round(level, 2),
                    "fill_pct": round((level / tank.capacity_m3) * 100.0, 2) if tank.capacity_m3 else 0.0,
                }
            )
    tank_level_profile_df = pd.DataFrame(tank_level_rows)

    blend_cost = float((batch_recipe_df["volume_m3"] * batch_recipe_df["component"].map(blendstocks_df.set_index("component")["cost_per_m3"].to_dict())).sum()) if not batch_recipe_df.empty else 0.0
    spec_slack_cost = float(slack_summary_df.loc[slack_summary_df["category"] == "Spec", "value"].sum() * base_spec_penalty) if not slack_summary_df.empty else 0.0
    inventory_slack_cost = float(slack_summary_df.loc[slack_summary_df["category"] == "Inventory", "value"].sum() * inventory_slack_penalty) if not slack_summary_df.empty else 0.0

    return DeadstockRefineResult(
        blend_cost=blend_cost,
        spec_slack_cost=spec_slack_cost,
        inventory_slack_cost=inventory_slack_cost,
        batch_recipe_df=batch_recipe_df,
        batch_quality_df=batch_quality_df,
        blend_df=blend_df,
        component_summary_df=component_summary_df,
        slack_summary_df=slack_summary_df,
        inventory_profile_df=inventory_profile_df,
        tank_level_profile_df=tank_level_profile_df,
    )
