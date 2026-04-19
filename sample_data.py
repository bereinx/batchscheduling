from __future__ import annotations

import random
from calendar import monthrange
from datetime import date, datetime, time, timedelta
from typing import Dict

import pandas as pd


def make_blendstocks_example() -> pd.DataFrame:
    rows = [
        {"component": "LCN", "ron": 72.0, "rvp": 78.0, "density": 0.680, "sulfur_ppm": 25.0, "olefin_pct": 2.0, "cost_per_m3": 520.0, "initial_inventory": 1300.0, "rundown_per_hour": 5.5, "max_inventory_m3": 4000.0},
        {"component": "HCN", "ron": 82.0, "rvp": 58.0, "density": 0.720, "sulfur_ppm": 80.0, "olefin_pct": 4.0, "cost_per_m3": 610.0, "initial_inventory": 1300.0, "rundown_per_hour": 4.95, "max_inventory_m3": 4000.0},
        {"component": "Reformate", "ron": 98.0, "rvp": 22.0, "density": 0.790, "sulfur_ppm": 4.0, "olefin_pct": 2.0, "cost_per_m3": 840.0, "initial_inventory": 3000.0, "rundown_per_hour": 9.6, "max_inventory_m3": 4000.0},
        {"component": "Isomerate", "ron": 88.0, "rvp": 72.0, "density": 0.690, "sulfur_ppm": 2.0, "olefin_pct": 1.0, "cost_per_m3": 760.0, "initial_inventory": 700.0, "rundown_per_hour": 3.52, "max_inventory_m3": 4000.0},
        {"component": "Alkylate", "ron": 96.0, "rvp": 34.0, "density": 0.700, "sulfur_ppm": 3.0, "olefin_pct": 1.0, "cost_per_m3": 900.0, "initial_inventory": 1600.0, "rundown_per_hour": 5.5, "max_inventory_m3": 4000.0},
        {"component": "FCC Gasoline", "ron": 92.0, "rvp": 46.0, "density": 0.750, "sulfur_ppm": 260.0, "olefin_pct": 28.0, "cost_per_m3": 690.0, "initial_inventory": 700.0, "rundown_per_hour": 7.15, "max_inventory_m3": 4000.0},
        {"component": "Hydrocrackate", "ron": 84.0, "rvp": 42.0, "density": 0.740, "sulfur_ppm": 12.0, "olefin_pct": 8.0, "cost_per_m3": 640.0, "initial_inventory": 2800.0, "rundown_per_hour": 6.3, "max_inventory_m3": 4000.0},
        {"component": "Coker Naphtha", "ron": 78.0, "rvp": 61.0, "density": 0.770, "sulfur_ppm": 320.0, "olefin_pct": 18.0, "cost_per_m3": 540.0, "initial_inventory": 600.0, "rundown_per_hour": 3.08, "max_inventory_m3": 4000.0},
        {"component": "MTBE", "ron": 118.0, "rvp": 8.0, "density": 0.740, "sulfur_ppm": 0.0, "olefin_pct": 0.0, "cost_per_m3": 930.0, "initial_inventory": 1600.0, "rundown_per_hour": 2.45, "max_inventory_m3": 4000.0},
        {"component": "Butane", "ron": 92.0, "rvp": 120.0, "density": 0.580, "sulfur_ppm": 0.0, "olefin_pct": 0.0, "cost_per_m3": 470.0, "initial_inventory": 1300.0, "rundown_per_hour": 2.4, "max_inventory_m3": 4000.0},
    ]
    return pd.DataFrame(rows)


def _service_spec_targets(orders_df: pd.DataFrame) -> dict[str, dict[str, float]]:
    working = orders_df.copy()
    working["service"] = working["market_type"].str.upper() + "_" + working["sulfur_class"].str.upper()
    targets: dict[str, dict[str, float]] = {}
    for service, service_orders in working.groupby("service"):
        density_center = ((service_orders["density_min"] + service_orders["density_max"]) / 2.0).mean()
        targets[service] = {
            "ron": round(service_orders["ron_min"].mean() * 1.05, 2),
            "rvp": round(service_orders["rvp_max"].mean() * 0.95, 2),
            "density": round(density_center, 4),
            "sulfur_ppm": round(service_orders["sulfur_max_ppm"].mean() * 0.95, 2),
            "olefin_pct": round(service_orders["olefin_max_pct"].mean() * 0.95, 2),
        }
    return targets


def _make_tanks_for_orders(orders: pd.DataFrame) -> pd.DataFrame:
    service_targets = _service_spec_targets(orders)
    rng = random.Random(7)
    rows = []
    for prefix, count in [("EXPORT_LS", 4), ("EXPORT_HS", 4), ("DOMESTIC_LS", 2)]:
        capacity = 2200.0 if prefix.startswith("EXPORT") else 1800.0
        deadstock = capacity * 0.20
        target = service_targets[prefix]
        for index in range(1, count + 1):
            initial_volume = round(rng.uniform(deadstock + 20.0, capacity * 0.50), 1)
            rows.append(
                {
                    "tank_id": f"{prefix}_TK{index}",
                    "service": prefix,
                    "capacity_m3": capacity,
                    "initial_volume_m3": initial_volume,
                    "initial_ron": target["ron"],
                    "initial_rvp": target["rvp"],
                    "initial_density": target["density"],
                    "initial_sulfur_ppm": target["sulfur_ppm"],
                    "initial_olefin_pct": target["olefin_pct"],
                }
            )
    return pd.DataFrame(rows)


def make_tanks_example() -> pd.DataFrame:
    return _make_tanks_for_orders(make_orders_example())


def _domestic_orders() -> list[dict]:
    rows = []
    for cycle in range(1, 4):
        due_day = cycle * 2
        rows.append(
            {
                "order_id": f"DLS_{cycle:02d}",
                "market_type": "DOMESTIC",
                "sulfur_class": "LS",
                "grade_name": "Domestic Regular LS",
                "region": "Korea",
                "due_day": due_day,
                "due_hour": 18,
                "volume_m3": 750.0,
                "ron_min": 91.0,
                "rvp_max": 65.0,
                "density_min": 0.720,
                "density_max": 0.775,
                "sulfur_max_ppm": 10.0,
                "olefin_max_pct": 18.0,
                "demurrage_per_hour": 0.0,
            }
        )
    return rows


def _export_orders() -> list[dict]:
    return [
        {"order_id": "ELS_AUS91", "market_type": "EXPORT", "sulfur_class": "LS", "grade_name": "Australia ULP 91", "region": "Australia", "due_day": 2, "due_hour": 18, "volume_m3": 1100.0, "ron_min": 91.0, "rvp_max": 62.0, "density_min": 0.720, "density_max": 0.775, "sulfur_max_ppm": 10.0, "olefin_max_pct": 18.0, "demurrage_per_hour": 280.0},
        {"order_id": "ELS_AUS95", "market_type": "EXPORT", "sulfur_class": "LS", "grade_name": "Australia PULP 95", "region": "Australia", "due_day": 5, "due_hour": 18, "volume_m3": 1100.0, "ron_min": 95.0, "rvp_max": 60.0, "density_min": 0.720, "density_max": 0.780, "sulfur_max_ppm": 10.0, "olefin_max_pct": 18.0, "demurrage_per_hour": 330.0},
        {"order_id": "EHS_IDN90", "market_type": "EXPORT", "sulfur_class": "HS", "grade_name": "Indonesia 90", "region": "Southeast Asia", "due_day": 2, "due_hour": 18, "volume_m3": 1150.0, "ron_min": 90.0, "rvp_max": 67.0, "density_min": 0.720, "density_max": 0.780, "sulfur_max_ppm": 150.0, "olefin_max_pct": 22.0, "demurrage_per_hour": 250.0},
        {"order_id": "EHS_VNM92", "market_type": "EXPORT", "sulfur_class": "HS", "grade_name": "Vietnam 92", "region": "Southeast Asia", "due_day": 4, "due_hour": 18, "volume_m3": 1100.0, "ron_min": 92.0, "rvp_max": 66.0, "density_min": 0.720, "density_max": 0.782, "sulfur_max_ppm": 150.0, "olefin_max_pct": 22.0, "demurrage_per_hour": 260.0},
    ]


def make_orders_example() -> pd.DataFrame:
    orders = pd.DataFrame(_domestic_orders() + _export_orders())
    start_date = datetime.fromisoformat(make_settings_example()["schedule_start"])
    orders["due_at"] = [
        start_date + timedelta(days=float(due_day) - 1.0, hours=float(due_hour))
        for due_day, due_hour in zip(orders["due_day"], orders["due_hour"])
    ]
    return orders


def make_settings_example() -> Dict[str, float]:
    today = date.today()
    days_until_next_monday = (7 - today.weekday()) % 7
    days_until_next_monday = 7 if days_until_next_monday == 0 else days_until_next_monday
    next_monday = today + timedelta(days=days_until_next_monday)
    next_sunday = next_monday + timedelta(days=6)
    return {
        "horizon_days": 7,
        "schedule_start": datetime.combine(next_monday, time(0, 0)).isoformat(),
        "schedule_end": datetime.combine(next_sunday, time(23, 59)).isoformat(),
        "batch_hours": 4,
        "batches_per_order": 6,
        "tanks_per_order": 2,
        "qc_hours": 8,
        "solver_time_limit_sec": 20,
        "mip_gap_rel": 0.20,
        "deadstock_fraction": 0.20,
        "base_spec_penalty": 100.0,
        "hard_slack_penalty": 1000.0,
        "inventory_slack_penalty": 5000.0,
        "key_spec_multiplier": 2.0,
        "domestic_late_penalty": 5000.0,
        "interim_relax_pct": 0.10,
    }


def _add_months(anchor: date, months: int) -> date:
    month_index = anchor.month - 1 + months
    year = anchor.year + month_index // 12
    month = month_index % 12 + 1
    day = min(anchor.day, monthrange(year, month)[1])
    return date(year, month, day)


def make_two_month_settings_example() -> Dict[str, float]:
    settings = make_settings_example().copy()
    start_date = datetime.fromisoformat(settings["schedule_start"]).date()
    end_date = _add_months(start_date, 2) - timedelta(days=1)
    settings["schedule_start"] = datetime.combine(start_date, time(0, 0)).isoformat()
    settings["schedule_end"] = datetime.combine(end_date, time(23, 59)).isoformat()
    settings["horizon_days"] = (end_date - start_date).days + 1
    settings["solver_time_limit_sec"] = 60
    return settings


def make_two_month_orders_example() -> pd.DataFrame:
    settings = make_two_month_settings_example()
    start_date = datetime.fromisoformat(settings["schedule_start"])
    horizon_days = int(settings["horizon_days"])

    domestic_template = {
        "market_type": "DOMESTIC",
        "sulfur_class": "LS",
        "grade_name": "Domestic Regular LS",
        "region": "Korea",
        "volume_m3": 750.0,
        "ron_min": 91.0,
        "rvp_max": 65.0,
        "density_min": 0.720,
        "density_max": 0.775,
        "sulfur_max_ppm": 10.0,
        "olefin_max_pct": 18.0,
        "demurrage_per_hour": 0.0,
        "due_hour": 18,
    }
    export_ls_templates = [
        {"suffix": "AUS91", "grade_name": "Australia ULP 91", "region": "Australia", "volume_m3": 1100.0, "ron_min": 91.0, "rvp_max": 62.0, "density_min": 0.720, "density_max": 0.775, "sulfur_max_ppm": 10.0, "olefin_max_pct": 18.0, "demurrage_per_hour": 280.0},
        {"suffix": "AUS95", "grade_name": "Australia PULP 95", "region": "Australia", "volume_m3": 1100.0, "ron_min": 95.0, "rvp_max": 60.0, "density_min": 0.720, "density_max": 0.780, "sulfur_max_ppm": 10.0, "olefin_max_pct": 18.0, "demurrage_per_hour": 330.0},
        {"suffix": "JPN90", "grade_name": "Japan Regular", "region": "Japan", "volume_m3": 1050.0, "ron_min": 90.0, "rvp_max": 65.0, "density_min": 0.720, "density_max": 0.775, "sulfur_max_ppm": 10.0, "olefin_max_pct": 18.0, "demurrage_per_hour": 300.0},
        {"suffix": "JPN95", "grade_name": "Japan Premium", "region": "Japan", "volume_m3": 1000.0, "ron_min": 95.0, "rvp_max": 60.0, "density_min": 0.720, "density_max": 0.780, "sulfur_max_ppm": 10.0, "olefin_max_pct": 18.0, "demurrage_per_hour": 350.0},
        {"suffix": "SGP92", "grade_name": "Singapore 92", "region": "Southeast Asia", "volume_m3": 1150.0, "ron_min": 92.0, "rvp_max": 64.0, "density_min": 0.720, "density_max": 0.778, "sulfur_max_ppm": 10.0, "olefin_max_pct": 18.0, "demurrage_per_hour": 320.0},
    ]
    export_hs_templates = [
        {"suffix": "IDN90", "grade_name": "Indonesia 90", "region": "Southeast Asia", "volume_m3": 1150.0, "ron_min": 90.0, "rvp_max": 67.0, "density_min": 0.720, "density_max": 0.780, "sulfur_max_ppm": 150.0, "olefin_max_pct": 22.0, "demurrage_per_hour": 250.0},
        {"suffix": "VNM92", "grade_name": "Vietnam 92", "region": "Southeast Asia", "volume_m3": 1100.0, "ron_min": 92.0, "rvp_max": 66.0, "density_min": 0.720, "density_max": 0.782, "sulfur_max_ppm": 150.0, "olefin_max_pct": 22.0, "demurrage_per_hour": 260.0},
        {"suffix": "THA91", "grade_name": "Thailand 91", "region": "Southeast Asia", "volume_m3": 1000.0, "ron_min": 91.0, "rvp_max": 66.0, "density_min": 0.720, "density_max": 0.780, "sulfur_max_ppm": 100.0, "olefin_max_pct": 20.0, "demurrage_per_hour": 275.0},
        {"suffix": "PHL95", "grade_name": "Philippines 95", "region": "Southeast Asia", "volume_m3": 950.0, "ron_min": 95.0, "rvp_max": 63.0, "density_min": 0.720, "density_max": 0.780, "sulfur_max_ppm": 120.0, "olefin_max_pct": 18.0, "demurrage_per_hour": 290.0},
        {"suffix": "SEA93", "grade_name": "SEA 93 Pool", "region": "Southeast Asia", "volume_m3": 1050.0, "ron_min": 93.0, "rvp_max": 64.0, "density_min": 0.720, "density_max": 0.782, "sulfur_max_ppm": 180.0, "olefin_max_pct": 22.0, "demurrage_per_hour": 295.0},
    ]

    rows: list[dict] = []
    domestic_due_days = list(range(2, horizon_days + 1, 2))
    for idx, due_day in enumerate(domestic_due_days, start=1):
        row = domestic_template.copy()
        row["order_id"] = f"DLS_{idx:03d}"
        row["due_day"] = due_day
        rows.append(row)

    ls_due_days = [3 * i for i in range(1, 21)]
    hs_due_days = [3 * i + 1 for i in range(1, 21)]
    for idx, due_day in enumerate(ls_due_days, start=1):
        template = export_ls_templates[(idx - 1) % len(export_ls_templates)].copy()
        template["order_id"] = f"ELS_{template['suffix']}_{idx:02d}"
        template["market_type"] = "EXPORT"
        template["sulfur_class"] = "LS"
        template["due_day"] = min(due_day, horizon_days)
        template["due_hour"] = 18
        rows.append(template)
    for idx, due_day in enumerate(hs_due_days, start=1):
        template = export_hs_templates[(idx - 1) % len(export_hs_templates)].copy()
        template["order_id"] = f"EHS_{template['suffix']}_{idx:02d}"
        template["market_type"] = "EXPORT"
        template["sulfur_class"] = "HS"
        template["due_day"] = min(due_day, horizon_days)
        template["due_hour"] = 18
        rows.append(template)

    orders = pd.DataFrame(rows)
    orders["due_at"] = [
        start_date + timedelta(days=float(due_day) - 1.0, hours=float(due_hour))
        for due_day, due_hour in zip(orders["due_day"], orders["due_hour"])
    ]
    return orders


def make_two_month_tanks_example() -> pd.DataFrame:
    return _make_tanks_for_orders(make_two_month_orders_example())
