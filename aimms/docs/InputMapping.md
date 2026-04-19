# Input Mapping for AIMMS 4.7.2

This document maps the current Excel / Streamlit inputs to the AIMMS identifiers defined in [Identifiers.ams](/Users/eksong/Documents/석준/batch_scheduling/aimms/src/Identifiers.ams).

## 1. Settings Sheet

Suggested AIMMS import target:

- scalar parameters in `BatchSchedulingData`

| Input field | AIMMS identifier |
| --- | --- |
| `schedule_start` | `ScheduleStartSerial` |
| `schedule_end` | `ScheduleEndSerial` |
| `batch_hours` | `BatchHours` |
| `horizon_days` | `HorizonDays` |
| `batches_per_order` | `BatchesPerOrder` |
| `tanks_per_order` | `TanksPerOrder` |
| `qc_hours` | `QCHours` |
| `solver_time_limit_sec` | `SolverTimeLimitSec` |
| `mip_gap_rel` | `MipGapRel` |
| `base_spec_penalty` | `BaseSpecPenalty` |
| `hard_slack_penalty` | `HardSlackPenalty` |
| `inventory_slack_penalty` | `InventorySlackPenalty` |
| `key_spec_multiplier` | `KeySpecMultiplier` |
| `domestic_late_penalty` | `DomesticLatePenalty` |
| `interim_relax_pct` | `InterimRelaxPct` |
| `deadstock_fraction` | `DeadstockFraction` |
| `min_blender_gap_hours` | `MinBlenderGapHours` |

## 2. Sales Orders Sheet

Suggested index domain:

- primary key: `Orders`

| Input field | AIMMS identifier |
| --- | --- |
| `order_id` | element in `Orders` |
| `market_type` | `OrderMarketType(o)` |
| `sulfur_class` | `OrderSulfurClass(o)` |
| `service` | `OrderService(o)` |
| `grade_name` | `GradeName(o)` |
| `region` | `Region(o)` |
| `display_name` or short label | `DisplayCode(o)` |
| `due_day` | `DueDay(o)` |
| `volume_m3` | `Volume(o)` |
| `ron_min` | `RonMin(o)` |
| `rvp_max` | `RvpMax(o)` |
| `density_min` | `DensityMin(o)` |
| `density_max` | `DensityMax(o)` |
| `sulfur_max_ppm` | `SulfurMax(o)` |
| `olefin_max_pct` | `OlefinMax(o)` |
| `demurrage_per_hour` | `DemurragePerHour(o)` |

## 3. Blendstocks Sheet

Suggested index domain:

- primary key: `Components`

| Input field | AIMMS identifier |
| --- | --- |
| `component` | element in `Components` |
| `ron` | `CompRon(c)` |
| `rvp` | `CompRvp(c)` |
| `density` | `CompDensity(c)` |
| `sulfur_ppm` | `CompSulfur(c)` |
| `olefin_pct` | `CompOlefin(c)` |
| `cost_per_m3` | `CostPerM3(c)` |
| `initial_inventory` | `InitialInventory(c)` |
| `rundown_per_hour` | `RundownPerHour(c)` |
| `max_inventory_m3` | `MaxInventory(c)` |

## 4. Product Tanks Sheet

Suggested index domain:

- primary key: `Tanks`

| Input field | AIMMS identifier |
| --- | --- |
| `tank_id` | element in `Tanks` |
| `service` | `TankService(t)` |
| `capacity_m3` | `TankCapacity(t)` |
| `initial_volume_m3` | `InitialTankVolume(t)` |
| `initial_ron` | `InitialTankRon(t)` |
| `initial_rvp` | `InitialTankRvp(t)` |
| `initial_density` | `InitialTankDensity(t)` |
| `initial_sulfur_ppm` | `InitialTankSulfur(t)` |
| `initial_olefin_pct` | `InitialTankOlefin(t)` |

## 5. Derived Identifiers

The following are calculated after import:

- `HorizonSlots`
- `StagesPerTank`
- `QCSlots`
- `OccupancySlots`
- `LastStartSlot`
- `TankDeadstockMin(t)`
- `DueHour(o)`
- `HalfVolume(o)`
- `BatchVolume(o)`

## 6. Recommended Data Load Sequence

1. load scalar settings,
2. load orders,
3. load blendstocks,
4. load tanks,
5. call `InitializeSettings`,
6. call `ValidateInputs`,
7. call `SolveBatchScheduling`.
