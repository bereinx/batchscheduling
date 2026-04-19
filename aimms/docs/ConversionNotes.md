# Conversion Notes

## Conversion Philosophy

The Python implementation is not a single monolithic optimization model. It is a layered solver:

1. master MIP,
2. local resequencing heuristic,
3. QC-aware time repair,
4. deadstock / heel LP refinement,
5. inventory shortage repair.

The AIMMS conversion keeps the same architecture. This matters because the Python implementation already uses decomposition to stay within short solve times.

## What Is Exact vs. Approximate

### Directly translated

- one-start-per-order campaign decision
- service-level single-blender capacity
- tank occupancy
- per-batch volume balance
- cumulative quality constraints
- tardiness
- global component inventory balance over the horizon
- objective decomposition and penalties

### Procedural in AIMMS

- assigning chosen tanks into explicit tank groups
- local tank resequencing
- enforcing QC-driven batch timing with same-tank sequence logic
- deadstock-aware heel carryover update
- inventory shortage repair iterations

These parts were already procedural in Python, so keeping them procedural in AIMMS is a natural translation.

## Suggested Next AIMMS Implementation Steps

1. Build import procedures from the Excel template.
2. Add a second mathematical program for the tank-order deadstock LP.
3. Implement a deterministic ordering rule for assigning selected tanks to `TankGroups`.
4. Add result tables for:
   - order summary,
   - batch schedule,
   - batch recipe,
   - batch quality,
   - inventory profile,
   - tank level profile.
5. Build AIMMS pages for planning input and result review.

## Mapping to Existing Python Functions

| Python function | AIMMS location |
| --- | --- |
| `sanitize_inputs` | `InitializeSettings`, `ValidateInputs` |
| `solve_batch_schedule` | `SolveBatchScheduling`, `SolveMasterMIP` |
| `_apply_local_tank_resequence` | `ApplyLocalTankResequence` |
| `_reschedule_batches_with_qc` | `ApplyQCReschedule` |
| `refine_deadstock_plan` | `RefineDeadstockPlan` |
| `_repair_inventory_shortages` | `RepairInventoryShortages` |

## Known Gaps

- No AIMMS page objects are included yet.
- No binary `.aimms` project container is included.
- The heuristic procedures are intentionally documented and scaffolded, but not fully coded line-by-line in AIMMS in this first pass.
- Charting and Streamlit-specific presentation logic are not part of this package.

## Why This Is Still Useful

This package gives a strong migration baseline:

- naming and index structure are already normalized,
- the master optimization core is translated,
- the post-processing sequence is defined,
- the data contract is documented.

That makes the remaining work mostly AIMMS implementation effort, not architecture discovery.
