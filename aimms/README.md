# AIMMS 4.7.2 Conversion Package

This folder contains an AIMMS-oriented source conversion of the current Python refinery batch scheduling prototype.

The goal of this package is to provide:

- an AIMMS 4.7.2 compatible algebraic skeleton for the master MIP,
- procedural placeholders for the Python heuristics that are currently executed after the master solve,
- a clear mapping from the existing Excel/Streamlit inputs to AIMMS identifiers.

This is not a binary `.aimms` project export. Instead, it is a text-first AIMMS source package intended to be imported into a new AIMMS 4.7.2 project.

## Folder Layout

- [BatchSchedulingMain.ams](/Users/eksong/Documents/석준/batch_scheduling/aimms/BatchSchedulingMain.ams)
- [src/Identifiers.ams](/Users/eksong/Documents/석준/batch_scheduling/aimms/src/Identifiers.ams)
- [src/MasterModel.ams](/Users/eksong/Documents/석준/batch_scheduling/aimms/src/MasterModel.ams)
- [src/Heuristics.ams](/Users/eksong/Documents/석준/batch_scheduling/aimms/src/Heuristics.ams)
- [src/Workflow.ams](/Users/eksong/Documents/석준/batch_scheduling/aimms/src/Workflow.ams)
- [docs/InputMapping.md](/Users/eksong/Documents/석준/batch_scheduling/aimms/docs/InputMapping.md)
- [docs/ConversionNotes.md](/Users/eksong/Documents/석준/batch_scheduling/aimms/docs/ConversionNotes.md)

## What Was Converted

The current Python code has two layers:

1. Master mixed-integer scheduling model in `optimizer.py`
2. Post-processing heuristics in `optimizer.py` and `deadstock_heuristic.py`

The AIMMS conversion mirrors that split:

- `MasterModel.ams` contains the main MIP structure:
  - order start selection,
  - tank assignment,
  - batch-level blending quantities,
  - cumulative spec constraints,
  - tardiness,
  - inventory slack,
  - tank capacity slack,
  - objective decomposition.
- `Heuristics.ams` contains procedural stubs and data structures for:
  - local tank resequencing,
  - QC-aware batch timing adjustment,
  - deadstock / heel carryover refinement,
  - inventory shortage repair.
- `Workflow.ams` orchestrates the end-to-end run sequence so that the AIMMS model follows the same overall logic as the Python prototype.

## Recommended AIMMS 4.7.2 Import Steps

1. Create a new empty AIMMS 4.7.2 project.
2. Add a main model file and use [BatchSchedulingMain.ams](/Users/eksong/Documents/석준/batch_scheduling/aimms/BatchSchedulingMain.ams).
3. Add the files in `src/` as model source files.
4. Define data pages or data import procedures that load:
   - `Settings`
   - `Sales Orders`
   - `Blendstocks`
   - `Product Tanks`
5. Connect those imports to the identifiers documented in [docs/InputMapping.md](/Users/eksong/Documents/석준/batch_scheduling/aimms/docs/InputMapping.md).
6. Start with the `SolveBatchScheduling` procedure in [src/Workflow.ams](/Users/eksong/Documents/석준/batch_scheduling/aimms/src/Workflow.ams).

## Practical Scope

This package focuses on the model and workflow translation. The Streamlit user interface was not reimplemented as AIMMS pages in this first pass.

For AIMMS page development, the recommended next step is:

- create one page for data input,
- one page for solve controls and summary KPIs,
- one page for Gantt / tank level / inventory charts,
- one page for batch detail drilldown.

## Python-to-AIMMS Traceability

- Python master MIP: [optimizer.py](/Users/eksong/Documents/석준/batch_scheduling/optimizer.py)
- Python deadstock refinement: [deadstock_heuristic.py](/Users/eksong/Documents/석준/batch_scheduling/deadstock_heuristic.py)
- Input helper: [input_manager.py](/Users/eksong/Documents/석준/batch_scheduling/input_manager.py)
- Example data: [sample_data.py](/Users/eksong/Documents/석준/batch_scheduling/sample_data.py)

## Notes

- The master MIP translation is the most direct and complete part.
- The heuristic layer is represented as AIMMS procedures and temporary parameters so that the logic can be implemented incrementally.
- The deadstock refinement remains procedural, which is consistent with the current Python architecture.
