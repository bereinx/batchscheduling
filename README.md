# Batch Scheduling

Streamlit-based refinery gasoline blending and batch scheduling tool.

## What It Does

- Builds practical batch schedules for domestic/export gasoline orders
- Accounts for:
  - due dates and demurrage
  - product specs: `RON`, `RVP`, `Density`, `Sulfur`, `Olefin`
  - blender capacity and minimum gap
  - tank heel/deadstock behavior
  - blendstock inventory and rundown
- Provides:
  - Planning UI with editable inputs
  - Excel input template upload
  - Campaign Gantt
  - Batch detail tables
  - Tank level and blendstock inventory profiles
  - Runtime breakdown and summary views

## Main Files

- `app.py`: Streamlit UI
- `input_manager.py`: consolidated input handling and Excel import logic
- `optimizer.py`: master MILP, resequencing, timing heuristics, inventory repair
- `deadstock_heuristic.py`: heel/deadstock-aware post-processing
- `sample_data.py`: default example scenario and sample master data
- `outputs/`: manuals, executive summary, PPT, Excel input template

## Default Example Scenario

- Planning horizon: 2 months
- Domestic LS: every 2 days
- Export LS: 20 orders
- Export HS: 20 orders
- Total orders: 70
- Total batches: 420

## Run Locally

```bash
pip install -r requirements.txt
python3 -m streamlit run app.py
```

Open `http://localhost:8501`

## Excel Input

Use the provided template:

- `outputs/batch_scheduling_input_template.xlsx`

Expected sheets:

- `Settings`
- `Orders`
- `Blendstocks`
- `Tanks`

## Included Deliverables

- `outputs/Batch_Scheduling_Documentation.docx`
- `outputs/Batch_Scheduling_Documentation.html`
- `outputs/Batch_Scheduling_Executive_Summary.docx`
- `outputs/Batch_Scheduling_Executive_Summary.html`
- `outputs/Batch_Scheduling_Executive_Summary_Presentation.pptx`

## Suggested Git Steps

```bash
git init
git add .
git commit -m "Initial batch scheduling project"
git branch -M main
git remote add origin <YOUR_REPO_URL>
git push -u origin main
```
