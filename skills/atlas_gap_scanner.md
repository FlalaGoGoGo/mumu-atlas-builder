# Atlas Gap Scanner (MuMu Atlas Builder)

## Purpose
Scan the current MuMu seed datasets (museums, artworks, artists, exhibitions) to quantify missingness, coverage, and enrichment priorities. This skill outputs a reproducible run folder with a gap report and a ranked backlog for subsequent skills.

## Inputs
- seed_dir (string): directory containing seed CSVs (default: `data/seed`)
- run_id (string): optional run id; if empty, use timestamp like `run_YYYYMMDD_HHMMSS`
- targets:
  - target_artworks_per_museum (int): default 100
  - target_exhibitions_per_museum (int): default 30
- focus (optional):
  - focus_museum_ids (list[string])
  - focus_countries (list[string])

## Outputs (written to `runs/<run_id>/`)
- `gap_report.md`: human-readable summary
- `gap_report.json`: machine-readable stats
- `backlog.json`: ranked tasks for enrichment
- `copies/`: snapshot copy of seed CSVs used in this run

## Guardrails
- Do NOT invent or enrich any data in this skill.
- Only read and analyze CSVs, then write reports.
- If a column is missing, record it as missing and continue.

## How to Run (reproducible)
1. Install deps: `pip install -r requirements.txt`
2. Run: `python scripts/gap_scanner.py --seed_dir data/seed --target_artworks_per_museum 100 --target_exhibitions_per_museum 30`
3. Check outputs in `runs/<run_id>/`

## Failure Modes
- Missing CSV file → stop with clear error
- Missing key columns (museum_id / artist_id) → continue, but record limitations in report
