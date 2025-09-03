### Cumulative GPA conversion (Stata .do to Python)

This repo includes a Python script that reproduces the provided Stata `.do` logic using pandas and an ODBC DSN for SQL access.

Install deps:

```bash
pip install -r requirements.txt
```

Run (example):

```bash
python scripts/compute_cumulative_gpa.py \
  --dsn SIF \
  --year 2023 \
  --biog-csv "/mnt/r/Assessment & Accountability/RPSG/SOUPS/Student_Biographic June Status (Split Pea Soup)/DATA/INTERNAL/UNSCRAMBLED/2023-24_June-Biog_PK-12_internal_PRELIM.csv" \
  --prev-courses "2021-22=/mnt/r/.../2021-22_HsCourseAndGrades.csv" "2022-23=/mnt/r/.../2022-23_HsCourseAndGrades.csv" \
  --output "/mnt/r/.../2021-2 to 2023-24 Cumulative GPA.dta"
```

Notes:
- Ensure the `SIF` ODBC DSN exists and is reachable from this environment.
- Mount or otherwise make accessible any network paths referenced (e.g., Windows `R:\` paths).
- Prior-year CSVs are expected to contain: `school_year, student_id, credits, is_passing, numeric_equivalent, grade_average_factor, grade_averaged_flag`.
# zahanat
a custom algorithmic trading portfolio project
