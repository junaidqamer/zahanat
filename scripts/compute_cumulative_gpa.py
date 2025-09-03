#!/usr/bin/env python3
"""
Compute cumulative GPA for 2021-22 through 2023-24, replicating the provided Stata .do logic.

Key steps (mirroring .do):
1) Pull current-year (e.g., 2023) High School course and grade data from DSN "SIF" tables.
2) Merge StudentMarks, CourseInfo, CourseFlag, School, MarkDefinition.
3) Derive subject and course_description from CourseCD rules.
4) Join with biographic CSV (filter to grades 09-12), compute GPA aggregates for current year.
5) Load prior-year CSV(s) (2021-22, 2022-23), compute analogous aggregates.
6) Append all, collapse (sum) by student_id, compute cumulative GPA, round to 0.01, and save to .dta.

CLI example:
  python scripts/compute_cumulative_gpa.py \
    --dsn SIF \
    --year 2023 \
    --biog-csv "/mnt/r/Assessment & Accountability/RPSG/SOUPS/Student_Biographic June Status (Split Pea Soup)/DATA/INTERNAL/UNSCRAMBLED/2023-24_June-Biog_PK-12_internal_PRELIM.csv" \
    --prev-courses \
      "2021-22=/mnt/r/.../2021-22_HsCourseAndGrades.csv" \
      "2022-23=/mnt/r/.../2022-23_HsCourseAndGrades.csv" \
    --output "/mnt/r/.../2021-2 to 2023-24 Cumulative GPA.dta"

Notes:
- Paths using Windows drive letters (e.g., R:\) should be mounted/accessible from this environment.
- This script uses pyodbc via a configured ODBC DSN.
"""

from __future__ import annotations

import argparse
import sys
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import sqlalchemy as sa


def parse_prev_courses(pairs: List[str]) -> List[Tuple[str, str]]:
    """Parse --prev-courses arguments of the form "label=/absolute/path.csv".

    Returns list of (label, path) pairs. Label is only used for logging; school_year is read from the CSV column.
    """
    results: List[Tuple[str, str]] = []
    for item in pairs or []:
        if "=" not in item:
            raise ValueError(f"Invalid --prev-courses value: {item!r}. Expected label=/abs/path.csv")
        label, path = item.split("=", 1)
        if not path.lower().endswith(".csv"):
            raise ValueError(f"--prev-courses path must be a .csv file: {path}")
        results.append((label.strip(), path.strip()))
    return results


def read_sql_table(engine: sa.engine.Engine, sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, con=engine)


def build_odbc_engine(dsn: str) -> sa.engine.Engine:
    # Use SQLAlchemy with pyodbc and DSN
    # Trusts a configured ODBC DSN named `dsn`
    url = sa.engine.URL.create(
        "mssql+pyodbc",
        query={"dsn": dsn, "TrustServerCertificate": "yes"},
    )
    engine = sa.create_engine(url, fast_executemany=True)
    return engine


def load_current_year_data(dsn: str, year: int) -> Dict[str, pd.DataFrame]:
    engine = build_odbc_engine(dsn)

    # StudentMarks
    df_studentmarks = read_sql_table(
        engine,
        f"""
        SELECT DISTINCT SchoolYear, StudentID, SchoolDBN, TermCD, CourseCD, Section, Credits, Mark, MarkingPeriod
        FROM SIF.dbo.Hsst_tbl_StudentMarks
        WHERE SchoolYear = {year} AND Credits > 0 AND IsFinal = 1 AND IsExam = 0
        """,
    )

    # CourseInfo
    df_courseinfo = read_sql_table(
        engine,
        f"""
        SELECT DISTINCT SchoolYear, SchoolDBN, TermCD, CourseCD, CourseTitle, GradeAverageFactor
        FROM SIF.dbo.Hsst_tbl_CourseInfo
        WHERE SchoolYear = {year}
        """,
    )

    # CourseFlag (dedupe within group)
    df_courseflag = read_sql_table(
        engine,
        f"""
        SELECT DISTINCT SchoolYear, SchoolDBN, TermCD, CourseCD, GradeAveragedFlag, MarkingPeriod
        FROM SIF.dbo.Hsst_tbl_CourseFlag
        WHERE SchoolYear = {year}
        """,
    )
    df_courseflag = df_courseflag.sort_values(
        ["SchoolYear", "SchoolDBN", "TermCD", "CourseCD", "MarkingPeriod"]
    ).drop_duplicates(
        subset=["SchoolYear", "SchoolDBN", "TermCD", "CourseCD", "MarkingPeriod"], keep="first"
    )

    # School
    df_school = read_sql_table(
        engine,
        """
        SELECT DISTINCT SchoolDBN, NumericSchoolDBN
        FROM SIF.STARS.School
        """,
    )

    # MarkDefinition (rename Term -> TermCD)
    df_markdef = read_sql_table(
        engine,
        f"""
        SELECT DISTINCT SchoolYear, NumericSchoolDBN, Term AS TermCD, Mark, isPassing, AlphaEquivalent, NumericEquivalent
        FROM SIF.STARS.vw_MarkDefinition
        WHERE SchoolYear = {year}
        """,
    )

    return {
        "StudentMarks": df_studentmarks,
        "CourseInfo": df_courseinfo,
        "CourseFlag": df_courseflag,
        "School": df_school,
        "MarkDefinition": df_markdef,
    }


def derive_subject_and_description(df: pd.DataFrame) -> pd.DataFrame:
    # Initialize
    df = df.copy()
    df["subject"] = ""
    # Use first char of CourseCD
    first = df["CourseCD"].astype(str).str.slice(0, 1)
    sixth = df["CourseCD"].astype(str).str.slice(5, 6)
    fourth = df["CourseCD"].astype(str).str.slice(3, 4)

    df.loc[first == "E", "subject"] = "English"
    df.loc[first == "H", "subject"] = "Social Studies"
    df.loc[first == "M", "subject"] = "Mathematics"
    df.loc[first == "S", "subject"] = "Science"
    df.loc[first == "F", "subject"] = "Foreign Language"
    df.loc[first == "P", "subject"] = "Physical Education & Health"
    df.loc[first.isin(["A", "U", "D", "C"]), "subject"] = "Arts"
    df.loc[first == "T", "subject"] = "Technology"
    df.loc[first == "R", "subject"] = "Career Development"
    df.loc[first == "B", "subject"] = "Business"
    df.loc[first == "K", "subject"] = "Human Services"
    df.loc[first == "G", "subject"] = "Guidance"
    df.loc[first == "Z", "subject"] = "Undefined"

    # Adjust for elementary
    df.loc[fourth == "J", "subject"] = ""

    # course_description
    df["course_description"] = ""
    df.loc[sixth == "H", "course_description"] = "Honors"
    df.loc[sixth == "X", "course_description"] = "Advanced Placement (AP)"
    df.loc[sixth == "B", "course_description"] = "International Baccalaureate (IB)"
    df.loc[sixth == "U", "course_description"] = "College-Level: College Credit"
    df.loc[sixth == "C", "course_description"] = "College-Level: Non-College Credit"
    df.loc[sixth == "T", "course_description"] = "CTE"
    df.loc[sixth == "S", "course_description"] = "Non-Credit/Remediation"
    df.loc[sixth == "P", "course_description"] = "Exam Preparation"
    df.loc[sixth == "Q", "course_description"] = "N/A"

    # Adjust for elementary
    df.loc[fourth == "J", "course_description"] = ""
    # Accelerated middle school
    df.loc[(sixth == "A") & (fourth == "M"), "course_description"] = "Accelerated"
    df.loc[(sixth.isin(["X", "B", "U", "C", "P"])) & (fourth == "M"), "course_description"] = ""

    return df


def compute_current_year_coursegrades(dsn: str, year: int) -> pd.DataFrame:
    tables = load_current_year_data(dsn=dsn, year=year)

    marks = tables["StudentMarks"].copy()
    info = tables["CourseInfo"].copy()
    flag = tables["CourseFlag"].copy()
    school = tables["School"].copy()
    markdef = tables["MarkDefinition"].copy()

    # Merge StudentMarks + CourseInfo (inner)
    m1 = pd.merge(
        marks,
        info,
        on=["SchoolYear", "SchoolDBN", "TermCD", "CourseCD"],
        how="inner",
        validate="m:1",
    )

    # Merge with CourseFlag (keep left rows even if no match; drop right-only not applicable in pandas)
    m2 = pd.merge(
        m1,
        flag,
        on=["SchoolYear", "SchoolDBN", "TermCD", "CourseCD", "MarkingPeriod"],
        how="left",
        validate="m:1",
    )

    # Merge with School (inner)
    m3 = pd.merge(
        m2,
        school,
        on=["SchoolDBN"],
        how="inner",
        validate="m:1",
    )

    # Merge with MarkDefinition (left; drop right-only not relevant)
    m4 = pd.merge(
        m3,
        markdef,
        left_on=["SchoolYear", "NumericSchoolDBN", "TermCD", "Mark"],
        right_on=["SchoolYear", "NumericSchoolDBN", "TermCD", "Mark"],
        how="left",
        validate="m:1",
    )

    m4["dataset"] = "High School"

    m4 = derive_subject_and_description(m4)

    # Rename and lower-case
    rename_map = {
        "SchoolYear": "school_year",
        "StudentID": "student_id",
        "SchoolDBN": "dbn",
        "TermCD": "term_code",
        "CourseCD": "course_code",
        "CourseTitle": "course_title",
        "GradeAverageFactor": "grade_average_factor",
        "GradeAveragedFlag": "grade_averaged_flag",
        "isPassing": "is_passing",
        "AlphaEquivalent": "alpha_equivalent",
        "NumericEquivalent": "numeric_equivalent",
    }
    m4 = m4.rename(columns=rename_map)
    # Lower-case remaining columns to mimic Stata rename *, lower
    m4.columns = [c.lower() for c in m4.columns]

    # Column order similar to Stata
    preferred_cols = [
        "school_year",
        "student_id",
        "dbn",
        "term_code",
        "section",
        "course_code",
        "course_title",
        "subject",
        "course_description",
        "credits",
        "mark",
        "grade_average_factor",
        "grade_averaged_flag",
        "is_passing",
        "alpha_equivalent",
        "numeric_equivalent",
    ]
    # Only keep those that exist; retain all others afterward
    cols = [c for c in preferred_cols if c in m4.columns] + [
        c for c in m4.columns if c not in preferred_cols
    ]
    m4 = m4.loc[:, cols]
    return m4


def compute_yearly_totals(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    # Equivalent of: drop if grade_averaged_flag==0 | numeric_equivalent==.
    # Treat missing numeric_equivalent as NaN
    d = d[~((d.get("grade_averaged_flag").fillna(0) == 0) | (d.get("numeric_equivalent").isna()))]

    # is_passing could be boolean or numeric; coerce to float
    is_passing = pd.to_numeric(d.get("is_passing"), errors="coerce")
    credits = pd.to_numeric(d.get("credits"), errors="coerce")
    grade_average_factor = pd.to_numeric(d.get("grade_average_factor"), errors="coerce")
    numeric_equivalent = pd.to_numeric(d.get("numeric_equivalent"), errors="coerce")

    d["tot_cred_earned"] = credits * is_passing
    d["tot_gpa_pts"] = numeric_equivalent * credits * grade_average_factor

    # rename credits to tot_cred_att
    d = d.rename(columns={"credits": "tot_cred_att"})

    # Collapse by school_year and student_id summing tot_*
    group_cols = [c for c in ["school_year", "student_id"] if c in d.columns]
    sum_cols = [c for c in d.columns if c.startswith("tot_")]
    out = (
        d.groupby(group_cols, dropna=False)[sum_cols]
        .sum(min_count=1)
        .reset_index()
    )
    return out


def load_prior_courses(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # Normalize expected columns to lowercase
    df.columns = [c.lower() for c in df.columns]
    # Ensure required columns exist
    required = {
        "school_year",
        "student_id",
        "credits",
        "is_passing",
        "numeric_equivalent",
        "grade_average_factor",
        "grade_averaged_flag",
    }
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Prior course CSV missing required columns {missing}. File: {csv_path}"
        )
    return df


def compute_cumulative_gpa(
    dsn: str,
    year: int,
    biog_csv: str,
    prev_courses: List[Tuple[str, str]],
    output_path: str,
) -> None:
    # Current-year course grades from DB
    coursegrades = compute_current_year_coursegrades(dsn=dsn, year=year)

    # Load biographic CSV and filter grades 09-12, inner-join to coursegrades by student_id
    biog = pd.read_csv(biog_csv)
    biog.columns = [c.lower() for c in biog.columns]
    if "grade_level" not in biog.columns:
        raise ValueError("Biographic CSV must include column 'grade_level'")
    if "student_id" not in biog.columns:
        raise ValueError("Biographic CSV must include column 'student_id'")

    biog = biog[biog["grade_level"].astype(str).isin(["09", "10", "11", "12"])].drop(
        columns=["grade_level"], errors="ignore"
    )

    merged = pd.merge(
        biog,
        coursegrades,
        on="student_id",
        how="inner",
        validate="1:m",
    )

    # is_passing set to NaN where mark is missing (to mimic Stata replace is_passing=. if missing(mark))
    if "mark" in merged.columns and "is_passing" in merged.columns:
        merged.loc[merged["mark"].isna(), "is_passing"] = np.nan

    totals_current = compute_yearly_totals(merged)

    # Prior years
    prior_totals: List[pd.DataFrame] = []
    for label, path in prev_courses:
        prior_df = load_prior_courses(path)
        totals = compute_yearly_totals(prior_df)
        prior_totals.append(totals)

    all_frames = [totals_current] + prior_totals
    all_cat = pd.concat(all_frames, ignore_index=True, sort=False)

    # Collapse(sum) tot_* by student_id
    sum_cols = [c for c in all_cat.columns if c.startswith("tot_")]
    cumulative = (
        all_cat.groupby(["student_id"], dropna=False)[sum_cols]
        .sum(min_count=1)
        .reset_index()
    )

    # Compute GPA
    cumulative["tot_gpa"] = cumulative["tot_gpa_pts"] / cumulative["tot_cred_att"]
    cumulative["tot_gpa"] = cumulative["tot_gpa"].round(2)

    # Save to .dta
    cumulative.to_stata(output_path, write_index=False)


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="Compute cumulative GPA replicating Stata .do workflow")
    ap.add_argument("--dsn", required=True, help="ODBC DSN name (e.g., SIF)")
    ap.add_argument("--year", type=int, required=True, help="Current SchoolYear integer (e.g., 2023)")
    ap.add_argument("--biog-csv", required=True, help="Path to current-year biographic CSV")
    ap.add_argument(
        "--prev-courses",
        nargs="*",
        default=[],
        help="Zero or more items like LABEL=/abs/path.csv for prior years (e.g., 2021-22, 2022-23)",
    )
    ap.add_argument("--output", required=True, help="Output .dta path")
    args = ap.parse_args(argv)

    prev_pairs = parse_prev_courses(args.prev_courses)

    compute_cumulative_gpa(
        dsn=args.dsn,
        year=args.year,
        biog_csv=args.biog_csv,
        prev_courses=prev_pairs,
        output_path=args.output,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

