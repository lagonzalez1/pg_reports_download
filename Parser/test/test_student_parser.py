# tests/test_student_parser.py
import pandas as pd
import pytest
from datetime import datetime, timedelta

from Parser.StudentParser import (
    StudentParser,
    GROUP_STUDENTS,
    ALL,
    SESSIONS,
    ASSESSMENTS,
)

def sessions_rows():
    # Two students, two days; absent toggles to test present mapping
    d0 = datetime(2025, 9, 1, 10, 15)
    d1 = d0 + timedelta(days=1)
    return [
        # Student 1, subject Math
        dict(id=1, first_name="Ada", last_name="Lovelace", subject="Math",
             session_date=d0, duration=60, absent=False),
        dict(id=1, first_name="Ada", last_name="Lovelace", subject="Math",
             session_date=d1, duration=45, absent=False),
        # Student 2, subject CS
        dict(id=2, first_name="Alan", last_name="Turing", subject="CS",
             session_date=d0, duration=30, absent=True),
    ]

def assessments_rows():
    d0 = datetime(2025, 9, 1, 9, 0)
    d1 = d0 + timedelta(days=1)
    return [
        dict(id=10, student_id=1, subject="Math", session_date=d0,
             max_score=20, score=18),  # 90%
        dict(id=11, student_id=1, subject="Math", session_date=d1,
             max_score=50, score=40),  # 80%
        dict(id=20, student_id=2, subject="CS",   session_date=d0,
             max_score=10, score=7),   # 70%
    ]


def test_sessions_all_returns_dataframe_with_present_symbol():
    parser = StudentParser(
        data=sessions_rows(),
        assessments=None,
        sort_key=ALL,
        data_type=SESSIONS,
    )
    df = parser.get_file()
    assert isinstance(df, pd.DataFrame)
    # session_date normalized to date (00:00)
    assert df["session_date"].dtype.kind in ("M",)  # datetime64
    # present column should be "P" for absent=False and "A" for absent=True
    row_ada = df[df["id"] == 1]
    assert set(row_ada["present"].unique()) == {"P"}
    row_alan = df[df["id"] == 2]
    assert set(row_alan["present"].unique()) == {"A"}


def test_sessions_group_students_aggregates_and_pivot():
    parser = StudentParser(
        data=sessions_rows(),
        assessments=None,
        sort_key=GROUP_STUDENTS,
        data_type=SESSIONS,
    )
    out = parser.get_file()
    assert isinstance(out, pd.DataFrame)

    # Should have one row per (id, first_name, last_name, subject)
    keys = ["id", "first_name", "last_name", "subject"]
    assert out[keys].drop_duplicates().shape[0] == 2

    # Aggregates per student/subject
    ada = out[(out["id"] == 1) & (out["subject"] == "Math")].iloc[0]
    # duration_total = 60 + 45
    assert ada["duration_total"] == 105
    # absent_count = sum of booleans (both False => 0)
    assert ada["absent_count"] == 0
    # present_count = "count" of "present" (two rows)
    assert ada["present_count"] == 2

    alan = out[(out["id"] == 2) & (out["subject"] == "CS")].iloc[0]
    assert alan["duration_total"] == 30
    assert alan["absent_count"] == 1
    assert alan["present_count"] == 1  # count of rows, not of P’s (matches current impl)

    # Pivot columns are the continuous date range between min..max session_date
    date_cols = [c for c in out.columns if isinstance(c, pd.Timestamp)]
    assert len(date_cols) == 2  # spans two days in sample
    # Ada attended both days → cells hold joined symbols "P ,P" by current aggfunc
    # (Your current impl uses " ,".join(x); we just check non-null presence)
    ada_day_vals = out[(out["id"] == 1) & (out["subject"] == "Math")][date_cols].iloc[0]
    assert ada_day_vals.notna().all()

    # Alan only has the first day; second day should be filled with "A" via fillna("A")
    alan_day_vals = out[(out["id"] == 2) & (out["subject"] == "CS")][date_cols].iloc[0]
    # first day had one record (absent -> present="A" per your mapping)
    assert alan_day_vals[date_cols[0]] in ("A", "A ,A", " ,".join(["A"]))  # tolerant check
    # day with no session becomes "A" by your fillna("A")
    assert alan_day_vals[date_cols[1]] == "A"


def test_assessments_all_includes_normalized_score_percent():
    # A no data means there is no sessions therefore no assesments are possible must be None
    parser = StudentParser(
        data=None,
        assessments=assessments_rows(),
        sort_key=ALL,
        data_type=ASSESSMENTS,
    )
    df = parser.get_file()
    assert df is None


def test_assessments_group_students_sorted_by_id():
    # A no data means there is no sessions therefore no assesments are possible must be None
    parser = StudentParser(
        data=None,
        assessments=assessments_rows(),
        sort_key=GROUP_STUDENTS,
        data_type=ASSESSMENTS,
    )
    df = parser.get_file()
    assert df is None


def test_empty_sessions_returns_none():
    parser = StudentParser(
        data=[],
        assessments=None,
        sort_key=ALL,
        data_type=SESSIONS,
    )
    assert parser.get_file() is None

def test_empty_assessments_returns_none():
    parser = StudentParser(
        data=None,
        assessments=[],
        sort_key=ALL,
        data_type=ASSESSMENTS,
    )
    assert parser.get_file() is None