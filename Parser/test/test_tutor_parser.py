# tests/test_tutor_parser.py
import pandas as pd
import pytest
from datetime import datetime, timedelta

from Parser.TutorParser import TutorParser
SESSIONS = 'Sessions'
ASSESSMENTS = 'Assessments'
GROUP_TUTORS = 'group_tutors'
ALL = 'all'

def _sample_rows():
    base = datetime(2025, 9, 1, 10, 30)
    return [
        {
            "first_name": "Ada", "last_name": "Lovelace",
            "id": 100, "tutor_id": 1, "student_count": 3,
            "session_date": base, "duration": 60, "notes": "ok",
            "program_name": "Math Boost", "start_time": "10:30", "substitute": False
        },
        {
            "first_name": "Ada", "last_name": "Lovelace",
            "id": 101, "tutor_id": 1, "student_count": 2,
            "session_date": base + timedelta(days=1), "duration": 45, "notes": "",
            "program_name": "Math Boost", "start_time": "10:30", "substitute": True
        },
        {
            "first_name": "Alan", "last_name": "Turing",
            "id": 200, "tutor_id": 2, "student_count": 5,
            "session_date": base, "duration": 30, "notes": "n/a",
            "program_name": "CS Lab", "start_time": "11:00", "substitute": False
        },
    ]

def test_all_returns_reindexed_and_renamed_columns():
    parser = TutorParser(_sample_rows(), sort_key=ALL)
    df = parser.get_file()
    assert isinstance(df, pd.DataFrame)
    # Expected final header names (after reindex + rename)
    expected_cols = {
        "First name","Last name","Session id","Tutor id","Student count",
        "Session date","Duration","Notes","Program name","Start time","Substitute"
    }
    assert set(df.columns) == expected_cols
    # Row count matches input (no grouping in ALL)
    assert len(df) == 3

def test_group_tutors_basic_aggregation_and_pivot():
    parser = TutorParser(_sample_rows(), sort_key=GROUP_TUTORS)
    df = parser.get_file()
    assert isinstance(df, pd.DataFrame)
    # Should aggregate to one row per (First name, Last name, Tutor id, Program name)
    # Our sample has two tutors and two programs (Ada/Math Boost, Alan/CS Lab) => 2 rows
    group_cols = ["First name","Last name","Tutor id","Program name"]
    assert df[group_cols].drop_duplicates().shape[0] == 2

    # Check aggregates
    ada_row = df[(df["First name"]=="Ada") & (df["Last name"]=="Lovelace")].iloc[0]
    assert ada_row["total_students"] == 3 + 2
    assert ada_row["sessions"] == 2
    # substitute_flag should be "Yes" because one of Ada's sessions has substitute=True
    assert ada_row["substitute_flag"] == "Yes"

    alan_row = df[(df["First name"]=="Alan") & (df["Last name"]=="Turing")].iloc[0]
    assert alan_row["total_students"] == 5
    assert alan_row["sessions"] == 1
    assert alan_row["substitute_flag"] == "No"

    # Pivot columns are dates from min..max (normalized) and filled with "N" where missing
    # Find the dynamic date headers (DatetimeIndex converted to columns by pivot)
    date_cols = [c for c in df.columns if isinstance(c, pd.Timestamp)]
    assert len(date_cols) >= 2  # we span at least two days in the sample

    # Ada should have "P" in both days (present symbol), Alan only on day 1
    min_day = min(date_cols)
    max_day = max(date_cols)
    # Ada
    ada_vals = df[(df["First name"]=="Ada") & (df["Last name"]=="Lovelace")][date_cols].iloc[0]
    # Both days should be P (since you set present="P" for each session)
    assert set(ada_vals.values) == {"P"}
    # Alan
    alan_vals = df[(df["First name"]=="Alan") & (df["Last name"]=="Turing")][date_cols].iloc[0]
    assert alan_vals[min_day] == "P"
    # If there are more days than Alan attended, they should be "N"
    if len(date_cols) > 1:
        others = [d for d in date_cols if d != min_day]
        assert all(alan_vals[d] == "N" for d in others)

def test_empty_data_returns_none():
    parser = TutorParser([], sort_key=ALL)
    assert parser.get_file() is None

def test_handles_missing_optional_fields():
    # Missing some optional fields; ensure reindex/rename doesn’t crash
    base = datetime(2025, 9, 1)
    rows = [{
        "first_name":"Ada","last_name":"Lovelace","id":100,"tutor_id":1,
        "student_count":3,"session_date":base,"duration":60,"program_name":"Math Boost",
        # "notes" missing, "start_time" missing, "substitute" missing -> should become NaN/False
    }]
    parser = TutorParser(rows, sort_key=ALL)
    df = parser.get_file()
    assert len(df) == 1
    assert "Notes" in df.columns
    assert "Start time" in df.columns
    assert "Substitute" in df.columns