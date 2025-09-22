import pandas as pd
import json
from datetime import datetime, timedelta


"""
    This class will help parse the data incoming from get_tutor_file_data()
    init(_): List[dict]
"""

SESSIONS = 'Sessions'
ASSESSMENTS = 'Assessments'
GROUP_TUTORS = 'group_tutors'
ALL = 'all'

class TutorParser:
    def __init__(self, data, sort_key):
        self.data = data
        self.sort_key = sort_key
        self.file = None
        if self.data:
            self.file = self.parse()
        
    def isDataEmpty(self)->bool:
        return not self.data
    
    def get_file(self) -> pd.DataFrame:    
        if self.isDataEmpty():
            return None
        return self.file

    def parse(self) ->pd.DataFrame:
        if self.isDataEmpty():
            return None
        file = pd.DataFrame(self.data)
        columns = ["tutor_id", "first_name", "last_name", "session_id", "student_count","session_date", "duration", "notes", "program_name" ,"start_time", "substitute"]
        file = file.reindex(columns=columns)
        file = file.rename(columns={"first_name": "First name",
                                    "last_name": "Last name",
                                    "session_id": "Session id",
                                    "tutor_id": "Tutor id",
                                    "student_count": "Student count",
                                    "session_date": "Session date",
                                    "duration": "Duration",
                                    "notes": "Notes",
                                    "program_name": "Program name",
                                    "substitute": "Substitute",
                                    "start_time": "Start time"})
    
        if self.sort_key == GROUP_TUTORS:
            # Normalize session date column
            file["Session date"] = pd.to_datetime(file["Session date"]).dt.normalize()
            #Mark present days
            file["present"] = "P"
            rows = ['First name', 'Last name','Tutor id']
            # Get the min max dates range using pandas
            all_dates = pd.date_range(file["Session date"].min(), file["Session date"].max(), freq="D")
            df = file.groupby(['First name', 'Last name', 'Tutor id', 'Program name']).agg(
                                                                  total_students=("Student count", "sum"), 
                                                                  substitute_flag=("Substitute", lambda x: "Yes" if x.any() else "No"),
                                                                  sessions=("Session id", "count"),
                                                                ).reset_index()    
            
            pivot_table = (
                file.pivot_table(
                    index=rows,
                    columns='Session date',
                    values='present',
                    aggfunc=lambda x : " ,".join(x),
                )
                .reindex(columns=all_dates)
                .fillna("N")
            )
            file = pivot_table.reset_index()
            final = pd.merge(
                df,
                pivot_table,
                on=["Tutor id", "First name", "Last name"],
                how="inner"
            )
        
            return final
        elif self.sort_key == ALL:
            return file
        else:
            return None

    
