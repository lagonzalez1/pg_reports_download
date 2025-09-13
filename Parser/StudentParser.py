import pandas as pd
import json


"""
    This class will help parse the data incoming from get_tutor_file_data()
    init(_): List[dict]
"""

SESSIONS = 'Sessions'
ASSESSMENTS = 'Assessments'
GROUP_STUDENTS = 'group_students'
ALL = 'all'


class StudentParser:
    def __init__(self, data, assessments, sort_key, data_type):
        self.data = data
        self.data_type = data_type
        self.assessments = assessments
        self.sort_key = sort_key
        self.file = None
        if self.data and self.data_type == SESSIONS:
            self.file = self.parse()
        elif self.data and self.data_type == ASSESSMENTS:
            self.file = self.parse_assessments()
        else:
            self.file = None

    def isAssessmentDataEmpty(self) ->bool:
        return not self.assessments

    def isDataEmpty(self)->bool:
        return not self.data
    
    def get_file(self) -> pd.DataFrame:    
        if self.isDataEmpty():
            return None
        return self.file
    
    def parse_assessments(self)->pd.DataFrame:
        if self.isAssessmentDataEmpty():
            return None
        file = pd.DataFrame(self.assessments)
        file['session_date'] = pd.to_datetime(file['session_date']).dt.normalize()
        file['normalized_score'] = file[['max_score', 'score']].apply(lambda row: (row['score']/row['max_score']) * 100 , axis=1)
        if self.sort_key == GROUP_STUDENTS:
            grouped = file.sort_values(by='id')
            return grouped
        elif self.sort_key == ALL:
            return file
        return None
    
    def parse(self) ->pd.DataFrame:
        if self.isDataEmpty():
            return None
        file = pd.DataFrame(self.data)
        file["session_date"] = pd.to_datetime(file["session_date"]).dt.normalize()
        file["present"] = file["absent"].apply(lambda x: "P" if not x else "A")

        if self.sort_key == GROUP_STUDENTS:
            pivote_rows = ['id', 'first_name', 'last_name', 'subject']
            date_range = pd.date_range(file['session_date'].min(), file["session_date"].max(), freq="D")

            file_df = file.groupby(['id', 'first_name', 'last_name', 'subject']).agg(
                duration_total=("duration", "sum"),
                absent_count=("absent", "sum"),
                present_count=("present", "count")
            ).reset_index()
            
            pivot = (
                file.pivot_table(
                    index=pivote_rows,
                    columns="session_date",
                    values="present",
                    aggfunc=lambda x: " ,".join(x)
                )
            ).reindex(columns=date_range).fillna("A")

            file_pivot_combined = pd.merge(
                file_df,
                pivot,
                on=['id', 'first_name', 'last_name', 'subject'],
                how="inner"
            )    
            return file_pivot_combined
        elif self.sort_key == ALL:
            return file
        else:
            return None

    
