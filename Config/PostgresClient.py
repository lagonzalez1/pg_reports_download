import os
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import OperationalError, ProgrammingError, Error
from dotenv import load_dotenv
import logging

# --- 1. Set up basic logging to stdout ---
logging.basicConfig(
    level=logging.INFO, # Adjust to logging.DEBUG for more verbose logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
load_dotenv()

class PostgresClient:
    def __init__(self):
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Internal method to handle the database connection and logging."""
        try:
            logger.info("Attempting to connect to PostgreSQL database.")
            self.conn = psycopg2.connect(
                host=os.getenv("POSTGRES_URL"),
                port=os.getenv("POSTGRES_PORT"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                dbname=os.getenv("POSTGRES_DB_NAME")
            )
            self.conn.autocommit = True
            logger.info("Successfully connected to PostgreSQL database.")
        except OperationalError as e:
            # This handles connection-related errors
            logger.error("Failed to connect to PostgreSQL database.")
            logger.exception(e)
            raise RuntimeError("Database connection failed") from e
        except Exception as e:
            logger.exception("An unexpected error occurred during database connection.")
            raise RuntimeError("Database connection failed") from e

    def _get_cursor(self, cursor_factory=None):
        """Internal helper to get a cursor and handle potential connection issues."""
        if not self.conn or self.conn.closed:
            logger.warning("Database connection is closed. Attempting to reconnect...")
            self._connect()
        return self.conn.cursor(cursor_factory=cursor_factory)

    def fetch_one(self, query, params=None):
        try:
            with self._get_cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                logger.debug(f"Executed query: {query} with params: {params}")
                return cursor.fetchone()
        except (OperationalError, ProgrammingError) as e:
            logger.error(f"Failed to execute query: {query}")
            logger.exception(e)
            raise RuntimeError("Database query failed") from e

    def fetch_all(self, query, params=None):
        try:
            with self._get_cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                logger.debug(f"Executed query: {query} with params: {params}")
                return cursor.fetchall()
        except (OperationalError, ProgrammingError) as e:
            logger.error(f"Failed to execute query: {query}")
            logger.exception(e)
            raise RuntimeError("Database query failed") from e

    def execute(self, query, params=None):
        try:
            with self._get_cursor() as cursor:
                cursor.execute(query, params)
                logger.info(f"Executed command: {query} with params: {params}")
        except (OperationalError, ProgrammingError) as e:
            logger.error(f"Failed to execute command: {query}")
            logger.exception(e)
            raise RuntimeError("Database command failed") from e
        
    

    def update_organization_report(self, params):
        query = "" \
        "UPDATE stu_tracker.Organization_report SET status = %s, retry_count = %s WHERE s3_output_key = %s;"
        self.execute(query, params)

    def get_tutor_file_data(self, params: dict):
        query = [
            "SELECT "
            "ss.id,",
            "ss.tutor_id,",
            "ss.session_date,",
            "ss.substitute,",
            "ss.student_count,",
            "ss.start_time,",
            "ss.duration,",
            "ss.notes,",
            "t.first_name,",
            "t.last_name,",
            "pg.program_name,",
            "pg.id",
            "FROM stu_tracker.Sessions ss",
            "LEFT JOIN stu_tracker.Tutors t ON t.id = ss.tutor_id",
            "LEFT JOIN stu_tracker.Programs pg ON pg.id = ss.program_id",
        ]
        args = []
        conditions = []
        if params.get("location_id"):
            conditions.append("ss.location_id = %s")
            args.append(int(params.get("location_id")))
        
        if params.get("program_id"):
            conditions.append("ss.program_id = %s")
            args.append(int(params.get("program_id")))
        
        if params.get("semester_id"):
            conditions.append("ss.semester_id = %s")
            args.append(int(params.get("semester_id")))
        
        if params.get("date") != "0001-01-01T00:00:00Z" and params.get("date_end") != "0001-01-01T00:00:00Z":
            conditions.append("DATE(ss.session_date) BETWEEN %s AND %s")
            args.append(params.get("date"))
            args.append(params.get("date_end"))
        
        elif params.get("date") != "0001-01-01T00:00:00Z":
            conditions.append("DATE(ss.session_date) >= %s")
            args.append(params.get("date"))

        if params.get("subject_id"):
            conditions.append("ss.subject_id = %s")
            args.append(params.get("subject_id"))
        
        qu = None
        if len(conditions) > 0:
            query.append("WHERE")
            query.append(" AND ".join(conditions))
            qu = " ".join(query)
        else:
            return None
            
        data = self.fetch_all(qu, args)
        if not data:
            return None
        return [dict(row) for row in data]

    def get_student_assessments(self, params: dict):
        query = [
            "SELECT",
            "a.title, ",
            "a.max_score,",
            "ast.score,"
            "sn.session_date,",
            "a.letter,",
            "a.cycle,",
            "a.pre,",
            "a.mid,",
            "a.post,",
            "a.version,",
            "ss.first_name,",
            "ss.last_name,",
            "ss.id,",
            "ast.session_id",
            "FROM stu_tracker.Assessments_students ast",
            "JOIN stu_tracker.Assessments a ON a.id = ast.assessment_id",
            "LEFT JOIN stu_tracker.Students ss ON ss.id = ast.student_id",
            "LEFT JOIN stu_tracker.Sessions sn ON sn.id = ast.session_id",
            ""
        ]
        args = []
        conditions = []
        if params.get("location_id"):
            conditions.append("sn.location_id = %s")
            args.append(int(params.get("location_id")))
        
        if params.get("program_id"):
            conditions.append("sn.program_id = %s")
            args.append(int(params.get("program_id")))
        
        if params.get("semester_id"):
            conditions.append("sn.semester_id = %s")
            args.append(int(params.get("semester_id")))
        
        if params.get("date") != "0001-01-01T00:00:00Z" and params.get("date_end") != "0001-01-01T00:00:00Z":
            conditions.append("DATE(sn.session_date) BETWEEN %s AND %s")
            args.append(params.get("date"))
            args.append(params.get("date_end"))
        
        elif params.get("date") != "0001-01-01T00:00:00Z":
            conditions.append("DATE(sn.session_date) >= %s")
            args.append(params.get("date"))

        if params.get("subject_id") and params.get("subject") != "all":
            conditions.append("ast.subject_id = %s")
            args.append(params.get("subject_id"))
        
        qu = None
        if len(conditions) > 0:
            query.append("WHERE")
            query.append(" AND ".join(conditions))
            qu = " ".join(query)
        else:
            qu = " ".join(query)
        
        print(qu)
        if qu is None:
            return None

        data = self.fetch_all(qu, args)
        if not data:
            return None
        return [dict(row) for row in data]

    def get_student_sessions(self, params: dict):
        query = [
            "SELECT ",
            "s.id,",  
            "s.first_name,",
            "s.last_name,",
            "s.grade_level,",        
            "ss.id AS session_id,",
            "ss.absent,",
            "ss.duration,",
            "st.session_date,",
            "s.timeframe,",
            "s.timeframe_start,",
            "s.timeframe_end,",
            "CASE ",
            "       WHEN ss.subject_id IS NULL THEN 'NA'",
            "       ELSE sj.title",
            "END AS subject",
            "FROM stu_tracker.Students s",
            "LEFT JOIN stu_tracker.Session_students ss ON s.id = ss.student_id",
            "JOIN stu_tracker.Sessions st ON st.id = ss.session_id",
            "JOIN stu_tracker.Subjects sj ON sj.id = ss.subject_id",
        ]
        args = []
        conditions = []
        if params.get("location_id"):
            conditions.append("st.location_id = %s")
            args.append(int(params.get("location_id")))
        
        if params.get("program_id"):
            conditions.append("st.program_id = %s")
            args.append(int(params.get("program_id")))
        
        if params.get("semester_id"):
            conditions.append("st.semester_id = %s")
            args.append(int(params.get("semester_id")))
        
        if params.get("date") != "0001-01-01T00:00:00Z" and params.get("date_end") != "0001-01-01T00:00:00Z":
            conditions.append("DATE(st.session_date) BETWEEN %s AND %s")
            args.append(params.get("date"))
            args.append(params.get("date_end"))
        
        elif params.get("date") != "0001-01-01T00:00:00Z":
            conditions.append("DATE(st.session_date) >= %s")
            args.append(params.get("date"))

        if params.get("subject_id") and params.get("subject") != "all":
            conditions.append("ss.subject_id = %s")
            args.append(params.get("subject_id"))
        
        qu = None
        if len(conditions) > 0:
            query.append("WHERE")
            query.append(" AND ".join(conditions))
            qu = " ".join(query)
        else:
            qu = " ".join(query)
        
        print(qu)
        if qu is None:
            return None

        data = self.fetch_all(qu, args)
        if not data:
            return None
        return [dict(row) for row in data]
    
                
    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("PostgreSQL connection closed.")