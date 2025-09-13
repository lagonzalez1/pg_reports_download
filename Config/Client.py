import json
import logging
import datetime
from typing import Optional


# --- Python logger ---
logging.basicConfig(
    level=logging.INFO, # Adjust to logging.DEBUG for more verbose logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class Client:
    def __init__(self, body):
        try:
            self.body = json.loads(body.decode('utf-8'))
        except json.JSONDecodeError as e:
            logger.info("Unable to parse file")
            self.body = {}
        
        # Used to differentiate between a tutor or student
        self._entity: Optional[str] = self.body.get("entity")
        self._location_id: Optional[int] = self.body.get("location_id")
        self._program_id: Optional[int] = self.body.get("program_id")
        self._subject_id: Optional[int] = self.body.get("subject_id")
        self._semester_id: Optional[int] = self.body.get("semester_id")
        self._date_start: Optional[datetime] = self.body.get("date")
        self._date_end: Optional[datetime] = self.body.get("date_end")
        self._sort_key: Optional[str] = self.body.get("sort_key")
        self._s3_output_key: Optional[str] = self.body.get("s3_output_key")
        self._data_type: Optional[str] = self.body.get("data_type")
        
    
    
    def get_body(self) ->Optional[dict]:
        return self.body

    def get_data_type(self) -> Optional[str]:
        return self._data_type

    def get_s3_output_key(self) -> Optional[str]:
        return self._s3_output_key

    def get_entity(self) -> Optional[str]:
        return self._entity

    def get_location_id(self) -> Optional[int]:
        return self._location_id

    def get_program_id(self) -> Optional[int]:
        return self._program_id

    def get_subject_id(self) -> Optional[int]:
        return self._subject_id

    def get_semester_id(self) -> Optional[int]:
        return self._semester_id

    def get_date_start(self) -> Optional[datetime]:
        return self._date_start

    def get_date_end(self) -> Optional[datetime]:
        return self._date_end

    def get_sort_key(self) -> Optional[str]:
        return self._sort_key