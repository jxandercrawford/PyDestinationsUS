
from src.extract import ExtractBTS
from src.database import Database
import pandas as pd

class Pipeline:

    def __init__(self, database_config: dict):
        self.__extracter = ExtractBTS("data/")
        self.__db = Database(**database_config)

        self.__records = None

    def can_write(self):
        if self.__records:
            return True
        return False

    def extract_records(self, year: int, month: int) -> None:
        self.__records = self.__extracter.extract_flights(year, month)

    def write_records(self) -> None:
        if not self.__records:
            raise RuntimeError("The pipeline is not in the correct state. Please run `extract_records()` before writing.")

        self.__db.executeBatch(
            "INSERT INTO airport(id, name, city, state) VALUES(%s, %s, %s, %s) ON CONFLICT DO NOTHING;", 
            set(self.__records["destination"]).union(set(self.__records["origin"]))
        )
        self.__db.executeBatch(
            "INSERT INTO flight(date, origin, destination) VALUES(%s, %s, %s) ON CONFLICT DO NOTHING;", 
            self.__records["flight"]
        )

        self.__records = None

    def run(self, year: int, month: int) -> int:
        self.extract_records(year, month)
        n_inserted = len(self.__records["flight"])
        self.write_records()
        return n_inserted
