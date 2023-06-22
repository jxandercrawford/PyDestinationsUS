
from src.extract import ExtractBTS
from src.database import Database
import pandas as pd

class Pipeline:

    def __init__(self, database_config: dict):
        self.__extracter = ExtractBTS("data/")
        self.__db = Database(**database_config)

        self.__records = None

    def can_write(self):
        """
        Returns if the pipeline state allows for writing.
        :returns: Boolean if can write to database.
        """
        if self.__records:
            return True
        return False

    def extract_records(self, year: int, month: int) -> None:
        """
        Extract flight records from the Bereau of Transporation Statistics.
        :param year (int): The target year to extract.
        :param month (int): The target month to extract.
        :returns: None.
        """
        self.__records = self.__extracter.extract_flights(year, month)

    def write_records(self) -> None:
        """
        Write flight records to the database.
        :returns: None.
        """
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
        """
        Run the full pipeline. This consists of:
        1. Extracting date for year and month.
        2. Writing data to database.

        :param year (int): The target year to extract.
        :param month (int): The target month to extract.
        :returns: The integer of number of records extracted and written.
        """
        self.extract_records(year, month)
        n_inserted = len(self.__records["flight"])
        self.write_records()
        return n_inserted
