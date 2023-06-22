"""
File: extract.py
Author: jxandercrawford@gmail.com
Date: 2023-06-22
Purpose: A component of a pipeline for Bereau of Transporation Statistics flight data.
"""
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

import csv
from datetime import datetime
import os

import ssl

SSL_CONTEXT = ssl._create_unverified_context()

class ExtractBTS:
    """
    Extraction component for the  Bereau of Transporation Statistics flight data pipeline.
    :param download_path (str): Path to temporaryly download files to.
    """

    def __init__(self, download_path: str):
        self.__download_path = download_path
        
        self.__base_url = "https://transtats.bts.gov/PREZIP/"
        self.__url_file_pattern = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_%d_%d.zip"
        self.__file_name_pattern = "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_%d_%d.csv"

    def download(self, year: int, month: int) -> str:
        """
        Download and unzip Bereau of Transporation Statistics flight data into a csv file.
        :param year (int): Year to download.
        :param month (int): Month to download.
        :returns: Downloaded file path.
        """
        url = self.__base_url + self.__url_file_pattern % (year, month)
        file_name = self.__file_name_pattern % (year, month)

        with urlopen(url, context=SSL_CONTEXT) as resp:
            with ZipFile(BytesIO(resp.read())) as zip_file:
                zip_file.extract(file_name, self.__download_path)

        return self.__download_path + file_name

    def __get_origin_airport(self, row) -> tuple:
        """
        Extract an origin airport tuple from a row of the data file.
        :returns: A tuple (int, str, str, str) of (id, name, city, state).
        """
        airport_id = row[11]
        name = row[14]
        city = row[15].split(",")[0]
        state = row[16]
        return (int(airport_id), name, city, state)

    def __get_destination_airport(self, row) -> tuple:
        """
        Extract a destination airport tuple from a row of the data file.
        :returns: A tuple (int, str, str, str) of (id, name, city, state).
        """
        airport_id = row[20]
        name = row[23]
        city = row[24].split(",")[0]
        state = row[25]
        return (int(airport_id), name, city, state)

    def __get_flight(self, row) -> tuple:
        """
        Extract a flight tuple from a row of the data file.
        :returns: A tuple (datetime, int, int) of (date, origin id, destination id).
        """
        date = row[5]
        origin_id = row[11]
        destination_id = row[20]
        return (datetime.strptime(date, "%Y-%m-%d"), int(origin_id), int(destination_id))

    def parse_row(self, row) -> None:
        """
        Parse a row into a dictionary containing an origin airport, destination ariport, and a flight.
        :param row (itetable): A row to parse into flight data objects.
        :returns: A dictionary of the format: {
            "origin": *airport*,
            "destination": *airport*,
            "flight": *flight*
        }
        """
        try:
            origin = self.__get_origin_airport(row)
            destination = self.__get_destination_airport(row)
            flight = self.__get_flight(row)
            return {
                "origin": origin,
                "destination": destination,
                "flight": flight
            }
        except:
            return None

    def parse(self, file_path: str) -> dict:
        """
        Parse a csv file from the Bereau of Transporation Statistics into a records of origin airports, destination ariports, and flights.
        :param file_path (str): Path of the file to parse.
        :returns: Dictionary of flight records in the format {
            "origin": [*airports*],
            "destination": [*airport*],
            "flight": [*flight*]
        }
        """
        records = {"origin": [], "destination": [], "flight": []}

        with open(file_path, "r", encoding="utf-8") as date_file:
            doc = csv.reader(date_file, delimiter=",", quotechar="\"")
            header = True
            for row in doc:
                if not header:
                    record = self.parse_row(row)
                    records["origin"].append(record["origin"])
                    records["destination"].append(record["destination"])
                    records["flight"].append(record["flight"])
                else:
                    header = False
        return records

    def extract_flights(self, year: int, month: int):
        """
        Extract flights from the Bereau of Transporation Statistics of a time span into records.
        :param year (int): The year to extract from.
        :param month (int): The month to extract from.
        :returns: A dictionary of lists in the format {
            "origin": [*airports*],
            "destination": [*airport*],
            "flight": [*flight*]
        }
        """
        path = self.download(year, month)
        abspath = os.path.abspath(path)

        records = self.parse(abspath)
        os.remove(path)
        return records
