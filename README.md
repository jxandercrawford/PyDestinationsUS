# PyDestinationsUS

A simple ETL project inspired from the original [DestinationsUS](). This is an implmentation is Python.

### Usage
```
usage: run.py [-h] -y YEAR -m MONTH [-n NUMBER]

Run the Bereau of Transporation Statistics flight data pipeline.

options:
  -h, --help            show this help message and exit
  -y YEAR, --year YEAR  The year to target as an integer.
  -m MONTH, --month MONTH
                        The year to target as an integer.
  -n NUMBER, --number NUMBER
                        Optional, the number of months to ingest as an integer starting from the year and month and increasing by 1 month.
                        Must be greater than 0.
```