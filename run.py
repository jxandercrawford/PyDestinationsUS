"""
File: run.py
Author: jxandercrawford@gmail.com
Date: 2023-06-22
Purpose: Run the BTS data pipeline
"""
import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from src.pipeline import Pipeline

def validate_month(month: int) -> bool:
    """
    Validate month input.
    :param month (int): Month to validate.
    :returns: Bool if valid.
    """
    return month > 0 and month < 13

def validate_year(year: int) -> bool:
    """
    Validate year input.
    :param year (int): Year to validate.
    :returns: Bool if valid.
    """
    return year > 1886

def validate_number(number: int) -> bool:
    """
    Validate number input.
    :param number (int): Number to validate.
    :returns: Bool if valid.
    """
    return number > 0

def validate_date(date: datetime) -> bool:
    """
    Validate date input.
    :param date (int): Date to validate.
    :returns: Bool if valid.
    """
    return date < datetime.now()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Run the Bereau of Transporation Statistics flight data pipeline."
    )
    parser.add_argument(
        "-y", "--year", 
        help="The year to target as an integer.",
        required=True, type=int
    )
    parser.add_argument(
        "-m", "--month",
        help="The year to target as an integer.",
        required=True, type=int
    )
    parser.add_argument(
        "-n", "--number",
        help="Optional, the number of months to ingest as an integer starting from the year and month and increasing by 1 month. Must be greater than 0.",
        type=int, default=1
    )
    parser.add_argument(
        "-s", "--serverhost",
        help="Optional, the database host server.",
        type=str, default="localhost"
    )
    parser.add_argument(
        "-d", "--database",
        help="Optional, the database name.",
        type=str, default="flights"
    )
    parser.add_argument(
        "-u", "--user",
        help="Optional, the database username.",
        type=str, default="jxan"
    )
    parser.add_argument(
        "-p", "--password",
        help="Optional, the database user password.",
        type=str, default=""
    )

    args = parser.parse_args()
    if not (validate_month(args.month) and validate_year(args.year)):
        print("ERROR: The given year (%d) and/or month (%d) is not valid." % (args.year, args.month))
        exit(1)
    elif not validate_number(args.number):
        print("ERROR: The given number of months (%d) is not valid." % args.number)
        exit(1)

    bts_pipeline = Pipeline({"host": args.serverhost, "database": args.database, "user": args.user, "password": args.password})
    target_date = datetime(args.year, args.month, 1)
    n_months = args.number
    n_inserted = 0

    while n_months > 0:
        if not validate_date(target_date):
            print("ERROR: The month of %s is not valid." % target_date.strftime("%Y-%m"))
            exit(1)

        n_inserted += bts_pipeline.run(target_date.year, target_date.month)
        n_months -= 1
        target_date += relativedelta(months=1)

    print("%d records inserted." % n_inserted)
