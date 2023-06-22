import argparse
from src.pipeline import Pipeline
from datetime import datetime
from dateutil.relativedelta import relativedelta

def validate_month(month: int) -> bool:
    return month > 0 and month < 13

def validate_year(year: int) -> bool:
    return year > 1886

def validate_number(number: int) -> bool:
    return number > 0

def validate_date(date: datetime) -> bool:
    return date < datetime.now()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run the Bereau of Transporation Statistics flight data pipeline.")
    parser.add_argument("-y", "--year", help="The year to target as an integer.", required=True, type=int)
    parser.add_argument("-m", "--month", help="The year to target as an integer.", required=True, type=int)
    parser.add_argument("-n", "--number", help="Optional, the number of months to ingest as an integer starting from the year and month and increasing by 1 month. Must be greater than 0.", type=int, default=1)

    args = parser.parse_args()
    if not (validate_month(args.month) and validate_year(args.year)):
        print("ERROR: The given year (%d) and/or month (%d) is not valid." % (args.year, args.month))
        exit(1)
    elif not validate_number(args.number):
        print("ERROR: The given number of months (%d) is not valid." % args.number)
        exit(1)

    bts_pipeline = Pipeline({"host": "localhost", "database": "flights", "user": "jxan", "password": ""})
    date = datetime(args.year, args.month, 1)
    n_months = args.number
    n_inserted = 0

    while n_months > 0:
        if not validate_date(date):
            print("ERROR: The month of %s is not valid." % date.strftime("%Y-%m"))
            exit(1)

        n_inserted += bts_pipeline.run(date.year, date.month)
        n_months -= 1
        date += relativedelta(months=1)

    print("%d records inserted." % n_inserted)
