import argparse
from src.pipeline import Pipeline

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run the Bereau of Transporation Statistics flight data pipeline")
    parser.add_argument("-y", "--year", help="The year to target as an integer.", required=True, type=int)
    parser.add_argument("-m", "--month", help="The year to target as an integer.", required=True, type=int)

    args = parser.parse_args()

    bts_pipeline = Pipeline({"host": "localhost", "database": "flights", "user": "jxan", "password": ""})
    n_inserted = bts_pipeline.run(args.year, args.month)
    print("%d records inserted." % n_inserted)
