"""
database.py
Date: 3/2022
Updated: 6/22/2023
Author: xc383@drexel.edu
Desc: A Postgresql database interface
REF: https://github.com/NCATSTranslator/Explanatory-Agent/blob/master/xARA-ETL/src/clsDatabase.py
"""

# Update Notes
# 8/11/2022: Edited uploadTableViaDataFrame() to support returning clause

import psycopg2
import psycopg2.extras
import pandas as pd

class Database:
    """
    See header
    """

    def __init__(self, host: str, database: str, user: str, password: str):
        """
        Constructor
        """
        self.__host = host
        self.__database = database
        self.__user = user
        self.__password = password

        self.connection = None

    def connect(self):
        """
        Connect to Postgresql
        :return: None
        """
        self.connection = psycopg2.connect(
            host=self.__host, 
            database=self.__database, 
            user=self.__user, 
            password=self.__password
        )

    def disconnect(self):
        """
        Disconnect from Postgresql
        :return: None
        """
        if self.connection.close == 0:
            self.connection.close()

    def reconnect(self):
        """
        Reconnect to Postgresql
        :return: None
        """
        self.disconnect()
        self.connect()

    def execute(self, sql, expecting_return=False):
        """
        Execute a sql query, and optional return results as a pandas DataFrame
        :param sql: Any sql statement
        :param expectingReturn: True meaning return a pandas DataFrame, False meaning no return
        :return: pandas DataFrame or None
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(sql)
        if expecting_return:
            cols = []
            for desc in cursor.description:
                cols.append(desc[0])
            return pd.DataFrame(cursor.fetchall(), columns=cols)
        else:
            self.connection.commit()
        cursor.close()

    def execute_batch(self, sql, values):
        """
        Execute a sql query in batch, useful for insert and update
        :param sql: An update sql statement
        :param values: a list of values, should be in chronological order
        :return: None
        """

        self.connect()
        cursor = self.connection.cursor()
        psycopg2.extras.execute_batch(
            cursor,
            sql,
            values
        )
        self.connection.commit()
        cursor.close()


    def upload_table_via_dataframe(self, df: pd.DataFrame, table_name: str, clear_table: bool=False, conflict_statement: str="", return_columns: list=None, should_crash_on_bad_row: bool=True):
        """
        Uploads a pandas DataFrame to a given Postgresql table via insert statements
        :param df: A pandas DataFrame with column names which match the target table column names
        :param tableName: A Postgresql table name
        :param clearTable: Boolean whether to clear the table before uploading
        :param conflictStatement: A string that reflects Postgresql's ON CONFLICT logic
        :param returnColumns: An iterable of columns to return on write
        :return: 0 if no returnColumns else pandas dataframe
        """

        # ???remove shouldCrashOnBadRow???

        self.connect()
        cursor = self.connection.cursor()
        if clear_table:
            cursor.execute("TRUNCATE " + table_name + ";")

        return_statement = ""
        return_values = return_columns is not None
        if return_values:
            return_statement = "RETURNING " + ", ".join(return_columns)
        
        try:
            values = psycopg2.extras.execute_values(
                cursor, 
                "insert into " + table_name + "(" + ', '.join(df.columns) + ")\nvalues %s " + conflict_statement + " " + return_statement + ";",
                df.values.tolist(),
                page_size=len(df),
                fetch=return_values
            )
            self.connection.commit()
        except e as Exception:
            if should_crash_on_bad_row:
                raise e

        if return_values:
            cols = []
            for desc in cursor.description:
                cols.append(desc[0])

            df = pd.DataFrame(values, columns=cols)

        self.disconnect()

        if return_values:
            return df
        return 0

if __name__ == '__main__':

    credentials = {"host": "localhost", "database": "flights", "user": "jxan", "password": ""}

    print("Connecting to database")
    db = Database(**credentials)
    db.connect()

    print("Querying database")
    df = db.execute(sql="select current_timestamp", expecting_return=True)
    print(df)

    print("Disconnecting from database")
    db.disconnect()
