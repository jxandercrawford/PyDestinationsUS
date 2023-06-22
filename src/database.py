"""
database.py
Date: 3/2022
Updated: 8/11/2022
Author: xc383@drexel.edu
Desc: A Postgresql database interface
REF: https://github.com/NCATSTranslator/Explanatory-Agent/blob/master/xARA-ETL/src/clsDatabase.py
"""

# Update Notes
# 8/11/2022: Edited uploadTableViaDataFrame() to support returning clause

import psycopg2
import psycopg2.extras
import pandas as pd
from tqdm import tqdm
import logging
import os


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

    def execute(self, sql, expectingReturn=False):
        """
        Execute a sql query, and optional return results as a pandas DataFrame
        :param sql: Any sql statement
        :param expectingReturn: True meaning return a pandas DataFrame, False meaning no return
        :return: pandas DataFrame or None
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(sql)
        if expectingReturn:
            cols = []
            for desc in cursor.description:
                cols.append(desc[0])
            return pd.DataFrame(cursor.fetchall(), columns=cols)
        else:
            self.connection.commit()
        cursor.close()

    def executeBatch(self, sql, values):
        """
        Execute a sql query in batch, useful for insert and update
        :param sql: An update sql statement
        :param values: a list of values, should be in chronological order
        :return: None
        """

        self.connect()
        cursor = self.connection.cursor()
        psycopg2.extras.execute_batch(cursor, 
                sql,
                values
            )
        self.connection.commit()
        cursor.close()


    def uploadTableViaDataFrame(self, df, tableName, clearTable=False, conflictStatement="", returnColumns=None, shouldCrashOnBadRow=True):
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
        if clearTable:
            clearTable_sql = "TRUNCATE " + tableName + ";"
            cursor.execute(clearTable_sql)

        returnStatement = ""
        return_values = returnColumns != None
        if return_values:
            returnStatement = "RETURNING " + ", ".join(returnColumns)
        
        try:
            values = psycopg2.extras.execute_values(
                cursor, 
                "insert into " + tableName + "(" + ', '.join(df.columns) + ")\nvalues %s " + conflictStatement + " " + returnStatement + ";",
                df.values.tolist(),
                page_size=len(df),
                fetch=return_values
            )
            self.connection.commit()
        except:
            if shouldCrashOnBadRow:
                raise

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

    print("Connecting to database")
    db = Database()
    db.connect()

    print("Querying database")
    df = db.execute(sql="select current_timestamp", expectingReturn=True)
    print(df)

    print("Disconnecting from database")
    db.disconnect()


