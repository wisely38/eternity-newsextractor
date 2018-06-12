from sqlalchemy import Table, Column, Integer, String, ForeignKey
import sqlalchemy as sa
from dao.PostgresDriver import PostgresDriver
import pandas as pd
import time
import io


class PostgresDao:

    def __init__(self):
        self.db_driver = PostgresDriver()
        self.engine, self.metadata, self.connection = self.db_driver.connect_with_retries(
            3)

    def create_table(self, table_parameters):
        if not isinstance(table_parameters, list):
            raise Exception(
                'Need to pass list of parameters including table name and column names!')
        else:
            columns = table_parameters[2:]
            self.table = Table(table_parameters[0], self.metadata, Column(table_parameters[1], String, primary_key=True),
                               *(Column(header, String) for header in columns))

            self.metadata.create_all(self.engine)
            self.connection.execute("commit")
 
    def delete_table(self, table_name):
        if not table_name:
            raise Exception('Need to pass table name!')
        self.connection.execute("commit")
        self.connection.execute('DROP TABLE IF EXISTS {0}'.format(table_name))
        self.connection.execute("commit")
        self.connection.execute('VACUUM', self.engine)
        self.connection.execute("commit")
        self.metadata.drop_all(self.engine)
        self.engine, self.metadata, self.connection = self.db_driver.connect_with_retries(
            3)

    def read_table(self, table_name):
        return pd.read_sql('select * from {0}'.format(table_name), self.connection)

    def write_table(self, dataframe, table_parameters):
        connection = self.engine.raw_connection()
        cur = connection.cursor()
        output = io.StringIO()
        dataframe.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        columns = table_parameters[1:]
        cur.copy_from(output, table_parameters[0], null="",
                      columns=tuple(columns))
        connection.commit()
