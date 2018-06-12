from sqlalchemy import Table, Column, String, sql
 #
 # 
 # Copyright (C) 2018 Raymond Wai Yan Ko <wisely38@hotmail.com>
 #
 # 
 # This file is part of eternity-newsextractor.
 # 
 # eternity-newsextractor cannot be copied and/or distributed for commercial use 
 # without the express permission of Raymond Wai Yan Ko <wisely38@hotmail.com>
 #
 #

from sqlalchemy import Table, Column, Integer, String, ForeignKey
import sqlalchemy as sa
from dao.PostgresDriver import PostgresDriver
import psycopg2
import pandas as pd
import io


class PostgresDao:

    def __init__(self):
        self.db_driver = PostgresDriver()
        self.engine, self.metadata, self.connection = self.db_driver.connect_with_retries(
            3)

    def has_records(self, table_name, primary_key, values):
        try:
            df = self.read_table(table_name)
            if not isinstance(values, list):
                values = list(values)
            result_df = df.loc[df['id'].isin(values)]
            if len(result_df.values.tolist()) == 0:
                return False
        except Exception as e:
            print(e)
            return False
        else:
            return True

    def has_record(self, table_name, primary_key, value):
        try:
            cmd = 'select * from {0} where {1} =:col'.format(
                table_name, primary_key)
            cursor = self.connection.execute(
                sql.text(cmd), col=value
            )
            return bool(cursor.first())
        except Exception as e:
            print(e)
            return False
        else:
            return True

    def has_table(self, table_name):
        try:
            return self.engine.dialect.has_table(self.connection, table_name)
        except Exception as e:
            print(e)
            return False
        else:
            return True

    def create_table(self, table_parameters):
        if not isinstance(table_parameters, list):
            raise Exception(
                'Need to pass list of parameters including table name and column names!')
        else:
            try:
                columns = table_parameters[2:]
                self.table = Table(table_parameters[0], self.metadata, Column(table_parameters[1], String, primary_key=True),
                                   *(Column(header, String) for header in columns))

                self.metadata.create_all(self.engine)
                self.connection.execute("commit")
            except Exception as e:
                print('Not able to create table {0}'.format(e))

    def delete_table(self, table_name):
        if not table_name:
            raise Exception('Need to pass table name!')
        self.connection.execute("commit")
        self.connection.execute('DROP TABLE IF EXISTS {0}'.format(table_name))
        self.connection.execute("commit")
        self.connection.execute('VACUUM', self.engine)
        self.connection.execute("commit")
        self.db_driver.reset_metadata()
        self.connection.close()
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
