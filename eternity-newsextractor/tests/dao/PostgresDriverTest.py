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

import os
import sys
current_path = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))
sys.path.append(current_path)
print('ParentCurrent PATH: {0}'.format(current_path))
import unittest
from dao.PostgresDriver import PostgresDriver


class PostgresDriverTest(unittest.TestCase):

    def setUp(self):
        self.db_driver = PostgresDriver()

    def _steps(self):
        for name in sorted(dir(self)):
            if name.startswith('step'):
                yield name, getattr(self, name)

    def step_01_connect(self):
        try:
            engine, metadata = self.db_driver.connect(
                'postgres', '123Postgres!', 'newspaper', 'localhost', 5432)
            connection = engine.connect()
        except Exception as db_conn_error:
            print(db_conn_error)
            try:
                self.db_driver.create_db()
            except Exception as db_create_error:
                print(db_conn_error)
        finally:
            if connection:
                connection.close()

    def step_02_connect_with_retries(self):
        try:
            engine, metadata, connection = self.db_driver.connect_with_retries(3,
                                                                               'postgres', '123Postgres!', 'newspaper', 'localhost', 5432)
        except Exception as db_conn_error:
            print(db_conn_error)
            try:
                self.db_driver.create_db()
            except Exception as db_create_error:
                print(db_conn_error)
        finally:
            if connection:
                connection.close()

    def step_03_connect_with_no_args(self):
        try:
            engine, metadata, connection = self.db_driver.connect_with_retries(
                3)
        except Exception as db_conn_error:
            print(db_conn_error)
            try:
                self.db_driver.create_db()
            except Exception as db_create_error:
                print(db_conn_error)
        finally:
            if connection:
                connection.close()

    # def tearDown(self):
    #     self.item_dao.delete_item_collection()

    def test_steps(self):
        for name, step in self._steps():
            try:
                print('Starting {0}...'.format(name))
                step()
                print('Finished running {0}...'.format(name))
            except Exception as e:
                self.fail("{} failed ({}: {})".format(step, type(e), e))


if __name__ == '__main__':
    unittest.main()
