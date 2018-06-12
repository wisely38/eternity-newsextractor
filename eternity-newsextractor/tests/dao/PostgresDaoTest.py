import os
import sys
current_path = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))
sys.path.append(current_path)
print('ParentCurrent PATH: {0}'.format(current_path))
import unittest
# from dao.PostgresDriver import PostgresDriver
from dao.PostgresDao import PostgresDao
import pandas as pd


class PostgresDaoTest(unittest.TestCase):

    def setUp(self):
        # self.db_driver = PostgresDriver()
        # self.engine, self.metadata = self.db_driver.connect(
        #     'postgres', '123Postgres!', 'newspaper', 'localhost', 5432)
        # self.connection = self.engine.connect()
        self.dao = PostgresDao()

    def _steps(self):
        for name in sorted(dir(self)):
            if name.startswith('step'):
                yield name, getattr(self, name)

    def step_01_delete_table(self):
        self.dao.delete_table('test_table')

    def step_02_create_table(self):
        self.dao.create_table(['test_table', 'id', 'col1', 'col2'])

    def step_03_read_table(self):
        df = self.dao.read_table('test_table')
        self.assertEqual(3, len(df.columns.tolist()))

    def step_03_update_table(self):
        df = self.dao.read_table('test_table')
        table_parameters = list()
        table_parameters.append('test_table')
        for col in df.columns.values.tolist():
            table_parameters.append(col)
        df.loc[-1] = ['1', 'a', 'b']
        df.index = df.index + 1  # shifting index
        df = df.sort_index()
        self.dao.write_table(df, table_parameters)
        df = self.dao.read_table('test_table')
        self.assertEqual(1, len(df.values.tolist()))

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