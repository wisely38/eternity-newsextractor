import os
import sys
current_path = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))
sys.path.append(current_path)
print('ParentCurrent PATH: {0}'.format(current_path))
import unittest
import pandas as pd
from CNNNewsExtractor import CNNNewsExtractor


class CNNNewsExtractorTest(unittest.TestCase):

    def setUp(self):
        self.extractor = CNNNewsExtractor()

    def _steps(self):
        for name in sorted(dir(self)):
            if name.startswith('step'):
                yield name, getattr(self, name)

    def step_01_extract(self):
        self.extractor.extract_site_metadata()
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
