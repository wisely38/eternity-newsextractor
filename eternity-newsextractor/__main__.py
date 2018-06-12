from __future__ import absolute_import

import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.exit(
        load_entry_point('eternity-newsextractor', 'console_scripts', 'twist')()
    )