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

from __future__ import absolute_import

import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.exit(
        load_entry_point('eternity-newsextractor', 'console_scripts', 'twist')()
    )
