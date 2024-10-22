'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import logging
import os

prj_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

FORMAT = "[%(levelname)s  %(name)s %(module)s:%(lineno)s - %(funcName)s() - %(asctime)s]\n\t %(message)s \n"
TIME_FORMAT = "%d.%m.%Y %I:%M:%S %p"
LOGS_FOLDER = os.path.join(prj_dir, r"logs")
FILENAME = os.path.join(LOGS_FOLDER, r"OpensourcePrj_logs.log")

# create logs folder if not exists
os.makedirs(LOGS_FOLDER, exist_ok=True)

logging.basicConfig(format=FORMAT, datefmt=TIME_FORMAT, level=logging.INFO, filename=FILENAME)

logger = logging.getLogger("OpensourcePrj_logs")
