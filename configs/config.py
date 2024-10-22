'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import os

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RESULTS_FILE_DIR = os.path.join(BASE_DIR, 'Output')

SUPPORTED_FILE_TYPES = ['txt', 'parquet', 'json', 'xml', "avro", "csv", "pdf"]
SUPPORTED_DBS = ["Mysql", "MsSql", "Oracle", "MongoDB", "Postgresql", "Drill", "Hana"]
