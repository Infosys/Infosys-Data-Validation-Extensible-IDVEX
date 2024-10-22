'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

from utils.ServerLogs import logger

def check_columns_length(src, tgt):
    """Function to check column length"""
    logger.info(f"!!!!!!src!!!!Len - {len(src.columns)}")
    logger.info(f"!!!!!!tgt!!!!Len - {len(tgt.columns)}")
    return len(src.columns) == len(tgt.columns)


def check_columns_names(src, tgt):
    """Function to check column names"""
    logger.info(f"src@@@@@@@@ - {src.columns.values}")
    logger.info(f"tgt@@@@@@@@ - {tgt.columns.values}")
    return set(src.columns.values) == set(tgt.columns.values)
