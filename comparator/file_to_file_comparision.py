'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import json
import time

from comparator.comparision import dataframes_record_based_comparison, \
    dataframes_column_based_comparison
from comparision_checks.conversion_check import is_csv_conversion_required, get_dataframes
from comparision_checks.convert_module import convert
from utils.ServerLogs import logger
from utils.exceptions import DataFrameReadError


def check_for_file_conversion(request_data):
    """Function to get source and target dataframes from file"""

    colMapping_temp = str(request_data.get('columnMapping'))
    colMapping_temp = colMapping_temp.replace("\'", "\"")

    colMapping = json.loads(colMapping_temp)
    sourceMap = []
    targetMap = []
    for d in colMapping:
        if "sourceColumn" in d:
            sourceMap.append(d["sourceColumn"])
        if "targetColumn" in d:
            targetMap.append(d["targetColumn"])

    source_file = request_data.get('sourceFilePath')
    source_type = request_data.get('sourceFileType')
    source_delim = request_data.get('sourceDelimiter')
    source_tag = request_data.get('sourceTag')
    src_columns = sourceMap

    target_file = request_data.get('targetFilePath')
    target_type = request_data.get('targetFileType')
    target_delim = request_data.get('targetDelimiter')
    target_tag = request_data.get('targetTag')

    tgt_columns = targetMap
    if (request_data.get('sourceDatabaseAlias') in ["", None]):

        if is_csv_conversion_required(source_type):
            logger.info("======================conversion Happening===============")
            source_file, status = convert(
                None, source_file, source_type, source_delim, source_tag)

        source_df = get_dataframes(
            source_file, source_type, src_columns, source_delim)
    else:
        raise Exception('DataBase Url provided for file')

    if (request_data.get('targetDatabaseAlias') in ["", None]):
        if is_csv_conversion_required(target_type):
            logger.info("======================conversion Happening===============")
            target_file, status = convert(
                None, target_file, target_type, target_delim, target_tag)

        target_df = get_dataframes(
            target_file, target_type, tgt_columns, target_delim)
    else:
        raise Exception('DataBase Url provided for file')

    return source_df, target_df


def file_to_file_comparison(request_data):
    """Function for file to file comparison"""

    logger.info(f"File to File comparison, this is the request data in fetch testcase:- {request_data}")
    start_time = time.time()

    record_or_column = request_data.get('record_or_column')
    reportType = request_data.get('reportType')

    source_primary_key = request_data.get('primaryKey')

    if record_or_column == 'column' and (source_primary_key == ''):
        raise Exception('please submit primaryKey for column based comparison')

    source_df, target_df = check_for_file_conversion(request_data)

    response = []
    message = None

    try:
        if source_df is None or target_df is None:
            raise DataFrameReadError

        if record_or_column == "record":
            message, response = dataframes_record_based_comparison(
                source_df, target_df, reportType)
        else:

            message, response = dataframes_column_based_comparison(
                source_df, target_df, source_primary_key, reportType)

        end_time = time.time()
        time_elapsed = round((end_time - start_time), 2)

        logger.info(f"Time Elapsed    -{time_elapsed}")

    except DataFrameReadError:
        end_time = time.time()
        message = "File Read Error."
        response = []
        time_elapsed = round((end_time - start_time), 2)
    except Exception as E:
        end_time = time.time()
        logger.error(f"Exception - {E}")
        message = str(E)
        response = []
        time_elapsed = round((end_time - start_time), 2)

    return message, response, time_elapsed
