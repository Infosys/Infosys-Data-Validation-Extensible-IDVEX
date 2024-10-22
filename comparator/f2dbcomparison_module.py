'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import json
import time

import pandas as pd
import pymongo

from comparator.comparision import dataframes_record_based_comparison, \
    dataframes_column_based_comparison
from comparision_checks.conversion_check import is_csv_conversion_required, get_dataframes
from comparision_checks.convert_module import convert
from utils.ServerLogs import logger
from utils.connect_to_db import connect
from utils.dataframes_utility import get_dataframe_from_file, get_dataframe_from_table
from utils.db_connect import get_database_type, mysql_db_Obj, sql_server_db_obj, oracle_obj, \
    postgres_db_obj, mongo_client_obj, drill_obj, s4_hana_obj
from utils.exceptions import DataFrameReadError


def check_for_conversion(request_data, src_columns):
    """Function to check for source file conversion"""

    source_file = request_data.get('sourceFilePath')
    source_type = request_data.get('sourceFileType')
    source_delim = request_data.get('sourceDelimiter')
    source_tag = request_data.get('sourceTag')

    if (request_data.get('sourceDatabaseAlias') in ["", None]):
        if is_csv_conversion_required(source_type):
            source_file, status = convert(
                None, source_file, source_type, source_delim, source_tag)

        source_df = get_dataframes(source_file, source_type, src_columns, source_delim)

    else:
        source_query, source_connection = prepare_dataframes_from_tables_source(request_data)
        source_df = get_dataframe_from_file(
            source_query, source_connection, source_delim, source_file, src_columns)
    return source_df


def prepare_datafrmaes_from_tables(request_data):
    """Caller Function to get target dataframe"""

    target_connectionAlias = request_data.get('targetDatabaseAlias')
    target_query = request_data.get('targetTableQuery')
    if request_data.get('targetDatabse'):
        target_database = request_data.get('targetDatabse')
    elif request_data.get('targetDatabase'):
        target_database = request_data.get('targetDatabase')
    else:
        raise Exception('Database Not Found')

    t_bool_val = connect(request_data['target_connection_details'], target_connectionAlias)
    if t_bool_val:
        target_databaseType = get_database_type(target_connectionAlias)
    else:
        raise ConnectionError('Unable to Connect to Target Db')

    if target_databaseType == 'Mysql':
        target_connection = mysql_db_Obj(
            target_database, target_connectionAlias)
    elif target_databaseType == 'MsSql':
        target_connection = sql_server_db_obj(
            target_database, target_connectionAlias)
    elif target_databaseType == 'Oracle':
        target_connection = oracle_obj(target_connectionAlias)
    elif target_databaseType == 'MongoDB':
        target_connection = mongo_client_obj(
            target_database, target_connectionAlias)

    elif target_databaseType == 'Postgresql':

        target_connection = postgres_db_obj(
            target_database, target_connectionAlias)
    elif target_databaseType == "Drill":
        target_connection = drill_obj(target_connectionAlias)
    elif target_databaseType == "Hana":
        target_connection = s4_hana_obj(target_connectionAlias)
    else:
        raise Exception('Connection to database Failed , please retry after sometime')

    return target_query, target_connection


def prepare_dataframes_from_tables_source(request_data):
    """Caller Function to get source dataframe"""

    source_connectionAlias = request_data.get('sourceDatabaseAlias')

    source_query = request_data.get('sourceTableQuery')
    if request_data.get('sourceDatabse'):
        source_database = request_data.get('sourceDatabse')
    elif request_data.get('sourceDatabase'):
        source_database = request_data.get('sourceDatabase')
    else:
        raise Exception('Empty sourceDatabase')

    source_databaseType = get_database_type(source_connectionAlias)
    source_connection = prepare_dataframes_from_tables_new(source_databaseType, source_database, source_connectionAlias)

    return source_query, source_connection


def prepare_dataframes_from_tables_new(databaseType, database, connectionAlias):
    """Caller Function to prepare dataframe from table"""

    if databaseType == 'Mysql':
        connection = mysql_db_Obj(database, connectionAlias)
    elif databaseType == 'MsSql':
        connection = sql_server_db_obj(database, connectionAlias)
    elif databaseType == 'Oracle':
        connection = oracle_obj(connectionAlias)
    elif databaseType == 'Postgresql':
        connection = postgres_db_obj(database, connectionAlias)
    elif databaseType == 'MongoDB':
        connection = mongo_client_obj(database, connectionAlias)
    elif databaseType == "Drill":
        connection = drill_obj(connectionAlias)
    elif databaseType == "Hana":
        connection = s4_hana_obj(connectionAlias)
    return connection


def file_to_db_comparison_new(request_data):
    """Function for file to db comparision"""

    response = 0

    logger.info(f"File to DB comparison, this is the request data in fetch testcase: -  {request_data}")
    source_primary_key = request_data.get('primaryKey')

    record_or_column = request_data.get('record_or_column')
    if record_or_column == 'column' and (source_primary_key == ''):
        raise Exception('please submit primaryKey for column based comparison')

    reportType = request_data.get('reportType')
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

    start_time = time.time()

    source_type = request_data.get('sourceFileType')

    source_df = check_for_conversion(request_data, sourceMap)

    target_query, target_connection = prepare_datafrmaes_from_tables(
        request_data)

    if isinstance(target_connection, pymongo.database.Database):
        logger.info("Trying to utils to Mongoinside db 2 db")
        cursor = target_connection[request_data.get('targetTableName')].find()
        df = pd.DataFrame(list(cursor))

        df1 = df.drop(columns=['_id'])
        target_df = df1
        target_df = target_df[targetMap]

    else:
        target_df = get_dataframe_from_table(
            target_query, target_connection, targetMap)

    source_df = pd.DataFrame(source_df)
    target_df = pd.DataFrame(target_df)

    logger.info("Source df from File to DB:")
    logger.info(source_df)
    logger.info("Target df from File to DB:")
    logger.info(target_df)

    try:
        if target_df is None or source_df is None:
            raise DataFrameReadError
        if source_df.empty or target_df.empty:
            raise Exception('Empty Source/Target - please check inputs')

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
        message = "Query Could Not be executed."
        response = []
        end_time = time.time()
        time_elapsed = round((end_time - start_time), 2)

    except Exception as E:
        message = f'Internal Error Occured - {E}'
        logger.error(f"This exception occurred in F2DB:-{E}")
        end_time = time.time()
        time_elapsed = round((end_time - start_time), 2)

    return message, response, time_elapsed
