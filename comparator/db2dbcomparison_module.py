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
from utils.ServerLogs import logger
from utils.connect_to_db import connect
from utils.dataframes_utility import get_dataframe_from_table
from utils.db_connect import get_database_type, mysql_db_Obj, sql_server_db_obj, oracle_obj, \
    postgres_db_obj, drill_obj, \
    s4_hana_obj, mongo_client_obj
from utils.exceptions import DataFrameReadError


######
def prepare_datafrmaes_from_tables(request_data):
    """Function to prepare data from tables as Dataframes"""

    source_connectionAlias = request_data.get('sourceDatabaseAlias')
    source_query = request_data.get('sourceTableQuery')
    if request_data.get('sourceDatabse'):
        source_database = request_data.get('sourceDatabse')
    elif request_data.get('sourceDatabase'):
        source_database = request_data.get('sourceDatabase')
    else:
        raise Exception('Database Not Found')

    target_connectionAlias = request_data.get('targetDatabaseAlias')

    target_query = request_data.get('targetTableQuery')
    if request_data.get('targetDatabse'):
        target_database = request_data.get('targetDatabse')
    elif request_data.get('targetDatabase'):
        target_database = request_data.get('targetDatabase')
    else:
        raise Exception('Database Not Found')

    s_bool_val = connect(request_data['source_connection_details'], source_connectionAlias)
    if s_bool_val:
        source_databaseType = get_database_type(source_connectionAlias)
    else:
        raise ConnectionError('Unable to Connect to Source Db')

    t_bool_val = connect(request_data['target_connection_details'], target_connectionAlias)
    if t_bool_val:
        target_databaseType = get_database_type(target_connectionAlias)
    else:
        raise ConnectionError('Unable to Connect to Target Db')

    if source_databaseType == 'Mysql':
        source_connection = mysql_db_Obj(
            source_database, source_connectionAlias)
    elif source_databaseType == 'MsSql':
        source_connection = sql_server_db_obj(
            source_database, source_connectionAlias)
    elif source_databaseType == 'Oracle':
        source_connection = oracle_obj(source_connectionAlias)

    elif source_databaseType == 'Postgresql':
        source_connection = postgres_db_obj(
            source_database, source_connectionAlias)

    elif source_databaseType == "Drill":
        source_connection = drill_obj(source_connectionAlias)

    elif source_databaseType == "Hana":
        source_connection = s4_hana_obj(source_connectionAlias)
    elif source_databaseType == 'MongoDB':
        source_connection = mongo_client_obj(
            source_database, source_connectionAlias)
    else:
        raise Exception('Connection to database Failed , please retry after sometime')

    if target_databaseType == 'Mysql':
        target_connection = mysql_db_Obj(
            target_database, target_connectionAlias)
    elif target_databaseType == 'MsSql':
        target_connection = sql_server_db_obj(
            target_database, target_connectionAlias)
    elif target_databaseType == 'Oracle':
        target_connection = oracle_obj(target_connectionAlias)

    elif target_databaseType == 'Postgresql':
        target_connection = postgres_db_obj(
            target_database, target_connectionAlias)

    elif target_databaseType == 'MongoDB':
        target_connection = mongo_client_obj(
            target_database, target_connectionAlias)
    elif target_databaseType == "Drill":
        target_connection = drill_obj(target_connectionAlias)
    elif target_databaseType == "Hana":
        target_connection = s4_hana_obj(target_connectionAlias)
    else:
        raise Exception('Connection to database Failed , please retry after sometime')

    return source_query, source_connection, target_query, target_connection


def db_to_db_comparison(request_data):
    """Function for DB to DB Comparison"""

    logger.info("Came for db comparison")

    start_time = time.time()
    source_primary_key = request_data.get('primaryKey')

    record_or_column = request_data.get('record_or_column')
    if record_or_column == 'column' and source_primary_key == '':
        raise Exception('please submit primaryKey for column based comparison')
    reportType = request_data.get('reportType')

    colMapping_temp = str(request_data.get('columnMapping'))
    if not colMapping_temp:
        raise Exception('Invalid Column Mappings')

    colMapping_temp = colMapping_temp.replace("\'", "\"")
    colMapping = json.loads(colMapping_temp)
    sourceMap = []
    targetMap = []
    for d in colMapping:
        if "sourceColumn" in d:
            sourceMap.append(d["sourceColumn"])
        if "targetColumn" in d:
            targetMap.append(d["targetColumn"])

    source_query, source_connection, target_query, target_connection = prepare_datafrmaes_from_tables(
        request_data)

    if isinstance(source_connection, pymongo.database.Database):
        logger.info("Trying to utils to Mongoinside db 2 db")
        cursor = source_connection[request_data.get('sourceTableName')].find()
        df = pd.DataFrame(list(cursor))

        df1 = df.drop(columns=['_id'])
        source_df = df1
        source_df = source_df[sourceMap]

    else:

        source_df = get_dataframe_from_table(
            source_query, source_connection, sourceMap)

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

    try:
        if source_df is None or target_df is None:
            raise DataFrameReadError

        if source_df.empty or target_df.empty:
            raise Exception('Empty Source/Target - please check inputs')

        target_df = target_df.replace("\r", "")

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
        time_elapsed = round((end_time - start_time), 2)
        logger.info("Query execution error is happening here.")
        message = "Query Could Not be executed."
        response = []

    return message, response, time_elapsed
