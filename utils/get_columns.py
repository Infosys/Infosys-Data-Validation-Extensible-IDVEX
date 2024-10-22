'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import fastavro
import pandas as pd
import pydrill
import pymongo

from comparator.db2dbcomparison_module import prepare_datafrmaes_from_tables as prepare_datafrmaes_from_tables_db2db
from comparator.f2dbcomparison_module import prepare_datafrmaes_from_tables as prepare_datafrmaes_from_tables_f2db
from utils.ServerLogs import logger
from utils.dataframes_utility import get_json_dataframe, get_xml_dataframe, dynamic_query_hbase_convert, \
    get_parquet_dataframe
from utils.db_connect import get_database_type
from utils.exceptions import DataFrameReadError


def read_file(file_path, extension):
    """Function to read uploaded file"""
    try:

        extension_cal = file_path.split('.')[-1]
        if extension_cal != extension:
            raise Exception('Uploaded File is of a different type than the one mentioned')

        if extension == 'txt' or extension == 'csv':
            df = pd.read_csv(file_path)
        elif extension == 'json':

            df = get_json_dataframe(file_path)

        elif extension == 'xml':
            df = get_xml_dataframe(file_path)
        elif extension == 'parquet':
            df = get_parquet_dataframe(file_path)

        elif extension == 'avro':
            with open(file_path, 'rb') as f:
                reader = fastavro.reader(f)
                records = [r for r in reader]
                df = pd.DataFrame(records)
        else:
            logger.error('Unsupported file format')
            return None

        if df is None:
            raise Exception('Unable to read the file, please try a different input')

        df = df.astype(str)
        return df.columns.tolist(), df.head(10)

    except Exception as e:
        logger.error(f"Exception - {e}")
        raise Exception(f"Exception - {e}")


def get_columns_from_database_2_database(request_data):
    """Function to get columns from db"""
    try:

        source_query, source_connection, target_query, target_connection = prepare_datafrmaes_from_tables_db2db(
            request_data)

        target_databaseType = get_database_type(request_data['targetDatabaseAlias'])
        source_databaseType = get_database_type(request_data['sourceDatabaseAlias'])

        if isinstance(source_connection, pymongo.database.Database):
            logger.info("Trying to utils to Mongoinside db 2 db")
            cursor = source_connection[request_data.get('sourceTableName')].find()
            df = pd.DataFrame(list(cursor))

            df1 = df.drop(columns=['_id'])
            source_df = df1
            columns_source = source_df.columns.tolist()


        else:

            columns_source, source_df = get_dataframe_from_table(
                source_query, source_connection, source_databaseType)

        if isinstance(target_connection, pymongo.database.Database):
            logger.info("Trying to utils to Mongoinside db 2 db")
            cursor = target_connection[request_data.get('targetTableName')].find()
            df = pd.DataFrame(list(cursor))

            df1 = df.drop(columns=['_id'])
            target_df = df1
            columns_target = target_df.columns.tolist()

        else:
            columns_target, target_df = get_dataframe_from_table(
                target_query, target_connection, target_databaseType)

        if source_df is None or target_df is None:
            raise DataFrameReadError

        if source_df.empty or target_df.empty:
            raise Exception('Empty Source/Target - please check inputs')

    except DataFrameReadError:

        logger.info("Query execution error is happening here.")
        raise Exception("Query Could Not be executed.")

    return columns_source, columns_target, source_df, target_df


def get_columns_from_file_2_database(request_data):
    """Functions to get columns from file and db"""
    try:
        target_query, target_connection = prepare_datafrmaes_from_tables_f2db(
            request_data)

        target_databaseType = get_database_type(request_data['targetDatabaseAlias'])

        if isinstance(target_connection, pymongo.database.Database):
            logger.info("Trying to utils to Mongoinside db 2 db")
            cursor = target_connection[request_data.get('targetTableName')].find()
            df = pd.DataFrame(list(cursor))

            df1 = df.drop(columns=['_id'])
            target_df = df1
            columns_target = target_df.columns.tolist()

        else:
            columns_target, target_df = get_dataframe_from_table(
                target_query, target_connection, target_databaseType)

        if target_df is None:
            raise DataFrameReadError

        if target_df.empty:
            raise Exception('Empty Source/Target - please check inputs')

    except DataFrameReadError:

        logger.info("Query execution error is happening here.")
        raise Exception("Query Could Not be executed.")

    return columns_target, target_df


def get_dataframe_from_table(query, connection, type_of_db):
    """Function to read data from table"""
    if isinstance(connection, pymongo.database.Database):
        logger.info("Trying to utils to Mongo")

    try:
        if type_of_db:
            if type_of_db == 'mongo':
                logger.info("coming in mongo")
                cursor = connection['docs'].find(
                    {}, {'_id': 0, "Order Priority": 0, "Order Date": 0, "Ship Date": 0})
                df = pd.DataFrame(list(cursor))
                logger.info(df)
                return df.columns.tolist(), df.head(10)
            elif isinstance(connection, pydrill.client.PyDrill):
                if "hbase." in query:
                    query_output = connection.query(query + " limit 1")
                    hbase_column, hbase_output = dynamic_query_hbase_convert(
                        query_output, connection, query, query_output.columns)
                    df = hbase_output.to_dataframe()
                else:
                    query_output = connection.query(query, timeout=600)

                    df = query_output.to_dataframe()

                return df.columns.tolist(), df.head(10)
            else:

                df = pd.read_sql(query, connection).fillna('')
                return df.columns.tolist(), df.head(10)
        else:
            df = pd.read_sql(query, connection).fillna('')
            return df.columns.tolist(), df.head(10)
    except Exception as e:
        logger.error(f"Exception occurred while running SQL query: - {e}")
        raise Exception(f"Exception occurred while running SQL query: - {e}")
