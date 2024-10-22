'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import json
import re

import fastavro
import pandas as pd
import pydrill
import pymongo
import xmltodict

from reports.jsontodf import JsonToCsv
from utils.ServerLogs import logger


def get_parquet_dataframe(file_name):
    """Function to read parquet data"""
    return pd.read_parquet(file_name)


def get_avro_dataframe(file_name):
    """Function to get avro dataframe"""
    with open(file_name, 'rb') as f:
        reader = fastavro.reader(f)
        records = [r for r in reader]
        df = pd.DataFrame(records)
    return df


def get_delimited_dataframe(file_name, col_list, delimiter):
    """Function to read csv data"""
    try:
        if col_list:
            if delimiter != '':
                df = pd.read_csv(file_name, usecols=col_list, delimiter=delimiter, header=0, dtype=str,
                                 keep_default_na=False).fillna('')

                sample = df

                logger.info("Dataframe inside dataframe utility---")

                sample = sample.convert_dtypes()

                logger.info("Datatype is----")
                logger.info(sample.dtypes)

                return sample[col_list]
            else:

                df = pd.read_csv(file_name, usecols=col_list, header=0).fillna('')

                return df[col_list]
        else:
            return pd.read_csv(file_name, delimiter=delimiter).fillna('')


    except Exception as e:
        logger.error(f"Exception while dataframe creation : {e}")
        raise Exception(f"Exception while dataframe creation : {e}")


def get_dataframe_from_file(query, connection, delimiter, filepath, *args):
    """Function to read sql data from file"""
    try:
        if args:
            if isinstance(connection, pydrill.client.PyDrill):
                FullQuery = filepathPrepending(query, filepath)

                query_output = connection.query(FullQuery, timeout=600)
                df = query_output.to_dataframe()
                split_data = df["columns"].str.split('"')
                data_list = split_data.to_list()
                results = []
                for element_list in data_list:
                    if (data_list.index(element_list) == 0):
                        continue
                    temp = []
                    for element in element_list:
                        if (element in delimiter or element in ('[', ']')):
                            continue
                        element = element.replace(r"\r", "")
                        temp.append(element)
                    results.append(temp)

                new_df = pd.DataFrame(results, columns=args[0])
                return new_df
            else:
                df = pd.read_sql(query, connection, columns=args[0])
                return df[args[0]]
        else:
            return pd.read_sql(query, connection)
    except Exception as e:
        logger.error(f"Exception occured while running SQL query:  {e}")
        return None


def get_dat_dataframe(file_name):
    """Function to read text data"""
    df = pd.DataFrame()
    with open(file_name, "r") as f:
        text = f.read()
    lines = text.split()
    df["Records"] = lines
    return df


def get_json_dataframe(file_name, data_frame=False):
    """Function to read json data"""
    try:
        if data_frame:
            with open(file_name, 'r') as f:
                json_data = json.load(f)
                f.close()
            return get_data_frame_obj(json_data)
        else:
            obj = JsonToCsv(file_name)
            df = obj.get_df()
            df.rename(columns={0: 'json_keys', 1: 'json_data'}, inplace=True)
        return df

    except Exception as e:
        logger.error(f"This exception has happened while converting: - {e}")
        return None


def get_xml_dataframe(file_name, data_frame=False):
    """Function to read xml data"""
    try:
        with open(file_name, 'r') as file:
            xml_content = file.read()
            file.close()

        # Parse XML content into an OrderedDict
        data_dict = xmltodict.parse(xml_content)

        # Find the root element and the first key which contains the data
        root_key = list(data_dict.keys())[0]
        data_list = data_dict[root_key]

        # If the data list is actually a dictionary, convert it to a list of dictionaries
        if isinstance(data_list, dict):
            data_list = [data_list]
        elif not isinstance(data_list, list):
            raise ValueError("Unexpected XML structure")

        # Handle cases where nested dictionaries exist under a single key (like "person")
        if isinstance(data_list, list) and all(isinstance(elem, dict) and len(elem) == 1 for elem in data_list):
            key_to_flatten = list(data_list[0].keys())[0]
            data_list = [item[key_to_flatten] for item in data_list][0]

        # Flatten each dictionary in the list
        flat_data = [flatten_dict(item) for item in data_list]

        # Create a DataFrame from the flattened data
        df = pd.DataFrame(flat_data)

        return df
    except Exception as E:
        logger.error(f"Error happened while converting XML file: - {E}")
        raise Exception("Error happened while converting XML file:", E)


def flatten_dict(d, parent_key='', sep='.'):
    """
    Flatten a nested dictionary.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if all(isinstance(i, dict) for i in v):
                for idx, item in enumerate(v):
                    items.extend(flatten_dict(item, f"{new_key}[{idx}]", sep=sep).items())
            else:
                items.append((new_key, v))
        else:
            items.append((new_key, v))
    return dict(items)


def get_data_frame_obj(json_data):
    """Function to get json dataframe object"""
    parent_key = list(json_data.keys())
    child_key = ''
    for key in parent_key:
        child_key = list(json_data[key].keys())  # multiple Table future we can add here
    iterate_key = ''
    for key in child_key:
        key_iterate = type(json_data[parent_key[0]][key])
        if str(key_iterate) == "<class 'list'>":
            iterate_key = key
    df = pd.DataFrame(json_data[parent_key[0]][iterate_key])
    logger.info(list(df.columns.values))
    return df


###Db


def get_dataframe_from_table(query, connection, *args):
    """Function to read data from table"""
    try:
        if args:
            if isinstance(connection, pymongo.database.Database):
                logger.info("coming in mongo")
                cursor = connection['docs'].find(
                    {}, {'_id': 0, "Order Priority": 0, "Order Date": 0, "Ship Date": 0})
                df = pd.DataFrame(list(cursor))
                logger.info(df)
                return df[args[0]]

            elif isinstance(connection, pydrill.client.PyDrill):
                if "hbase." in query:
                    query_output = connection.query(query + " limit 1")
                    hbase_column, hbase_output = dynamic_query_hbase_convert(
                        query_output, connection, query, query_output.columns)
                    df = hbase_output.to_dataframe()
                else:
                    query_output = connection.query(query, timeout=600)

                    df = query_output.to_dataframe()

                return df[args[0]]
            else:
                logger.info(args[0])
                df = pd.read_sql(query, connection, columns=args[0]).fillna('')
                return df[args[0]]
        else:
            df = pd.read_sql(query, connection).fillna('')
            return df[args[0]]

    except Exception as e:
        logger.error(f"Exception occurred while running SQL query: - {e}")
        return None


def dynamic_query_hbase_convert(df, connection, query, columns):
    """
    Hbase UTF to Readable Format
    Query generate from reading one record from Hbase column family using limit
    """
    try:
        data_frame = pd.DataFrame(df)
        query = query.lower()
        query_dynamic = "select "
        column_count = 0
        logger.info(f"{query} - {columns}")
        for key in columns:
            if column_count == 1:
                query_dynamic += ','
            if key != 'row_key':
                data_family = data_frame.head(1)[key]
                json_data = data_family.to_json()
                parsed_data = json.loads(json_data)
                column_key = list(json.loads(parsed_data['0']).keys())
                for c_key in range(0, len(column_key)):
                    query_dynamic += "CONVERT_FROM(" + query.split('hbase.')[1] + "." + key + "." + column_key[
                        c_key] + ", 'UTF8') As " + column_key[c_key]
                    if c_key < (len(column_key) - 1):
                        query_dynamic += ","
                column_count = 1
        query_dynamic += " from " + query.split('from')[1]
        hbase_data = connection.query(query_dynamic)
        return hbase_data.columns, hbase_data
    except Exception as E:
        logger.error(f"Error in dynamic_query_hbase_convert -  {E}")
        return [], []


#####################connection

def filepathPrepending(query, filepath):
    """Function to prepare upload file path"""
    #
    if ("`" in query):
        res = [i.start() for i in re.finditer("`", query)]
        str1 = query[0:res[0]]
        str2 = query[res[1] + 1:len(query)]
        if (filepath in ["", None]):
            uploadFilepath = GetSetfilePath.get_upload_filepath()
        else:
            uploadFilepath = filepath
        query = str1 + "`" + uploadFilepath + "`" + str2
    return query


################upload filepath
class GetSetfilePath:
    """Class for filepath"""
    upload_filepath = ""

    @staticmethod
    def set_upload_filepath(filepath):
        """Set filepath"""
        GetSetfilePath.upload_filepath = filepath
        logger.info(f"filePath: - {GetSetfilePath.upload_filepath}")

    @staticmethod
    def get_upload_filepath():
        """Get Filepath"""
        return GetSetfilePath.upload_filepath
