'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import datetime
import json
import os
from io import BytesIO

import fastavro  # For Avro
import pandas as pd
import pyarrow.parquet as pq  # For Parquet
import tabula

from utils.ServerLogs import logger


def createTempFolder():
    """Function to create temp folder"""
    prj_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    temp_folder = os.path.join(prj_dir, r"tempfiles")
    os.makedirs(temp_folder, exist_ok=True)


def createFileName(ext):
    """Function to create filename"""
    try:
        utc_timestmp = datetime.datetime.now()
        timestr = utc_timestmp.strftime("%Y%m%d-%H%M%S")
        utc_timestmp = str(utc_timestmp)[:-3]
        filename = f"temp_{timestr}.{ext}"
        return filename
    except Exception as e:
        logger.error(f"Error in DataConverter:createFileName() : {e}")


def read_json(filename: str) -> dict:
    """Function to read json"""
    data = {}
    try:
        with open(filename, "r", encoding="utf8") as file:
            # Load the entire file content as a single string
            content = file.read()
            # Parse the content as a list of dictionaries
            temp = json.loads(content)
            for i in range(len(temp)):
                data[str(i)] = temp[i]
        return data
    except Exception as e:
        logger.error(f"Reading {filename} encountered an error: {e}")
        return data


def generate_csv_data_from_json(data: dict) -> str:
    """function to generate csv from json"""
    # Defining CSV columns in a list to maintain 
    # the order 
    try:
        if len(data.keys()) > 0:
            csv_columns = data['0'].keys()
            # Generate the first row of CSV
            csv_data = ''
            new_row = list()
            new_row.append(",".join(csv_columns) + "\n")
            # Generate the single record present 

            record_index = data.keys()
            for i in record_index:
                temp = []

                for key, val in data[i].items():
                    temp.append(str(val))
                new_row.append(",".join(temp) + "\n")

            # Concatenate the record with the column information 
            # in CSV format 
            csv_data += "".join(new_row) + "\n"
            return csv_data
        else:
            return ""

    except Exception as e:
        logger.error(f"Error while creating csvdata DataConverter:generate_csv_data_from_json() : {e}")
        return ""


def write_jsontocsv_file(data: str, filepath: str):
    """function to write json to csv"""
    try:
        with open(filepath, "w+", encoding="utf-8") as f:
            f.write(data)
    except Exception as e:
        logger.error(
            f"Error : DataConverter:write_jsontocsv_file(), Saving data to {filepath} encountered an error, error={e}")


def convert_to_csv(data, format):
    """Converts data to CSV format.
    Args:
        data (str, bytes): The input data.
        format (str): The input data format (json, xml, pdf, parquet, avro).
    Returns:
        bytes: The CSV content.

    Raises:
        ValueError: If the input format is not supported or file upload fails.
    """
    try:
        createTempFolder()
        prj_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        output_folder = os.path.join(prj_dir, "tempfiles")
        if format == 'json':
            new_data = pd.read_json(data)
            output_file_path = os.path.join(output_folder, createFileName("csv"))
            new_data.to_csv(output_file_path, encoding='utf-8', index=False)
            return output_file_path
        elif format == 'xml':
            df = pd.read_xml(data)
            output_file_path = os.path.join(output_folder, createFileName("csv"))
            df.to_csv(output_file_path, encoding='utf-8', index=False)
            return output_file_path
        elif format == 'pdf':
            list_of_dfs = tabula.read_pdf(data, pages="all")
            output_file_path = os.path.join(output_folder, createFileName("csv"))
            table_count = 1
            with open(output_file_path, 'a') as f:
                for df in list_of_dfs:
                    f.write(f"Table {table_count}\n")
                    df.to_csv(f, encoding='utf-8')
                    table_count += 1
            return output_file_path
        elif format == 'parquet':
            # with open(data, "rb") as fb:
            table = pq.read_table(data)
            df = table.to_pandas()
            output_file_path = os.path.join(output_folder, createFileName("csv"))
            df.to_csv(output_file_path, encoding='utf-8', index=False)
            return output_file_path
        elif format == 'avro':
            with open(data, "rb") as fb:
                reader = fastavro.reader(BytesIO(fb.read()))
                records = [dict(r) for r in reader]
                df = pd.DataFrame(records)
            output_file_path = os.path.join(output_folder, createFileName("csv"))
            df.to_csv(output_file_path, encoding='utf-8', index=False)
            return output_file_path
        else:
            logger.error(f"Error in DataConverter:convert_to_csv, unsupported format={format}")
            return ""
    except Exception as e:
        logger.error(f"Error in DataConverter:convert_to_csv, format={format}, error={e}")
        return ""
