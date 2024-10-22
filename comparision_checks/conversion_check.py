'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

from utils.dataframes_utility import get_parquet_dataframe, get_json_dataframe, get_xml_dataframe, get_dat_dataframe, \
    get_delimited_dataframe, get_avro_dataframe


def is_csv_conversion_required(file_type):
    """Function to check if input file needs conversion or not"""
    FILE_TYPES = ['txt', 'parquet', 'json', 'xml', "DAT file", "dat file", "csv", "avro"]
    return not (file_type in FILE_TYPES)


def convert_columns_str_to_list(columns):
    """Function to get columns as a list"""
    return columns.split(",")


def get_dataframes(file_name, file_type, col_list, delimiter):
    """Function to get data as a dataframe from file"""
    if file_type == 'parquet':
        data_frame = get_parquet_dataframe(file_name)

    elif file_type == 'json':
        data_frame = get_json_dataframe(file_name)

    elif file_type == 'xml':
        data_frame = get_xml_dataframe(file_name)

    elif file_type.lower() == "dat file":
        data_frame = get_dat_dataframe(file_name)

    elif file_type.lower() == 'avro':
        data_frame = get_avro_dataframe(file_name)

    else:
        data_frame = get_delimited_dataframe(file_name, col_list, delimiter)

    if col_list:
        data_frame = data_frame[col_list]
        print('SELECTING COLS')

    return data_frame
