'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import csv
import json
import os
import xml.etree.cElementTree as ET
from io import BytesIO
from pathlib import Path

import fastavro
import pandas as pd
import pyarrow.parquet as pq
from starlette import status

from comparision_checks.filescanner import check_for_csv_injection
from configs import config as settings
from utils.ServerLogs import logger
from utils.exceptions import CSVInjectionError


def convert_parquet(file_to_convert, converted_filetype):
    """Function to Convert parquet to csv"""
    df1 = pq.read_table(file_to_convert).to_pandas()
    df1.to_csv(path_or_buf=converted_filetype, encoding='utf-8', index=False)


def convert_xml(file_to_convert, converted_filetype, recordtag):
    """Function to Convert xml to csv"""

    def get_range(col):
        return range(len(col))

    try:

        tree = ET.parse(file_to_convert)
        root = tree.getroot()
        l = [{r[i].tag: r[i].text for i in get_range(r)} for r in root]
        df = pd.DataFrame.from_dict(l)
        df.to_csv(converted_filetype, index=False)

    except Exception as E:
        logger.error(f"Exception - {str(E)}")


def convert_json(file_to_convert, converted_filetype):
    """Function to Convert json to csv"""

    with open(file_to_convert, 'r') as f:
        data = json.load(f)
    # data is a list of str(json elements): [{..},{..},{..}]
    columns = [x for x in data[0]]
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(path_or_buf=converted_filetype, encoding='utf-8',
              index=False, quoting=csv.QUOTE_ALL)


def convert_txt(file_to_convert, converted_filetype, delimCare, delimiter):
    """Function to Convert Text to csv"""
    csv_file_obj = open(converted_filetype, 'w', encoding='utf-8', newline='')
    if delimCare == True and delimiter:
        ip_file = file_to_convert
        df = pd.read_csv(ip_file, sep=delimiter)
        if check_for_csv_injection(df):
            os.remove(ip_file)
            raise CSVInjectionError()

        df.to_csv(csv_file_obj, index=False)


def convert_avro(file_to_convert, converted_filetype):
    with open(file_to_convert, "rb") as fb:
        reader = fastavro.reader(BytesIO(fb.read()))
        records = [dict(r) for r in reader]
        df = pd.DataFrame(records)

    df.to_csv(converted_filetype, index=False)


def get_file(fname, fpath):
    """Function to get converted file path"""
    from datetime import datetime
    now = datetime.now()
    timestr = now.strftime("%H_%M_%S_%f")
    file_to_convert = fpath
    file_name = Path(fpath).name
    file_path = fpath[:-len(file_name)]
    temp = os.path.splitext(file_name)[0] + '_' + timestr
    converted_filetype = os.path.join(
        settings.RESULTS_FILE_DIR, temp + '_converted.csv')  # TODO: Ask if normal path variables would work
    return file_to_convert, converted_filetype


#
def convert(fname, fpath, ftype, delimiter, recordtag):
    """main function to convert any file to csv"""
    message = []
    output_file = ''
    ftype = ftype.lower()
    file_path = fpath

    try:
        file_to_convert, converted_filetype = get_file(fname, fpath)
        delimCare = True

        if ftype == 'parquet':
            convert_parquet(file_to_convert, converted_filetype)
            delimCare = False

        elif ftype == 'xml':
            convert_xml(file_to_convert, converted_filetype, recordtag)
            delimCare = False

        elif ftype == 'json':
            convert_json(file_to_convert, converted_filetype)
            delimCare = False

        elif ftype == 'txt':
            convert_txt(file_to_convert, converted_filetype,
                        delimCare, delimiter)

        elif ftype == 'avro':
            convert_avro(file_to_convert, converted_filetype)
            logger.info("Avro converted")
            delimCare = False

        else:
            raise Exception('file cannot be converted')

        output_file = converted_filetype
        status_code = status.HTTP_200_OK

    except FileNotFoundError as fnf:
        raise FileNotFoundError('File Not Found')


    except CSVInjectionError:
        raise CSVInjectionError('File Contains Malicious Data and can not be processed Further')


    except Exception as E:
        logger.error(f"Exception - {E}")
        raise Exception('Some Error Occured converting the file')

    return output_file, status_code
