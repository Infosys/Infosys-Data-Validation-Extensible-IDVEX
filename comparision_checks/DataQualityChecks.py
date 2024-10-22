'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import csv
import datetime
import os

import pandas as pd
from pandas.api.types import is_numeric_dtype

from utils.ServerLogs import logger


def createTempFolder():
    """Function to create temp folder"""

    try:
        prj_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        temp_folder = os.path.join(prj_dir, r"tempfiles")
        os.makedirs(temp_folder, exist_ok=True)
    except Exception as e:
        logger.error(f"Error in DataQualityChecks : createFileName() : {e}")


def createFileName(ext):
    """Function to create file name"""

    try:
        utc_timestmp = datetime.datetime.now()
        timestr = utc_timestmp.strftime("%Y%m%d-%H%M%S")
        utc_timestmp = str(utc_timestmp)[:-3]
        filename = f"temp_{timestr}.{ext}"
        return filename
    except Exception as e:
        logger.error(f"Error in DataQualityChecks : createFileName() : {e}")
        logger.info(e)


def read_data(data_filepath: str):
    """function to read data"""

    df = None
    try:
        if data_filepath.endswith(".csv"):
            df = pd.read_csv(data_filepath)
            return df
        elif data_filepath.endswith(".xlsx"):
            df = pd.read_excel(data_filepath)
            return df
        else:
            return df
    except Exception as e:
        logger.error(f"Error in DataQualityChecks : read_data() : {e}")
        return None


def find_outliers_pandas(data, column):
    """Function to find outliers"""

    try:
        Q1 = data[column].quantile(0.25)
        Q3 = data[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        outliers = data[(data[column] < lower_bound) | (data[column] > upper_bound)]
        return len(outliers)
    except Exception as e:
        logger.error(f"Error in DataQualityChecks : find_outliers_pandas() : {e}")
        return None


def checkMissingValues(data):
    """Function to check missing values"""

    temp = {}
    try:
        for col in data.columns.to_list():
            missing_values = data[col].isnull().sum()
            temp[col] = int(missing_values)
        return temp
    except Exception as e:
        logger.error(f"Error in DataQualityChecks : checkMissingValues() : {e}")
        return None


def checkDuplicateValues(data):
    """Function to check duplicate values"""
    try:
        duplicate_rows_count = len(data) - len(data.drop_duplicates())
        return duplicate_rows_count
    except Exception as e:
        logger.error(f"Error in DataQualityChecks : checkDuplicateValues() : {e}")
        return None


def outliersAndRangeCheck(data):
    """Function for outlier and range check"""
    try:
        # outliers detection
        outliers_dict = {}
        # range check
        range_check = {}
        for column in data.columns.to_list():
            if is_numeric_dtype(data[column]):
                outliers_dict[column] = find_outliers_pandas(data, column)
                range_check[column] = {"min": 0, "max": 0}
                range_check[column]["min"] = float(data[column].min())
                range_check[column]["max"] = float(data[column].max())
            else:
                outliers_dict[column] = 'NA'
                range_check[column] = 'NA'
        return outliers_dict, range_check
    except Exception as e:
        logger.error(f"Error in DataQualityChecks : outliersAndRangeCheck() : {e}")
        return None, None


def getDataTypes(data):
    """Function to get data types"""
    try:
        data_types = data.dtypes
        temp = {}
        for col, val in data_types.items():
            val = str(val)
            if val == "object":
                temp[col] = "Text"
            elif val == "float64" or val == "int64":
                temp[col] = "Number"
            elif val == "bool":
                temp[col] = "Boolean"
            elif val == "datetime64":
                temp[col] = "DateTime"
            else:
                temp[col] = "Undefined"

        return temp
    except Exception as e:
        logger.error(f"Error in DataQualityChecks : getDataTypes() : {e}")
        return None


def createCSVFile(quality_checks):
    """Function to create csv files"""
    try:
        createTempFolder()
        prj_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        output_folder = os.path.join(prj_dir, "tempfiles")
        # add initial rows to csv data
        csv_data = [["IDVEX Data Quality Checks Report"], ["\n"], ["row_count", quality_checks["row_count"]],
                    ["column_count", quality_checks["column_count"]],
                    ["duplicate_rows_check", quality_checks["duplicate_rows_check"]], ["\n"]]
        # create columns in csv
        columns = [" "] + list(quality_checks["data_types_check"].keys())
        csv_data.append(columns)
        # create rows with different checks
        checks = ["data_types_check", "missing_values_check", "outliers_check", "range_check"]
        for check in checks:
            temp = [check]
            for i in range(1, len(columns)):
                if temp == "range_check":
                    if quality_checks[check][columns[i]] == "NA":
                        temp.append(quality_checks[check][columns[i]])
                    else:
                        temp.append(str(quality_checks[check][columns[i]]))
                else:
                    temp.append(quality_checks[check][columns[i]])
            csv_data.append(temp)
        # create temp csv file
        output_file_path = os.path.join(output_folder, createFileName("csv"))
        with open(output_file_path, 'w', newline='') as fb:
            write = csv.writer(fb)
            write.writerows(csv_data)
        return output_file_path
    except Exception as e:
        logger.error(f"Error in DataQualityChecks : perform_quality_checks() : {e}")
        return {"Error": "Error while creating csv file"}


def perform_quality_checks(data_filepath: str):
    """Function to perform quality checks"""
    try:
        data = read_data(data_filepath)
        if isinstance(data, pd.DataFrame):
            quality_checks = {}
            # total rows
            quality_checks["row_count"] = len(data)
            # total cols
            quality_checks["column_count"] = len(data.columns)
            # check datatypes
            quality_checks["data_types_check"] = getDataTypes(data)
            # missing value check
            quality_checks["missing_values_check"] = checkMissingValues(data)
            # duplicate rows
            quality_checks["duplicate_rows_check"] = checkDuplicateValues(data)
            # outliers and range check
            outliers_dict, range_check = outliersAndRangeCheck(data)
            quality_checks["outliers_check"] = outliers_dict
            quality_checks["range_check"] = range_check
            return quality_checks
        else:
            return {"Error": "Can't process the data, invalid format..."}
    except Exception as e:
        logger.error(f"Error in DataQualityChecks : perform_quality_checks() : {e}")
        return {"Error": "Unexpected Error encountered during quality checks."}


def perform_quality_checks_csv(data_filepath: str):
    """Function to perform quality checks"""
    try:
        data = read_data(data_filepath)
        if isinstance(data, pd.DataFrame):
            quality_checks = {}
            # total rows
            quality_checks["row_count"] = len(data)
            # total cols
            quality_checks["column_count"] = len(data.columns)
            # check datatypes
            quality_checks["data_types_check"] = getDataTypes(data)
            # missing value check
            quality_checks["missing_values_check"] = checkMissingValues(data)
            # duplicate rows
            quality_checks["duplicate_rows_check"] = checkDuplicateValues(data)
            # outliers and range check
            outliers_dict, range_check = outliersAndRangeCheck(data)
            quality_checks["outliers_check"] = outliers_dict
            quality_checks["range_check"] = range_check
            csv_filepath = createCSVFile(quality_checks)
            quality_checks["csv_filepath"] = csv_filepath
            return quality_checks
        else:
            return {"Error": "Can't process the data, invalid format..."}
    except Exception as e:
        logger.error(f"Error in DataQualityChecks : perform_quality_checks() : {e}")
        return {"Error": "Unexpected Error encountered during quality checks."}
