'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import os
from datetime import datetime

import pandas as pd

import configs.config as settings
from utils.ServerLogs import logger


def get_time_stamp():
    """Function to get timestamp"""
    now = datetime.now()
    timestr = now.strftime("%H_%M_%S_%f")
    return timestr


def create_result_reports(timestr):
    """Function to create result reports"""
    matched_file = os.path.join(os.path.join(
        settings.RESULTS_FILE_DIR, datetime.now().strftime("%Y_%m_%d_%H_%M_%S")),
        'matchfile%s.csv' % timestr)

    mismatched_file = os.path.join(os.path.join(
        settings.RESULTS_FILE_DIR, datetime.now().strftime("%Y_%m_%d_%H_%M_%S")),
        'additional_records_file%s.csv' % timestr)

    additional_in_source = os.path.join(os.path.join(
        settings.RESULTS_FILE_DIR, datetime.now().strftime("%Y_%m_%d_%H_%M_%S")),
        'additional_in_source%s.csv' % timestr)

    additional_in_target = os.path.join(os.path.join(
        settings.RESULTS_FILE_DIR, datetime.now().strftime("%Y_%m_%d_%H_%M_%S")),
        'additional_in_target%s.csv' % timestr)

    return matched_file, mismatched_file, additional_in_source, additional_in_target


def create_record_comparison_files(timestr):
    """Function to create record comparison files"""
    matched_file = os.path.join(os.path.join(
        settings.RESULTS_FILE_DIR, datetime.now().strftime("%Y_%m_%d_%H_%M_%S")), 'matchfile%s.csv' % timestr)

    mismatched_file = os.path.join(os.path.join(
        settings.RESULTS_FILE_DIR, datetime.now().strftime("%Y_%m_%d_%H_%M_%S")),
        'additional_records_file%s.csv' % timestr)

    source_only_file = os.path.join(os.path.join(
        settings.RESULTS_FILE_DIR, datetime.now().strftime("%Y_%m_%d_%H_%M_%S")), 'source_only_file%s.csv' % timestr)

    target_only_file = os.path.join(os.path.join(
        settings.RESULTS_FILE_DIR, datetime.now().strftime("%Y_%m_%d_%H_%M_%S")), 'target_only_file%s.csv' % timestr)

    return matched_file, mismatched_file, source_only_file, target_only_file


def generate_mismatched_excel_rename_csv(mismatched_file, mismatch_data, reportType):
    """Function to generate mismatched data report"""
    try:
        result_df = pd.DataFrame()

        for key, item in mismatch_data.items():
            if (len(item) != 0):
                result_df = pd.concat([result_df, item])

        result_df.to_csv(mismatched_file, index=False)


    except Exception as E:
        logger.error(f"Exception has occured in making mismatch record: - {str(E)}")

    return mismatched_file


# # -----------------------------New Addition--------------------------------
def generate_column_match_report(matched_file, matched_data, reportType):
    """Function to generate column comparison report"""
    try:
        result_df = pd.DataFrame()
        for key, item in matched_data.items():
            if (len(item) != 0):
                result_df = pd.concat([result_df, item])
        result_df.to_csv(matched_file, index=False)

    except Exception as E:
        logger.error(f"Exception has occurred in making match record: - {str(E)}")

    return matched_file


def generate_report(mismatch_data, matched_data, reportType, df):
    """Caller function to generate report"""

    timestr = get_time_stamp()
    matched_file, mismatched_file, additional_in_source, additional_in_target = create_result_reports(
        timestr)

    os.makedirs(os.path.dirname(matched_file), exist_ok=True)
    os.makedirs(os.path.dirname(mismatched_file), exist_ok=True)
    os.makedirs(os.path.dirname(additional_in_source), exist_ok=True)
    os.makedirs(os.path.dirname(additional_in_target), exist_ok=True)

    if reportType == "summary":
        mismatched_csv = generate_mismatched_excel_rename_csv(
            mismatched_file, mismatch_data, reportType)

        matched_csv = generate_column_match_report(matched_file, matched_data, reportType)
    else:
        raise TypeError('Invalid Report Type')

    return matched_csv, mismatched_csv


def generate_record_comparison_report(matched_data, mismatch_data, rows_SminusT, rows_TminusS, reportType):
    """Caller function to generate record comparison report"""
    logger.info(f"*****************Report type is :********************** - {reportType}")

    timestr = get_time_stamp()
    matched_file, mismatched_file, source_only_file, target_only_file = create_record_comparison_files(
        timestr)

    os.makedirs(os.path.dirname(matched_file), exist_ok=True)
    os.makedirs(os.path.dirname(mismatched_file), exist_ok=True)
    os.makedirs(os.path.dirname(source_only_file), exist_ok=True)
    os.makedirs(os.path.dirname(target_only_file), exist_ok=True)

    if reportType == "summary":
        generate_record_mismatched_csv(
            mismatched_file, mismatch_data, reportType)
        generate_record_matched_csv(matched_file, matched_data, reportType)
        generate_target_source_csv(
            target_only_file, rows_TminusS, reportType)
        generate_source_target_csv(
            source_only_file, rows_SminusT, reportType)

    elif reportType == "detailed mismatch":
        generate_record_mismatched_csv(
            mismatched_file, mismatch_data, reportType)
        generate_target_source_csv(
            target_only_file, rows_TminusS, reportType)
        generate_source_target_csv(
            source_only_file, rows_SminusT, reportType)
        matched_file = None

    elif reportType == "detailed match":
        mismatched_file = None
        source_only_file = None
        target_only_file = None
        generate_record_matched_csv(matched_file, matched_data, reportType)

    else:
        raise TypeError('Invalid Report Type')

    return matched_file, mismatched_file, source_only_file, target_only_file


def generate_record_matched_csv(matched_file, matched_data, reportType):
    """Function to generate matched file report"""
    if reportType == "summary":
        matched_data.head(1000000).to_csv(matched_file, index=False)
    else:
        matched_data.to_csv(matched_file, index=False)


#
def generate_record_mismatched_csv(mismatched_file, mismatched_data, reportType):
    """Function to generate mismatched report"""
    if reportType == "summary":
        mismatched_data.head(100000).to_csv(mismatched_file, index=False)
    else:
        mismatched_data.to_csv(mismatched_file, index=False)


#
def generate_source_target_csv(source_exclude_matched_file, rows_SminusT, reportType):
    """Function to only source records report"""
    if reportType == "summary":
        rows_SminusT.head(100000).to_csv(
            source_exclude_matched_file, index=False)
    else:
        rows_SminusT.to_csv(source_exclude_matched_file, index=False)


def generate_target_source_csv(target_exclude_matched_file, rows_TminusS, reportType):
    """Function to only target records report"""
    if reportType == "summary":
        rows_TminusS.head(100000).to_csv(
            target_exclude_matched_file, index=False)
    else:
        rows_TminusS.to_csv(target_exclude_matched_file, index=False)
