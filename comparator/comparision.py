'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import traceback

import pandas as pd
from pandas.errors import InvalidIndexError

from comparision_checks.columnchecks import check_columns_length, check_columns_names
from reports.comparison_reports import generate_record_comparison_report, generate_report
from reports.response import RecordResponse, ColumnResponse
from utils.ServerLogs import logger
from utils.exceptions import ColumnsLengthMismatch, ColumnsNamesMismatch


###############Record comparision
def handling_datatypes(src_df, tgt_df):
    """Function to handle source and target data types"""
    logger.info(f"datatypes are -{src_df.astype(object)}, {tgt_df.astype(object)}")
    src_df1 = src_df.astype(str)
    tgt_df1 = tgt_df.astype(str)
    return src_df1, tgt_df1


def MeaningfulNames(name):
    """Function to get result alias"""
    if name == "both":
        return "Matched"
    elif name == "left_only":
        return "Source"
    else:
        return "Target"


def dataframes_record_based_comparison(src_df, tgt_df, reportType):
    """Function to compare dataframes based on record"""

    logger.info(f'{"*" * 50} "Record comparison started",{"*" * 50}')
    target_cols = list(tgt_df.columns)
    tgt_df.columns = src_df.columns  # Renaming target columns to maintain consistency
    record_response = RecordResponse()
    message = ''
    try:

        if not check_columns_length(src_df, tgt_df):
            raise ColumnsLengthMismatch

        if not check_columns_names(src_df, tgt_df):
            raise ColumnsNamesMismatch

        src_df, tgt_df = handling_datatypes(src_df, tgt_df)
        src_df1 = src_df.replace(['<NA>'], ' ')
        tgt_df1 = tgt_df.replace(['<NA>'], ' ')

        # Number of rows in src and target
        nrow_src = src_df.shape[0]
        nrow_tgt = tgt_df.shape[0]

        # Making comparison dataframe
        Comparison_df = src_df1.merge(tgt_df1, indicator=True, how='outer')

        # Meaningful naming
        Comparison_df['_merge'] = Comparison_df['_merge'].apply(
            MeaningfulNames)
        Comparison_df.rename(columns={'_merge': 'Record_type'}, inplace=True)

        # Making required dataframes
        Comparison_df = Comparison_df.sort_values(by='Record_type')
        rows_Similar = Comparison_df[Comparison_df['Record_type'] == 'Matched']

        rows_SminusT = Comparison_df[Comparison_df['Record_type'] == 'Source']

        rows_TminusS = Comparison_df[Comparison_df['Record_type'] == 'Target']

        df_header = pd.DataFrame([rows_TminusS.columns.tolist()], columns=rows_SminusT.columns.tolist())

        diff_df = pd.concat([rows_SminusT, df_header, rows_TminusS])

        target_cols.append('Record_type')
        rows_TminusS.columns = target_cols

        # Number of rows
        NoOfRows_Similar = rows_Similar.shape[0]
        NoOfsource_only = rows_SminusT.shape[0]
        NoOftarget_only = rows_TminusS.shape[0]
        Mismatched_records = diff_df.shape[0]

        # Attribute names
        attribute_names = list(src_df.columns)
        logger.info(f"Matched file is : -  {rows_Similar.head()}")
        logger.info(f"Mismatched file is: - {diff_df.head()}")

        logger.info("**************************** Comparison Report *********************************")
        logger.info("Total number of records present in source : %s" % nrow_src)
        logger.info("Total number of records present in target : %s" % nrow_tgt)
        logger.info("Records present in both source and target : %s" %
                    NoOfRows_Similar)
        logger.info("Records only present in Source-Exclude Matching records : %s" %
                    NoOfsource_only)
        logger.info("Records only present in Target-Exclude Matching records : %s" %
                    NoOftarget_only)

        rows_SminusT_r = rows_SminusT[:100]
        rows_TminusS_r = rows_TminusS[:100]
        rows_Similar_r = rows_Similar[:100]

        matched_file, mismatched_file, source_only_file, target_only_file = generate_record_comparison_report(
            rows_Similar,
            diff_df,
            rows_SminusT,
            rows_TminusS,
            reportType)

        record_response.get_instantiated_instance(nrow_src,
                                                  nrow_tgt,
                                                  Mismatched_records,
                                                  NoOfsource_only,
                                                  NoOftarget_only,
                                                  rows_SminusT_r.values.tolist(),
                                                  rows_TminusS_r.values.tolist(),
                                                  NoOfRows_Similar,
                                                  rows_Similar_r.values.tolist(),
                                                  attribute_names,
                                                  matched_file,
                                                  mismatched_file,
                                                  source_only_file,
                                                  target_only_file)

        logger.info("Completed comparison!")
        logger.info("---------@@@@@@@@---------")

        message = "success"
        return message, record_response.get_json_representaion()
    except TypeError:
        message = 'Invalid Report Type'

    except ColumnsLengthMismatch:
        message = "Length of Source and target columns is different"

    except ColumnsNamesMismatch:
        message = "Names of Source and target columns are different"

    except Exception as E:
        logger.error(f"Exception occured during record comparison: - {E}")
        message = "Some Internal error occurred"

    return (message, [])


##############Column comparision

def dataframes_column_based_comparison(src, tgt, source_primary_key, reportType):
    """Function to compare dataframes based on columns"""

    logger.info(f"this came for column based comparison: {src.head()}")
    logger.info(tgt.head())
    response = ColumnResponse()
    mismatch_data = {}
    match_data = {}
    mismatch_data_1000 = {}
    column_wise_mismatch_dict = {}
    total_count = 0
    tgt.columns = src.columns  # Renaming target columns to maintain consistency

    try:
        if not check_columns_length(src, tgt):
            raise ColumnsLengthMismatch

        if not check_columns_names(src, tgt):
            raise ColumnsNamesMismatch

        src, tgt = handling_datatypes(src, tgt)
        no_of_records_source = src.shape[0]
        no_of_records_target = tgt.shape[0]
        src = src.replace(['<NA>'], '')
        tgt = tgt.replace(['<NA>'], '')

        # Setting primary key as index
        source_pkey = source_primary_key

        src.set_index(source_pkey, inplace=True)
        tgt.set_index(source_pkey, inplace=True)

        cols = src.columns

        # Appending proper nomenclature
        src.columns = [col + "_Source" for col in src.columns]
        tgt.columns = [col + "_Target" for col in tgt.columns]

        # Concatenation
        df = pd.concat([src, tgt], axis=1, sort=False)

        # Comparison
        for col in cols:
            df[col + "_Status"] = df[col + "_Source"] == df[col + "_Target"]

        # Renaming
        for col in cols:
            df[col + "_Status"] = df[col +
                                     "_Status"].apply(lambda x: "Matched" if x else "Mismatched")

        # Reordering
        order = []
        for col in cols:
            order += [col + "_Source", col + "_Target", col + "_Status"]
        df = df[order]

        # Reporting dictionary
        for col in cols:
            coldf = df[[col + "_Source", col + "_Target", col + "_Status"]]
            df2 = df[[col + "_Source", col + "_Target", col + "_Status"]]
            coldf = coldf[coldf[col + "_Status"] == "Mismatched"]

            df2 = df2[df2[col + "_Status"] == "Matched"]
            coldf["Primary Key"] = coldf.index
            coldf = coldf[["Primary Key", col +
                           "_Source", col + "_Target", col + "_Status"]]

            df2["Primary Key"] = df2.index
            df2 = df2[["Primary Key", col +
                       "_Source", col + "_Target", col + "_Status"]]

            coldf.fillna("", inplace=True)
            mismatch_data[col] = coldf  # TODO

            match_data[col] = df2

            column_wise_mismatch_dict[col] = list(
                coldf[col + "_Status"]).count("Mismatched")

            total_count += column_wise_mismatch_dict[col]

        logger.info("Comparison completed. Report generation started.")

        matched_csv, mismatched_excel = generate_report(
            mismatch_data, match_data, reportType, df)

        response.get_instantiated_instance(total_count,
                                           column_wise_mismatch_dict,
                                           matched_csv,
                                           mismatched_excel,
                                           no_of_records_source,
                                           no_of_records_target)

        message = 'success'
        return message, response.get_json_representaion()

    except KeyError:
        message = 'Columns/Keys mentioned not found'

    except TypeError:
        message = 'Invalid Report Type'

    except ColumnsLengthMismatch:
        message = "Length of Source and target columns is different"

    except ColumnsNamesMismatch:
        message = "Names of Source and target columns are different"
    except InvalidIndexError:
        message = 'primary Key has duplicate values'

    except Exception as e:
        logger.error("Exception has occurred in Column comparison function ***" + str(e))
        logger.error(traceback.format_exc())
        message = "failure"

    return message, []
