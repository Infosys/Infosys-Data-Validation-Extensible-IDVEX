'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import json
import os
import pathlib
from typing import Dict, Any

from fastapi import Request, Body, Depends, APIRouter
from starlette import status
from starlette.responses import JSONResponse, FileResponse

from comparator.db2dbcomparison_module import db_to_db_comparison as db_to_db
from comparator.f2dbcomparison_module import file_to_db_comparison_new as file_to_db
from comparator.file_to_file_comparision import file_to_file_comparison as file_to_file
from comparision_checks.DataQualityChecks import perform_quality_checks_csv
from configs.output_format import FILE_TO_FILE, DB_TO_DB, FILE_TO_DB, DATA_PATH_INP, CONVERT_TO_CSV
from reports.Dataprofling import create_data_profile
from utils.DataConverter import convert_to_csv
from utils.InfyPDFConverter import validate_pdf, convert_pdf_to_format
from utils.ServerLogs import logger
from utils.exceptions import DataFrameReadError
from utils.get_columns import read_file, get_columns_from_file_2_database, get_columns_from_database_2_database
from utils.login import verify_token, TokenData

compare_router = APIRouter()


@compare_router.post("/file_to_file_comparision/", response_class=JSONResponse)
async def file_to_file_compare(request: Request, token_data: TokenData = Depends(verify_token),
                               body: Dict[str, Any] = Body(..., examples=[FILE_TO_FILE])
                               ):
    """Main function for file to file comparison"""
    try:
        logger.info(f'Logged IN User - {token_data.username}')
        payload = await request.body()
        request_data = json.loads(payload)["data"]

        message, response, time_elapsed = file_to_file(request_data)

        content = {"message": message, "time": time_elapsed, "response": response}
        if response == []:
            content = {"Error": message, "time": time_elapsed, "response": response}
            return JSONResponse(content, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return JSONResponse(content, status_code=status.HTTP_200_OK)

    except Exception as error:
        logger.info("Error", error)
        return JSONResponse({"Error": str(error)}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@compare_router.post("/db_to_db_comparision/", response_class=JSONResponse)
async def database_to_database(request: Request, token_data: TokenData = Depends(verify_token),
                               body: Dict[str, Any] = Body(..., examples=[DB_TO_DB])):
    """Main function for DB to DB comparison"""
    try:
        logger.info(f'Logged IN User - {token_data.username}')
        payload = await request.body()
        request_data = json.loads(payload)["data"]

        message, response, time_elapsed = db_to_db(request_data)

        content = {"message": message, "time": time_elapsed, "response": response}
        if response == []:
            content = {"Error": message, "time": time_elapsed, "response": response}
            return JSONResponse(content, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return JSONResponse(content, status_code=status.HTTP_200_OK)

    except DataFrameReadError:
        return JSONResponse({"Error": "Query Could Not be executed"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    except Exception as error:
        logger.error(f"Error - {error}")
        return JSONResponse({"Error": str(error)}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@compare_router.post("/file_to_db_comparision/", response_class=JSONResponse)
async def file_to_database(request: Request, token_data: TokenData = Depends(verify_token),
                           body: Dict[str, Any] = Body(..., examples=[FILE_TO_DB])):
    """Main function for file to DB comparison"""
    try:
        logger.info(f'Logged IN User - {token_data.username}')
        payload = await request.body()
        request_data = json.loads(payload)["data"]

        message, response, time_elapsed = file_to_db(request_data)

        content = {"message": message, "time": time_elapsed, "response": response}
        if response == []:
            content = {"Error": message, "time": time_elapsed, "response": response}
            return JSONResponse(content, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return JSONResponse(content, status_code=status.HTTP_200_OK)

    except Exception as error:
        logger.error(f"Error - {error}")
        return JSONResponse({"Error": str(error)}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@compare_router.post("/pdf_to_text_conversion/")
async def pdf_to_text(request: Request, token_data: TokenData = Depends(verify_token),
                      body: Dict[str, Any] = Body(..., examples=[DATA_PATH_INP])):
    """Main function to convert pdf to text"""
    logger.info(f'Logged IN User - {token_data.username}')
    payload = await request.body()
    request_data = json.loads(payload)
    data_filepath = request_data['data_filepath']
    ext = pathlib.Path(data_filepath).suffix

    if not os.path.exists(data_filepath):
        return JSONResponse({"Error": f"File Not Found:{data_filepath}"}, status_code=status.HTTP_404_NOT_FOUND)
    elif ext.lower() != ".pdf":
        return JSONResponse({"Error": f"Invalid File Format:{ext}, Only .pdf files are accepted."},
                            status_code=status.HTTP_406_NOT_ACCEPTABLE)
    else:
        # validate pdf
        res, msg = validate_pdf(data_filepath)
        if res == True:
            # conver pdf to text
            converted_data_filepath, msg = convert_pdf_to_format(data_filepath, "text")
            if converted_data_filepath == None:
                return JSONResponse({"Error": str(msg)}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
            else:
                filename = os.path.basename(converted_data_filepath)
                return FileResponse(converted_data_filepath, media_type='application/octet-stream', filename=filename)
        else:
            return JSONResponse({"Error": str(msg)}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@compare_router.post("/data_profile/")
async def data_profile_report(request: Request, token_data: TokenData = Depends(verify_token),
                              body: Dict[str, Any] = Body(..., examples=[DATA_PATH_INP])):
    """Main function For data profiler"""
    logger.info(f'Logged IN User - {token_data.username}')
    payload = await request.body()
    request_data = json.loads(payload)
    data_filepath = request_data['data_filepath']
    ext = pathlib.Path(data_filepath).suffix

    if not os.path.exists(data_filepath):
        return JSONResponse({"Error": f"File Not Found:{data_filepath}"}, status_code=status.HTTP_404_NOT_FOUND)
    elif ext.lower() != ".csv":
        return JSONResponse({"Error": f"Invalid File Format:{ext}, Only .csv files are accepted."},
                            status_code=status.HTTP_406_NOT_ACCEPTABLE)
    else:
        res = create_data_profile(data_filepath)
        if res == None:
            return JSONResponse({"Error": "internal server error"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            filename = os.path.basename(res)
            return FileResponse(res, media_type='application/octet-stream', filename=filename)


@compare_router.post("/data_quality_checks/")
async def data_quality_checks(request: Request, token_data: TokenData = Depends(verify_token),
                              body: Dict[str, Any] = Body(..., examples=[DATA_PATH_INP])):
    """Main function to perform data Quality Checks"""
    logger.info(f'Logged IN User - {token_data.username}')
    payload = await request.body()
    request_data = json.loads(payload)
    data_filepath = request_data['data_filepath']
    ext = pathlib.Path(data_filepath).suffix

    if not os.path.exists(data_filepath):
        return JSONResponse({"Error": f"File Not Found:{data_filepath}"}, status_code=status.HTTP_404_NOT_FOUND)
    elif ext.lower() != ".csv":
        return JSONResponse({"Error": f"Invalid File Format:{ext}, Only .csv format are accepted."},
                            status_code=status.HTTP_406_NOT_ACCEPTABLE)
    else:
        content = perform_quality_checks_csv(data_filepath)
        return JSONResponse(content)


@compare_router.post("/convert_to_csv/")
async def data_convert_to_csv(request: Request, token_data: TokenData = Depends(verify_token),
                              body: Dict[str, Any] = Body(..., examples=[CONVERT_TO_CSV])):
    """Main Function to convert file to csv"""
    logger.info(f'Logged IN User - {token_data.username}')
    payload = await request.body()
    request_data = json.loads(payload)
    data_filepath = request_data['data_filepath']
    format = request_data['format']
    ext = pathlib.Path(data_filepath).suffix

    if not os.path.exists(data_filepath):
        return JSONResponse({"Error": f"File Not Found:{data_filepath}"}, status_code=status.HTTP_404_NOT_FOUND)
    elif ext[1:].lower() != format:
        return JSONResponse({
            "Error": f"File Format doesn't match with selected format. file format : {ext}, selected format : {format}"},
            status_code=status.HTTP_406_NOT_ACCEPTABLE)
    else:
        res = convert_to_csv(data_filepath, format)
        if res == "":
            return JSONResponse({"Error": "Error while processing the data"},
                                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            filename = os.path.basename(res)
            return FileResponse(res, media_type="text/csv", filename=filename)


@compare_router.post("/get_columns/")
async def get_columns(request: Request, token_data: TokenData = Depends(verify_token)):
    """Main function to get columns from a file"""
    logger.info(f'Logged IN User - {token_data.username}')
    try:
        payload = await request.body()
        request_data = json.loads(payload)
        type = request_data.get('type', 'file')

        if type == 'file':
            data_filepath = request_data['filepath']
            extension = request_data['type_of_file']
            columns, sample = read_file(data_filepath, extension)
            sample = sample.to_dict()
        elif type == 'f2db':
            connection_data = request_data['db_details']
            columns, sample = get_columns_from_file_2_database(connection_data)
            sample = sample.to_dict()
        elif type == 'db2db':
            connection_data = request_data['db_details']
            columns_source, columns_target, source_df, target_df = get_columns_from_database_2_database(connection_data)
            if columns_source and columns_target:
                columns = [columns_source, columns_target]
                sample = [source_df.to_dict(), target_df.to_dict()]
            else:
                columns = None
                sample = []
        else:
            raise Exception('Invalid input type')

        if columns:
            return JSONResponse({'cols': columns, 'data': sample, 'Error': None}, status_code=status.HTTP_200_OK)
        else:
            return JSONResponse({'cols': None, 'data': {}, 'Error': 'Unable to get columns'},
                                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    except Exception as e:
        return JSONResponse({'cols': None, 'data': {}, 'Error': str(e)},
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
