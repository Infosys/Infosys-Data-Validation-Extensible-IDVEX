'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import datetime
import json
import os.path
import shutil
from typing import Any, Dict

from fastapi import APIRouter, Depends, Body
from fastapi import Request, Query
from fastapi.responses import FileResponse
from starlette import status as return_status
from starlette.responses import JSONResponse

from data_generator.configs import config
from data_generator.configs.output_format import INP_FORMAT
from data_generator.generator.generate import Generate
from data_generator.process.process import Process
from utils.ServerLogs import logger
from utils.login import verify_token, TokenData

data_generator_router = APIRouter()


@data_generator_router.get("/test_server/", response_class=JSONResponse)
def hello(token_data: TokenData = Depends(verify_token)):
    """Function to test the server

    Args:

    Returns:
        username : username to print

    Raises:
        None
    """
    content = {"response": f"Hello {token_data.username}"}
    return JSONResponse(content, status_code=return_status.HTTP_200_OK)


@data_generator_router.post("/generate_dataset/", response_class=JSONResponse)
async def generate_dataset(request: Request, token_data: TokenData = Depends(verify_token),
                           body: Dict[str, Any] = Body(..., examples=[INP_FORMAT])):
    """Function to check the dataset

    Args:
        request: Input request object

    Returns:
        Json of output

    Raises:
        Exception on Error
    """
    payload = await request.body()

    logger.info(f'Logged IN User - {token_data.username}')

    request_data = json.loads(payload)

    try:
        method = request_data.get('method', "faker")  # ["faker", "random"]
        format_to_save = request_data.get('format_to_save', "csv")
        n = int(request_data.get("no_of_records", 10))
        json_input = request_data.get("data", {})

        gen = Generate()
        if method.lower() == "faker":
            output_json = gen.generate_data_faker(json_input, n)
        else:
            output_json = gen.generate_data_random(json_input, n)

        process_obj = Process()

        extension = process_obj.get_file_extension(format_to_save)
        output_path = os.path.join(os.path.join(config.BASE_DIR, "output"),
                                   f'{datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}{extension}')
        os.makedirs(os.path.join(config.BASE_DIR, "output"), exist_ok=True)

        output_path, sample = process_obj.save_json(output_json=output_json, output_path=output_path,
                                                    file_format=format_to_save)

        return JSONResponse({'output_path': output_path, 'error': 'NA', 'sample_output': sample.to_dict()},
                            status_code=return_status.HTTP_200_OK)

    except Exception as e:

        output_json = {'output_path': "", 'error': f'Exception - {e}'}

        logger.critical(f'Exception - {e}', stack_info=True)
        return JSONResponse(output_json, status_code=return_status.HTTP_500_INTERNAL_SERVER_ERROR)


# Define a route for downloading a file
@data_generator_router.get("/download/")
def download_file(folder_path: str = Query(..., description='Path of file to download'),
                  token_data: TokenData = Depends(verify_token)):
    """Function to download a file

    Args:
        folder_path: Input file to download

    Returns:
        Json of output

    Raises:
        Exception on Error
    """

    output_path = None
    logger.info(f'Logged IN User - {token_data.username}')

    try:

        if folder_path == '' or folder_path is None:
            return JSONResponse(content={"error": f"Path not found"},
                                status_code=return_status.HTTP_404_NOT_FOUND)

        # Check if the folder exists
        if (folder_path.strip('\\') == config.BASE_DIR) or not os.path.exists(folder_path):
            return JSONResponse(content={"error": f"Path not found"},
                                status_code=return_status.HTTP_404_NOT_FOUND)

        if os.path.isdir(folder_path):

            fname = f'{os.path.basename(os.path.dirname(folder_path))}_{os.path.basename(folder_path)}'
            zip_path = os.path.join(os.path.join(os.path.dirname(folder_path), 'tmp'), fname)

            output_path = shutil.make_archive(zip_path, 'zip', folder_path)
            filename = f"{fname}.zip"

        else:
            fname = os.path.basename(folder_path)
            output_path = folder_path
            filename = fname

        # Use FastAPI's FileResponse to send the file for download
        response = FileResponse(output_path, headers={"Content-Disposition": f"attachment; filename={filename}"})

        return response

    except Exception as e:
        return JSONResponse(content={"error": str(e)},
                            status_code=return_status.HTTP_500_INTERNAL_SERVER_ERROR)

    finally:
        shutil.rmtree(output_path, ignore_errors=True)


@data_generator_router.delete("/cleanup/")
def delete_old_folders(token_data: TokenData = Depends(verify_token)):
    """Function to delete old images and input/output data
    Args:

    Returns:
        Json of output

    Raises:
        None
    """
    dirs = [os.path.join(os.path.join(config.BASE_DIR, "output")), ]
    try:
        logger.info(f'Logged IN User - {token_data.username}')
        # Get the current date and time
        current_time = datetime.datetime.now()

        for folder_path in dirs:
            logger.info(f'.................DELETING FOLDER - {os.path.basename(folder_path)} > 30 days............')

            # Iterate through the files and subdirectories in the folder
            for item in os.listdir(folder_path):

                item_path = os.path.join(folder_path, item)

                # Check if it's a directory
                if os.path.isdir(item_path):
                    try:
                        # Get the last modification time of the directory
                        modification_time = datetime.datetime.fromtimestamp(os.path.getmtime(item_path))

                        # Calculate the difference in days
                        days_difference = (current_time - modification_time).days

                        # Check if the directory is older than 1 day
                        if days_difference > 1:
                            # Delete the directory and its contents
                            shutil.rmtree(item_path)

                            logger.info(f'Deleted: {item}')

                    except Exception as e:
                        logger.critical(f"Error: {e}", stack_info=True)

                logger.info(f'Path Visited - {item}')

        logger.info('.................DELETING LOGS >30 days............')

        folder_path = os.path.join(os.path.join(os.path.dirname(config.BASE_DIR), "logs"))
        # Iterate through the files and subdirectories in the folder

        for item in os.listdir(folder_path):

            item_path = os.path.join(folder_path, item)

            try:
                # Get the last modification time of the directory
                modification_time = datetime.datetime.fromtimestamp(os.path.getmtime(item_path))

                # Calculate the difference in days
                days_difference = (current_time - modification_time).days

                # Check if the directory is older than 1 day
                if days_difference > 30:
                    # Delete the directory and its contents
                    os.remove(item_path)

                logger.info(f'Deleted: {item}')

            except Exception as e:
                logger.critical(f"Error: {e}", stack_info=True)

            logger.info(f'Path Visited - {item}')

    except Exception as e:
        return JSONResponse(content={"error": str(e)},
                            status_code=return_status.HTTP_500_INTERNAL_SERVER_ERROR)

    return JSONResponse(content={"status": "Completed"},
                        status_code=return_status.HTTP_200_OK)
