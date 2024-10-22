'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import json
import os
import shutil
import time
from multiprocessing import freeze_support, Process
from typing import Annotated

import requests
import uvicorn
from fastapi import FastAPI, Depends
from fastapi import Request
from fastapi.security import HTTPBasicCredentials, HTTPBasic
from keycloak.keycloak_openid import KeycloakOpenID
from starlette import status
from starlette import status as return_status
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

from configs import config
from routers.compare_routers import compare_router
from routers.data_generator_routers import data_generator_router
from utils.ServerLogs import logger
from utils.login import TokenData, verify_token

app = FastAPI(version="0.3.0", docs_url="/docs", redoc_url="/redoc", openapi_url="/openapi.json")

origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Keycloak OpenID client
keycloak_openid = KeycloakOpenID(server_url=config.KEYCLOAK_SERVER_URL,
                                 client_id=config.KEYCLOAK_CLIENT_ID_TOKEN,
                                 realm_name=config.KEYCLOAK_REALM_NAME)

security = HTTPBasic()

# import routers
app.include_router(data_generator_router, prefix='/generator', tags=['synthetic_data_generator'])
app.include_router(compare_router, prefix='', tags=['compare'])


@app.post("/get_token")
def get_token(credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    """Function to get the bearer token for authorization"""
    # Step 1: Get Token from Keycloak
    response = requests.post(
        f"{config.KEYCLOAK_SERVER_URL}realms/{config.KEYCLOAK_REALM_NAME}/protocol/openid-connect/token",
        data={
            "client_id": config.KEYCLOAK_CLIENT_ID_TOKEN,
            "username": credentials.username,
            "password": credentials.password,
            "grant_type": "password"
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"})

    token_data = response.json()

    if 'access_token' in token_data:
        access_token = token_data["access_token"]
        error = ''
        error_description = ''
    else:
        access_token = ''

        error = token_data['error'] if 'error' in token_data else token_data['detail']
        error_description = token_data['error_description'] if 'error' in token_data else ''

    return JSONResponse({'token': access_token, 'error': error, 'error_description': error_description},
                        status_code=return_status.HTTP_200_OK)


@app.get("/test_server/")
def hello(token_data: TokenData = Depends(verify_token)):
    """Test Function"""
    return f"Hello {token_data.username}"


@app.post("/delete_all_tempfiles/")
async def delete_all_tempfiles(token_data: TokenData = Depends(verify_token)):
    """Main Function to delete temp files"""
    logger.info(f'Logged IN User - {token_data.username}')
    prj_dir = os.path.dirname(os.path.realpath(__file__))
    temp_folder = os.path.join(prj_dir, "tempfiles")
    cnt = 0
    if os.path.exists(temp_folder):
        all_files = [os.path.join(temp_folder, x) for x in os.listdir(temp_folder)]
        cnt = len(all_files)
        for file in all_files:
            os.remove(file)
    content = f"Successfully removed all {cnt} files."
    return JSONResponse(content, status_code=status.HTTP_200_OK)


@app.post("/clear_reports/")
async def clear_reports(request: Request, token_data: TokenData = Depends(verify_token)):
    """Main Function to delete temp files"""
    logger.info(f'Logged IN User - {token_data.username}')
    prj_dir = os.path.dirname(os.path.realpath(__file__))
    payload = await request.body()

    request_data = json.loads(payload)
    folder_name = request_data.get('folder_name', None)
    time.sleep(10)
    if folder_name:
        temp_folder = os.path.join(os.path.join(prj_dir, "Output"), folder_name)
        cnt = 0
        if os.path.exists(temp_folder):
            all_files = [os.path.join(temp_folder, x) for x in os.listdir(temp_folder)]
            cnt = len(all_files)
            for file in all_files:
                os.remove(file)

        if os.path.exists(temp_folder) and not os.listdir(temp_folder):
            shutil.rmtree(temp_folder)

        content = f"Successfully removed all {cnt} files."
    else:
        content = {'No files to remove - exiting'}
    return JSONResponse(content, status_code=status.HTTP_200_OK)


@app.get("/login")
def login():
    """login function"""
    authorization_url = keycloak_openid.auth_url(redirect_uri=config.REDIRECT_URI)
    return JSONResponse({"login_url": authorization_url}, status_code=200)


@app.get("/callback")
def callback(request: Request):
    """callback function for login"""
    code = request.query_params.get('code')
    token = keycloak_openid.token(code=code, grant_type='authorization_code', redirect_uri=config.REDIRECT_URI)
    userinfo = keycloak_openid.userinfo(token['access_token'])
    return JSONResponse({"token": token, "userinfo": userinfo})


def start_fastapi():
    print('Starting Backend')
    # Start FastAPI with 3 workers
    uvicorn.run(f'main:app', host="0.0.0.0", port=8889, workers=3)


def start_streamlit():
    print('Starting Frontend')
    time.sleep(10)
    os.system("streamlit run ui/ui.py --server.port 9998")


if __name__ == "__main__":
    freeze_support()
    print('Starting MAIN')

    # Create separate processes for FastAPI and Streamlit
    fastapi_process = Process(target=start_fastapi)
    streamlit_process = Process(target=start_streamlit)

    # Start both processes
    fastapi_process.start()
    streamlit_process.start()

    # Wait for both processes to complete
    fastapi_process.join()
    streamlit_process.join()
