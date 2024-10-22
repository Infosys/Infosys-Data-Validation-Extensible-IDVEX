@REM
@REM Copyright 2024 Infosys Ltd.
@REM
@REM Use of this source code is governed by MIT license that can be found in the LICENSE file or at
@REM
@REM https://opensource.org/licenses/MIT.
@REM

REM Set the path to your FastAPI project
set PROJECT_PATH="<PATH_TO_FOLDER>"

REM Set the path to your FastAPI virtual environment
set ENV_PATH="<PATH_TO_VIRTUAL_ENV>"

set ENV_VAR_FOLDER_PATH="<PATH_TO_ENV_VARIABLES_FILE>"

REM cd to the project path
cd \
cd %PROJECT_PATH%

REM Activate the virtual environment
echo Starting Environment
call "%ENV_PATH%\Scripts\activate"

REM Check if the directory exists
if not exist %ENV_VAR_FOLDER_PATH% (
    echo Directory env_vars does not exist.
    exit /b
)

REM List all files in the env_vars directory

for %%f in (%ENV_VAR_FOLDER_PATH%\*.txt) do (
    echo Processing file: %%f
    REM Read and set variables from each file
    for /f "tokens=1,2 delims==" %%a in (%%f) do (
        set %%a=%%b
    )
)


echo Starting Server
python "%PROJECT_PATH%\main.py"
PAUSE
