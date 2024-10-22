@REM
@REM Copyright 2024 Infosys Ltd.
@REM
@REM Use of this source code is governed by MIT license that can be found in the LICENSE file or at
@REM
@REM https://opensource.org/licenses/MIT.
@REM

@echo off

@REM REM Set the path to your FastAPI project
set PROJECT_PATH="path_to_project"

@REM REM Set the path to your FastAPI virtual environment
set ENV_PATH="path_to_environment"


cd \
cd %PROJECT_PATH%
echo Setting Path


REM Create a virtual environment if it doesn't exist
if not exist %ENV_PATH% (
    echo Creating virtual environment...
    python -m venv %ENV_PATH%
)

REM Activate the virtual environment
echo Starting Environment
call "%ENV_PATH%\Scripts\activate"
echo %CD%


REM Install requirements
echo Installing requirements...
pip install -r "%PROJECT_PATH%\requirements.txt"

REM Deactivate the virtual environment
deactivate

PAUSE