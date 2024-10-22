# Compare Open Source Application README

'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''


## Introduction
The Infosys Data Validation IDVeX (eXtensible) Framework is the next gen automation solution that streamlines and accelerates testing of data integration processes by offering a user-friendly, comprehensive and integrated web-based platform. It helps Data Testing professionals to do Data Quality analysis, field level exhaustive data comparison and automates analytics testing.

This is a Pure Play Automation Framework built on Open Source – Python and this code can be extended or customized based on needs.

## Important Documents to Note
- **Swagger Documentation URL**: `/docs` - Fast API Swagger Documentation.
- **Requirements.txt**: - Libraries required by the app.

## API URLs
- **Get Token For Authentication**: `get_token/`
- **File to File Comparison**: `file_to_file_comparision/` 
- **File to Database Comparison**: `file_to_db_comparision/`.
- **Database to Database Comparison**: `db_to_db_comparision/`
- **PDF to Text Conversion**: `pdf_to_text_conversion/`
- **Data Profiling**: `data_profile/`
- **Data Quality Checks**: `data_quality_checks/`
- **Data Generator**: `/generator/generate_dataset/`
- **File Conversion to CSV**: `convert_to_csv/`
- **Cleanup Temporary files**: `delete_all_tempfiles/`

## Setup
1. Clone this repository to your local machine.
2. Navigate to the project directory.
3. Change the variable values for PROJECT_PATH and ENV_PATH with the project path and the path to the virtual environment(p.s in case the environment doesnot exist it will create a new one at the given path)
4. Run the setup script using the provided batch file:
   ```bash
   setup.bat

This script will install the necessary dependencies and set up the environment.

## Running the Server and UI
1. To start the API server Change the variable values for PROJECT_PATH and ENV_PATH with the project path and the path to the virtual environment(p.s in case the environment doesnot exist run the setup file first)
2. Create an env_vars file having all environment variables supplied
3Execute the following command:
    ```bash
    startup.bat
   
The server will run on http://localhost:8889 and ui will run at http://localhost:9998 .

## Running the API
1. Open the swagger document and get the api url you want to run
2. Using the authentication credentials generate the token using the token api
3. Use the authentication header as bearer token and hit the api using the generated token. 


## Closure Instructions
To gracefully shut down the server, follow these steps:

1. Press Ctrl+C once to initiate the shutdown process.
2. Wait for a moment.
3. Press Ctrl+C again to confirm and exit.

## IMP NOTE
1. This version of code requires python version **3.11** and above. Any change in python versions may lead to resolution of library dependency issues in case the given libraries are not supported in the version of python.
2. In order to go for a version of python 3.7 replace the library oracledb with cx_Oracle and resolve its dependencies.
3. Supported File types for comparison - **'txt', 'parquet', 'json', 'xml','avro','csv'**
4. Supported File types for conversion - **'parquet', 'json', 'xml', 'pdf', "avro"**
5. Supported File types for data generator download - **"parquet", "json", "xml", "pdf", "avro","txt","pdf","bin","xls","html","csv"**
6. Supported Databases - **"Mysql","Drill","MongoDB","MsSql","Postgresql","Oracle","Linux","Hive","Hana".**
7. **File Conversion to CSV** feature will require java installed on the system. Install Latest version of JDK and set the path in system's environment variables.
