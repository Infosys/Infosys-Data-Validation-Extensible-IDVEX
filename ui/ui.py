'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import json
import os
import shutil
import tempfile
import time
import zipfile

import pandas as pd
import requests
import streamlit as st

BACKEND_URL = os.getenv('BACKEND_URL')
REDIRECT_URI = os.getenv('REDIRECT_URI')

api_endpoints = {
    "File to File Comparison": f"{BACKEND_URL}/file_to_file_comparision",
    "File to Database Comparison": f"{BACKEND_URL}/file_to_db_comparision",
    "Database to Database Comparison": f"{BACKEND_URL}/db_to_db_comparision",
    "PDF to Text Conversion": f"{BACKEND_URL}/pdf_to_text_conversion",
    "Data Profiling": f"{BACKEND_URL}/data_profile",
    "Data Quality Checks": f"{BACKEND_URL}/data_quality_checks",
    "File Conversion to CSV": f"{BACKEND_URL}/convert_to_csv",
    "Data Generator": f"{BACKEND_URL}/generator"
}
get_cols_url = f"{BACKEND_URL}/get_columns"

# Create a temporary directory
temp_dir = tempfile.mkdtemp()


# Cleanup temp directory on session logout
def cleanup_temp_directory():
    """Function to cleanup directory"""
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    if os.path.exists("Output\\temp.zip"):
        os.remove("Output\\temp.zip")


# Add session state to cleanup temp directory
st.session_state.on_session_exit = cleanup_temp_directory


# Function to handle file upload and get file path
def upload_file(file):
    """Function to upload file"""
    if file is not None:
        file_path = os.path.join(temp_dir, file.name)
        with open(file_path, "wb") as f:
            f.write(file.getbuffer())
        return file_path
    return None


class Function_DataGenerator:
    """Class to display data generator ui"""

    def __init__(self):
        """Init function"""
        if 'data' in st.session_state and len(st.session_state.data['keys']) > 0:
            self.record_no = len(st.session_state.data['keys']) - 1
        else:
            self.record_no = 0

    def display_records(self):
        """Function to display records"""
        if 'data' not in st.session_state or len(st.session_state.data['keys']) <= 0:
            st.session_state.disabled = True
            st.write('No Records to display')

        else:
            st.session_state.disabled = False
            df = pd.DataFrame(st.session_state.data)
            # Convert range_values to string for display
            df['range_values'] = df['range_values'].apply(lambda x: str(x))
            st.table(df)

    def add_new_record(self):
        """Function to add new record"""
        if 'data' not in st.session_state:
            st.session_state.data = {'keys': [''], 'type_indices': [''], 'range_indices': [''], 'range_values': ['']}

        else:
            st.session_state.data['keys'].append('')
            st.session_state.data['type_indices'].append('')
            st.session_state.data['range_indices'].append('')
            st.session_state.data['range_values'].append('')

        return len(st.session_state.data['keys']) - 1

    def remove_last_record(self):
        """Function to remove record"""

        st.session_state.data['keys'].pop()
        st.session_state.data['type_indices'].pop()
        st.session_state.data['range_indices'].pop()
        st.session_state.data['range_values'].pop()

        return len(st.session_state.data['keys']) - 1


class Function:
    """class to display ui"""

    def __init__(self):
        """Init function"""
        if 'data' in st.session_state and len(st.session_state.data['sourcekeys']) > 0:
            self.record_no = len(st.session_state.data['sourcekeys']) - 1
        else:
            self.record_no = 0

    def display_records(self):
        """Function to display records"""
        if 'data' not in st.session_state or len(st.session_state.data['sourcekeys']) <= 0:
            st.session_state.disabled = True
            st.write('No Records to display')

        else:
            st.session_state.disabled = False
            df = pd.DataFrame(st.session_state.data)
            st.table(df)

    def add_new_record(self):
        """Function to add new record"""
        if 'data' not in st.session_state:
            st.session_state.data = {'sourcekeys': [''], 'targetkeys': ['']}

        else:
            st.session_state.data['sourcekeys'].append('')
            st.session_state.data['targetkeys'].append('')

        return len(st.session_state.data['sourcekeys']) - 1

    def remove_last_record(self):
        """Function to remove last record"""
        st.session_state.data['sourcekeys'].pop()
        st.session_state.data['targetkeys'].pop()

        return len(st.session_state.data['sourcekeys']) - 1


def file_to_file_comparison_form():
    """Function for file to file comparison"""
    st.subheader("File to File Comparison")

    source_file = st.file_uploader("Upload Source File", key='sf2ffile')
    sourceFilePath = upload_file(source_file)
    sourceFileType = st.selectbox('Type of Value for the parameter',
                                  options=['txt', 'parquet', 'json', 'xml', "avro", "csv"],
                                  key=f'box_s')

    sourceDelimiter = st.text_input("Source Delimiter", ",")
    target_file = st.file_uploader("Upload Target File", key='tf2ffile')
    targetFilePath = upload_file(target_file)
    targetFileType = st.selectbox('Type of Value for the parameter',
                                  options=['txt', 'parquet', 'json', 'xml', "avro", "csv"],
                                  key=f'box_t')
    targetDelimiter = st.text_input("Target Delimiter", ",")

    # if st.button('Fetch Columns',disabled=st.session_state.disabled):
    if sourceFilePath and targetFilePath:
        with st.spinner('Fetching Columns'):
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {st.session_state['token'].get('access_token')}"
            }

            payload = {'filepath': sourceFilePath, 'type_of_file': sourceFileType, 'type': 'file'}
            payload2 = {'filepath': targetFilePath, 'type_of_file': targetFileType, 'type': 'file'}
            response = requests.request("POST", get_cols_url, json=payload, headers=headers)
            response2 = requests.request("POST", get_cols_url, json=payload2, headers=headers)

            resp = response.json()
            resp2 = response2.json()

            if response.status_code == 200 and response2.status_code == 200:

                if ('cols' in resp and resp['cols']) and ('cols' in resp2 and resp2['cols']):
                    dict_df = {'Source': resp['cols'], 'Target': resp2['cols']}
                else:
                    dict_df = {'Source': str(resp['Error']), 'Target': str(resp2['Error'])}

                st.write('Columns')
                st.dataframe(dict_df)
                st.write('Source SAMPLE DATA')
                st.dataframe(resp['data'] if 'data' in resp else {})
                st.write('Target SAMPLE DATA')
                st.dataframe(resp2['data'] if 'data' in resp2 else {})

                columns = resp['cols'] if resp['cols'] else []
                columns2 = resp2['cols'] if resp2['cols'] else []

                if columns and columns2:
                    record_or_column = st.radio('Method', ['record', 'column'])

                    if record_or_column == 'column':
                        reporttype = "summary"

                        pkey = st.selectbox("Source Primary Key", options=columns)
                        if not pkey:
                            st.write('Primary key cannot be blank')

                    else:
                        reporttype = st.selectbox('Enter Type of report',
                                                  options=['summary', "detailed mismatch", "detailed match"],
                                                  key=f'type_rep', placeholder='summary')

                        pkey = ''

                    func_obj = Function()
                    st.subheader('Column Mappings')

                    # Function to display records
                    func_obj.display_records()
                    # Data keys, types of values, and ranges of values

                    i = func_obj.record_no

                    if i >= 0:
                        # Data key
                        source_key = st.selectbox('Select Source Column',
                                                  placeholder='Select Source Column',
                                                  options=columns,
                                                  key=f'sfkey_{i}')

                        target_key = st.selectbox('Select Target Column',
                                                  placeholder='Select Target Column',
                                                  options=columns2,
                                                  key=f'tfkey_{i}')

                        add_records = st.button(label='Add New Record')

                        remove_record = st.button(label='Remove Last Record', disabled=st.session_state.disabled)

                        # Button to add new record
                        if add_records:

                            if (source_key != '') and (target_key != ''):
                                i = func_obj.add_new_record()
                                if (source_key in st.session_state.data['sourcekeys']) or target_key in \
                                        st.session_state.data['targetkeys']:
                                    i = func_obj.remove_last_record()
                                    func_obj.record_no = i if i > 0 else 0
                                    st.write('Key already exists - ignored addition')
                                else:
                                    st.session_state.data['sourcekeys'][i] = source_key
                                    st.session_state.data['targetkeys'][i] = target_key
                                    func_obj.record_no = i

                                st.rerun()

                            else:
                                st.write('please add both source and target')

                        # Button to remove last record
                        if remove_record:
                            i = func_obj.remove_last_record()
                            func_obj.record_no = i if i > 0 else 0
                            st.rerun()

                    if st.button("Submit", disabled=st.session_state.disabled):
                        payload = {
                            "data": {
                                "sourceFilePath": sourceFilePath,
                                "sourceFileType": sourceFileType,
                                "sourceDelimiter": sourceDelimiter,
                                "sourceTag": "",
                                "sourceColumns": "",
                                "targetFilePath": targetFilePath,
                                "targetFileType": targetFileType,
                                "targetTag": "",
                                "targetColumns": "",
                                "targetDelimiter": targetDelimiter,
                                "sourceStorageAlias": "",
                                "targetStorageAlias": "",
                                "primaryKey": pkey,
                                "reportType": reporttype,
                                "columnMapping": [
                                    {
                                        "sourceColumn": st.session_state.data['sourcekeys'][key],
                                        "targetColumn": st.session_state.data['targetkeys'][key]
                                    }
                                    for key in range(len(st.session_state.data['sourcekeys']))

                                ],
                                "sourceDatabaseAlias": "",
                                "sourceTableName": "",
                                "sourceTableQuery": "",
                                "sourceDatabase": "",
                                "targetDatabaseAlias": "",
                                "targetTableName": "",
                                "targetTableQuery": "",
                                "targetDatabase": "",
                                "record_or_column": record_or_column,
                                "comparisonType": "file_to_file",
                                "testCaseOpType": "file_to_file",
                                "isRemotePath": False
                            }
                        }

                        headers = {
                            'Content-Type': 'application/json',
                            'Authorization': f"Bearer {st.session_state['token'].get('access_token')}"
                        }

                        with st.spinner('Comparing'):
                            # Send a POST request to the FastAPI server
                            response = requests.request("POST", api_endpoints["File to File Comparison"],
                                                        headers=headers,
                                                        json=payload)

                        if response.status_code == 200:

                            resp = response.json()['response']
                            output_keys = ['matched_file', 'mismatch_file', 'source_only_file', 'target_only_file']
                            files = [resp[file] for file in output_keys if file in resp]

                            filtered_response = {key: resp[key] for key in resp if key not in output_keys}

                            st.write('Comparison Completed Successfully!')

                            # Create a zip file containing only the specified files
                            with zipfile.ZipFile("Output\\temp.zip", "w", compression=zipfile.ZIP_DEFLATED) as zf:
                                for file_path in files:
                                    if file_path is not None:
                                        base_name = os.path.basename(file_path)
                                        zf.write(file_path, base_name)

                            download_data = filtered_response
                            st.download_button(
                                label="Download JSON",
                                data=json.dumps(download_data).encode("utf-8"),
                                file_name='data.json',
                                mime='application/json'
                            )

                            # Display a download button
                            with open("Output\\temp.zip", "rb") as fp:
                                st.download_button(
                                    label="Download ZIP",
                                    data=fp,
                                    file_name="temp.zip",
                                    mime="application/zip"
                                )

                            os.remove("Output\\temp.zip")

                            for file_path in files:
                                time.sleep(100)
                                response = requests.post(f"{BACKEND_URL}/clear_reports", headers=headers,
                                                         json={'folder_name': os.path.dirname(file_path)})

                        else:
                            resp = response.json()
                            error = resp.get("Error") if "Error" in resp else resp.get('detail')
                            st.error(f'Request Failed - {error}')

                else:
                    st.write('Unable to fetch Columns please try again later')

            else:

                st.write(
                    f'Unable to fetch Source/Target Columns please try again later - Source Error: {resp.get("Error", "") if "Error" in resp else resp.get("detail", "")} Target Error: {resp2.get("Error", "") if "Error" in resp2 else resp2.get("detail", "")}')
    else:
        st.write('Upload Files to proceed')
        if 'data' in st.session_state:
            del st.session_state['data']


def file_to_db_comparison_form():
    """Function for file to db comparison"""
    st.subheader("File to Database Comparison")

    source_file = st.file_uploader("Upload Source File", key='sf2db')
    sourceFilePath = upload_file(source_file)
    sourceFileType = st.selectbox('Type of Value for the parameter',
                                  options=['txt', 'parquet', 'json', 'xml', "avro", "csv"],
                                  key=f'box_s')
    sourceDelimiter = st.text_input("Source Delimiter", ",")
    targetDatabaseAlias = st.text_input("Target Database Alias", placeholder="postgres")
    targetTableName = st.text_input("Target Table Name", key='tn')
    targetTableQuery = st.text_input("Target Table Query", key='tq',
                                     help='Enclose table names having capital letters with a " ')
    targetDatabase = st.text_input("Target Database", placeholder="Name of the Database", key='td')
    databaseType = st.selectbox('Type of Value for the parameter',
                                options=["Mysql", "MsSql", "Oracle", "MongoDB", "Postgresql", "Drill",
                                         "Hana"],
                                key=f'box_d')

    hostname_or_url = st.text_input("Hostname or URL")
    portNumber = st.text_input("Port Number")
    userName = st.text_input("User Name")
    password = st.text_input("Password", type="password")

    if (sourceFilePath and targetDatabaseAlias and targetTableName
            and targetTableQuery and targetDatabase and databaseType and hostname_or_url
            and portNumber and userName and password):
        with st.spinner('Fetching Columns'):
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {st.session_state['token'].get('access_token')}"
            }

            payload = {'filepath': sourceFilePath, 'type_of_file': sourceFileType,
                       'type': 'file'}

            payload2 = {'type': 'f2db',
                        'db_details': {
                            "targetDatabaseAlias": targetDatabaseAlias,
                            "targetTableName": targetTableName,
                            "targetTableQuery": targetTableQuery,
                            "targetDatabase": targetDatabase,
                            "target_connection_details": {
                                "hostname_or_url": hostname_or_url,
                                "portNumber": portNumber,
                                "userName": userName,
                                "password": password,
                                "databaseType": databaseType,
                                "isRemotePath": False
                            }
                        }}

            response = requests.request("POST", get_cols_url, json=payload, headers=headers)
            response2 = requests.request("POST", get_cols_url, json=payload2, headers=headers)

            resp = response.json()
            resp2 = response2.json()
            if response.status_code == 200 and response2.status_code == 200:

                if ('cols' in resp and resp['cols']) and ('cols' in resp2 and resp2['cols']):
                    dict_df = {'Source': resp['cols'], 'Target': resp2['cols']}
                else:
                    dict_df = {'Source': str(resp['Error']), 'Target': str(resp2['Error'])}

                st.write('Columns')
                st.dataframe(dict_df)
                st.write('Source SAMPLE DATA')
                st.dataframe(resp['data'] if 'data' in resp else {})
                st.write('Target SAMPLE DATA')
                st.dataframe(resp2['data'] if 'data' in resp2 else {})

                columns_source = resp['cols'] if resp['cols'] else []
                columns_target = resp2['cols'] if resp2['cols'] else []

                if columns_source and columns_target:

                    record_or_column = st.radio('Method', ['record', 'column'])
                    if record_or_column == 'column':
                        reporttype = "summary"
                        pkey = st.selectbox("Source Primary Key", options=columns_source)
                        if not pkey:
                            st.write('Primary key cannot be blank')

                    else:
                        reporttype = st.selectbox('Enter Type of report',
                                                  options=['summary', "detailed mismatch", "detailed match"],
                                                  key=f'type_rep', placeholder='summary')
                        pkey = ''

                    # Function to display records
                    func_obj = Function()

                    st.subheader('Column Mapping')

                    func_obj.display_records()
                    # Data keys, types of values, and ranges of values
                    i = func_obj.record_no

                    if i >= 0:
                        # Data key
                        source_key = st.selectbox('Source column Name',
                                                  placeholder='Enter the source column name',
                                                  options=columns_source,
                                                  key=f'sfdkey_{i}')

                        target_key = st.selectbox('Target Column Name',
                                                  placeholder='Enter the target column name',
                                                  options=columns_target,
                                                  key=f'tfdkey_{i}')

                        add_records = st.button(label='Add New Record')
                        remove_record = st.button(label='Remove Last Record', disabled=st.session_state.disabled)

                        # Button to add new record
                        if add_records:
                            if (source_key != '') and (target_key != ''):
                                i = func_obj.add_new_record()
                                if (source_key in st.session_state.data['sourcekeys']) or target_key in \
                                        st.session_state.data['targetkeys']:

                                    i = func_obj.remove_last_record()
                                    func_obj.record_no = i if i > 0 else 0
                                    st.write('Key already exists - ignored addition')
                                else:
                                    st.session_state.data['sourcekeys'][i] = source_key
                                    st.session_state.data['targetkeys'][i] = target_key

                                st.rerun()
                            else:
                                st.write('please add both source and target')

                        # Button to remove last record
                        if remove_record:
                            i = func_obj.remove_last_record()
                            func_obj.record_no = i if i > 0 else 0
                            st.rerun()

                    if st.button("Submit", disabled=st.session_state.disabled):

                        payload = {
                            "data": {
                                "sourceFilePath": sourceFilePath,
                                "sourceFileType": sourceFileType,
                                "sourceDelimiter": sourceDelimiter,
                                "sourceTag": "",
                                "sourceColumns": "",
                                "targetFilePath": "",
                                "targetFileType": "",
                                "targetTag": "",
                                "targetColumns": "",
                                "targetDelimiter": "",
                                "sourceStorageAlias": "",
                                "targetStorageAlias": "",
                                "primaryKey": pkey,
                                "reportType": reporttype,
                                "columnMapping": [
                                    {
                                        "sourceColumn": st.session_state.data['sourcekeys'][key],
                                        "targetColumn": st.session_state.data['targetkeys'][key]
                                    }
                                    for key in range(len(st.session_state.data['sourcekeys']))

                                ],
                                "sourceDatabaseAlias": "",
                                "sourceTableName": "",
                                "sourceTableQuery": "",
                                "sourceDatabase": "",
                                "targetDatabaseAlias": targetDatabaseAlias,
                                "targetTableName": targetTableName,
                                "targetTableQuery": targetTableQuery,
                                "targetDatabase": targetDatabase,
                                "record_or_column": record_or_column,
                                "comparisonType": "file_to_db",
                                "testCaseOpType": "file_to_db",
                                "operationType": "compare",
                                "target_connection_details": {
                                    "hostname_or_url": hostname_or_url,
                                    "portNumber": portNumber,
                                    "userName": userName,
                                    "password": password,
                                    "databaseType": databaseType,
                                    "isRemotePath": False
                                }
                            }
                        }

                        headers = {
                            'Content-Type': 'application/json',
                            'Authorization': f"Bearer {st.session_state['token'].get('access_token')}"
                        }

                        with st.spinner('Comparing'):
                            # Send a POST request to the FastAPI server
                            response = requests.request("POST", api_endpoints["File to Database Comparison"],
                                                        headers=headers,
                                                        json=payload)

                        if response.status_code == 200:

                            resp = response.json()['response']

                            output_keys = ['matched_file', 'mismatch_file', 'source_only_file',
                                           'target_only_file']
                            files = [resp[file] for file in output_keys if file in resp]

                            filtered_response = {key: resp[key] for key in resp if key not in output_keys}

                            st.write('Comparison Completed Successfully!')

                            # Create a zip file containing only the specified files
                            with zipfile.ZipFile("Output\\temp.zip", "w", compression=zipfile.ZIP_DEFLATED) as zf:
                                for file_path in files:
                                    if file_path is not None:
                                        base_name = os.path.basename(file_path)
                                        zf.write(file_path, base_name)

                            download_data = filtered_response
                            st.download_button(
                                label="Download JSON",
                                data=json.dumps(download_data).encode("utf-8"),
                                file_name='data.json',
                                mime='application/json'
                            )

                            # Display a download button
                            with open("Output\\temp.zip", "rb") as fp:
                                st.download_button(
                                    label="Download ZIP",
                                    data=fp,
                                    file_name="temp.zip",
                                    mime="application/zip"
                                )

                            os.remove("Output\\temp.zip")
                            for file_path in files:
                                time.sleep(100)
                                response = requests.post(f"{BACKEND_URL}/clear_reports", headers=headers,
                                                         json={'folder_name': os.path.dirname(file_path)})

                        else:
                            resp = response.json()

                            error = resp.get("Error") if "Error" in resp else resp.get('detail')
                            st.error(f'Request Failed - {error}')

                else:
                    st.write('Unable to fetch Columns please try again later')

            else:
                st.write(
                    f'Unable to fetch Source/Target Columns please try again later - Source Error : {resp.get("Error", "") if "Error" in resp else resp.get("detail", "")} Target Error: {resp2.get("Error", "") if "Error" in resp2 else resp2.get("detail", "")}')


    else:
        st.write('Upload Details to proceed')
        if 'data' in st.session_state:
            del st.session_state['data']


def db_to_db_comparison_form():
    """Function for db to db comparison"""
    st.subheader("Database to Database Comparison")

    sourceDatabaseAlias = st.text_input("Source Database Alias", placeholder="postgres")
    sourceTableName = st.text_input("Source Table Name", key='st')
    sourceTableQuery = st.text_input("Source Table Query", key='stq',
                                     help='Enclose table names having capital letters with a " ')
    sourceDatabase = st.text_input("Source Database", placeholder="Name of the Database", key='sdb')
    targetDatabaseAlias = st.text_input("Target Database Alias", placeholder="postgres", key='tdb')
    targetTableName = st.text_input("Target Table Name", key='ttn')
    targetTableQuery = st.text_input("Target Table Query", key='ttq',
                                     help='Enclose table names having capital letters with a " ')
    targetDatabase = st.text_input("Target Database", placeholder="Name of the Database", key='tdn')

    st.subheader('Source Connection Details')

    shostname_or_url = st.text_input("Hostname or URL", key='sh')
    sportNumber = st.text_input("Port Number", key='sn')
    suserName = st.text_input("User Name", key='su')
    spassword = st.text_input("Password", type="password", key='sp')
    sdatabaseType = st.selectbox('Type of Value for the parameter',
                                 options=["Mysql", "MsSql", "Oracle", "MongoDB", "Postgresql", "Drill", "Hana"],
                                 key=f'sbox_d')

    st.subheader('Target Connection Details')
    thostname_or_url = st.text_input("Hostname or URL", key='th')
    tportNumber = st.text_input("Port Number", key='tn')
    tuserName = st.text_input("User Name", key='tu')
    tpassword = st.text_input("Password", type="password", key='tp')
    tdatabaseType = st.selectbox('Type of Value for the parameter',
                                 options=["Mysql", "MsSql", "Oracle", "MongoDB", "Postgresql", "Drill", "Hana"],
                                 key=f'tbox_d')

    if sourceDatabaseAlias and sourceTableName and sourceTableQuery and sourceDatabase and targetDatabaseAlias and targetTableName and targetTableQuery and targetDatabase and sdatabaseType and shostname_or_url and sportNumber and suserName and spassword and tuserName and tportNumber and tpassword and tdatabaseType and thostname_or_url:

        with st.spinner('Fetching Columns'):

            headers = {
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {st.session_state['token'].get('access_token')}"
            }

            payload = {'type': 'db2db',
                       'db_details': {
                           "sourceDatabaseAlias": sourceDatabaseAlias,
                           "sourceTableName": sourceTableName,
                           "sourceTableQuery": sourceTableQuery,
                           "sourceDatabase": sourceDatabase,
                           "targetDatabaseAlias": targetDatabaseAlias,
                           "targetTableName": targetTableName,
                           "targetTableQuery": targetTableQuery,
                           "targetDatabase": targetDatabase,
                           "source_connection_details": {
                               "hostname_or_url": shostname_or_url,
                               "portNumber": sportNumber,
                               "userName": suserName,
                               "password": spassword,
                               "databaseType": sdatabaseType,
                           },
                           "target_connection_details": {
                               "hostname_or_url": thostname_or_url,
                               "portNumber": tportNumber,
                               "userName": tuserName,
                               "password": tpassword,
                               "databaseType": tdatabaseType,
                           }}}

            response = requests.request("POST", get_cols_url, json=payload, headers=headers)

            resp = response.json()
            if response.status_code == 200:
                if ('cols' in resp and resp['cols']):
                    dict_df = {'Source': resp['cols'][0], 'Target': resp['cols'][1]}
                else:
                    dict_df = {'Source': str(resp['Error']), 'Target': str(resp['Error'])}

                st.write('Columns')
                st.dataframe(dict_df)
                st.write('Source SAMPLE DATA')
                st.dataframe(resp['data'][0] if 'data' in resp else {})
                st.write('Target SAMPLE DATA')
                st.dataframe(resp['data'][1] if 'data' in resp else {})

                columns_source = resp['cols'][0] if resp['cols'] else []
                columns_target = resp['cols'][1] if resp['cols'] else []

                if columns_source and columns_target:

                    record_or_column = st.radio('Method', ['record', 'column'])

                    if record_or_column == 'column':
                        reporttype = "summary"

                        pkey = st.selectbox("Source Primary Key", options=columns_source)

                        if not pkey:
                            st.write('Primary key cannot be blank')

                    else:
                        reporttype = st.selectbox('Enter Type of report',
                                                  options=['summary', "detailed mismatch", "detailed match"],
                                                  key=f'type_rep', placeholder='summary')

                        pkey = ''

                    func_obj = Function()

                    st.subheader('Column Mapping')

                    func_obj.display_records()
                    i = func_obj.record_no

                    if i >= 0:
                        # Data key
                        source_key = st.selectbox('Source column Name',
                                                  placeholder='Enter the source column name',
                                                  options=columns_source,
                                                  key=f'sdkey_{i}')

                        target_key = st.selectbox('Target Column Name',
                                                  placeholder='Enter the target column name',
                                                  options=columns_target,
                                                  key=f'tdkey_{i}')

                        add_records = st.button(label='Add New Record')
                        remove_record = st.button(label='Remove Last Record', disabled=st.session_state.disabled)

                        # Button to add new record
                        if add_records:
                            i = func_obj.add_new_record()
                            if (source_key != '') and (target_key != ''):
                                if (source_key in st.session_state.data['sourcekeys']) or target_key in \
                                        st.session_state.data['targetkeys']:

                                    st.write('Key already exists - ignored addition')
                                    i = func_obj.remove_last_record()
                                    func_obj.record_no = i if i > 0 else 0
                                    time.sleep(3)

                                else:

                                    st.session_state.data['sourcekeys'][i] = source_key
                                    st.session_state.data['targetkeys'][i] = target_key
                                    func_obj.record_no = i

                                st.rerun()

                            else:
                                st.write('please add both source and target')

                        # Button to remove last record
                        if remove_record:
                            i = func_obj.remove_last_record()
                            func_obj.record_no = i if i > 0 else 0
                            st.rerun()

                    if st.button("Submit", disabled=st.session_state.disabled):
                        payload = {

                            "data": {
                                "sourceFilePath": "",
                                "sourceFileType": "",
                                "sourceDelimiter": "",
                                "sourceTag": "",
                                "sourceColumns": "",
                                "targetFilePath": "",
                                "targetFileType": "",
                                "targetTag": "",
                                "targetColumns": "",
                                "targetDelimiter": "",
                                "sourceStorageAlias": "",
                                "targetStorageAlias": "",
                                "primaryKey": pkey,
                                "reportType": reporttype,  # take inp summary,detailed mismatch,detailed match
                                "columnMapping": [
                                    {
                                        "sourceColumn": st.session_state.data['sourcekeys'][key],
                                        "targetColumn": st.session_state.data['targetkeys'][key]
                                    }
                                    for key in range(len(st.session_state.data['sourcekeys']))

                                ],
                                "sourceDatabaseAlias": sourceDatabaseAlias,
                                "sourceTableName": sourceTableName,
                                "sourceTableQuery": sourceTableQuery,
                                "sourceDatabase": sourceDatabase,
                                "targetDatabaseAlias": targetDatabaseAlias,
                                "targetTableName": targetTableName,
                                "targetTableQuery": targetTableQuery,
                                "targetDatabase": targetDatabase,
                                "record_or_column": record_or_column,
                                "comparisonType": "db_to_db",
                                "testCaseOpType": "db_to_db",
                                "operationType": "compare",
                                "source_connection_details": {
                                    "hostname_or_url": shostname_or_url,
                                    "portNumber": sportNumber,
                                    "userName": suserName,
                                    "password": spassword,
                                    "databaseType": sdatabaseType,
                                },
                                "target_connection_details": {
                                    "hostname_or_url": thostname_or_url,
                                    "portNumber": tportNumber,
                                    "userName": tuserName,
                                    "password": tpassword,
                                    "databaseType": tdatabaseType,
                                }
                            }
                        }

                        headers = {
                            'Content-Type': 'application/json',
                            'Authorization': f"Bearer {st.session_state['token'].get('access_token')}"
                        }

                        with st.spinner('Comparing'):
                            # Send a POST request to the FastAPI server
                            response = requests.request("POST", api_endpoints["Database to Database Comparison"],
                                                        headers=headers,
                                                        json=payload)

                        if response.status_code == 200:
                            resp = response.json()['response']

                            output_keys = ['matched_file', 'mismatch_file', 'source_only_file', 'target_only_file']
                            files = [resp[file] for file in output_keys if file in resp]

                            filtered_response = {key: resp[key] for key in resp if key not in output_keys}

                            st.write('Comparison Completed Successfully!')

                            # Create a zip file containing only the specified files
                            with zipfile.ZipFile("Output\\temp.zip", "w", compression=zipfile.ZIP_DEFLATED) as zf:
                                for file_path in files:

                                    if file_path is not None:
                                        base_name = os.path.basename(file_path)
                                        zf.write(file_path, base_name)

                            # Display a download button
                            with open("Output\\temp.zip", "rb") as fp:
                                st.download_button(
                                    label="Download ZIP",
                                    data=fp,
                                    file_name="temp.zip",
                                    mime="application/zip"
                                )
                            download_data = filtered_response
                            st.download_button(
                                label="Download JSON",
                                data=json.dumps(download_data).encode("utf-8"),
                                file_name='data.json',
                                mime='application/json'
                            )

                            os.remove("Output\\temp.zip")
                            for file_path in files:
                                time.sleep(100)
                                response = requests.post(f"{BACKEND_URL}/clear_reports", headers=headers,
                                                         json={'folder_name': os.path.dirname(file_path)})

                        else:
                            resp = response.json()

                            error = resp.get("Error") if "Error" in resp else resp.get('detail')
                            st.error(f'Request Failed - {error}')

                else:
                    st.write('Unable to fetch Columns please try again later')

            else:

                st.write(
                    f'Unable to fetch Source/Target Columns please try again later - Source Error : {resp.get("Error", "") if "Error" in resp else resp.get("detail", "")} Target Error: {resp.get("Error", "") if "Error" in resp else resp.get("detail", "")}')
    else:
        st.write('Upload Details to proceed')
        if 'data' in st.session_state:
            del st.session_state['data']


def file_conversion_to_csv_form():
    """Function for file to file csv conversion"""
    st.subheader("File Conversion to CSV")

    source_file = st.file_uploader("Upload Source File",
                                   help='Ensure Data is in tabular format in the file - non tabular data will not be processed')
    data_filepath = upload_file(source_file)
    format_option = st.selectbox('Type of Value for the parameter',
                                 options=['parquet', 'json', 'xml', "pdf", "avro"],
                                 key=f'box_s')
    if source_file:
        st.session_state.disabled = False
    else:
        st.session_state.disabled = True

    if st.button("Submit", disabled=st.session_state.disabled):

        payload = {
            "data_filepath": data_filepath,
            "format": format_option
        }

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {st.session_state['token'].get('access_token')}"
        }

        with st.spinner('Converting'):
            # Send a POST request to the FastAPI server
            response = requests.request("POST", api_endpoints["File Conversion to CSV"], headers=headers,
                                        json=payload)

        if response.status_code == 200:
            csv_data = response.content  # Extract the CSV data
            st.write('File Converted Successfully!')
            # Display the download button
            st.download_button("Download CSV", csv_data, file_name="my_file.csv", mime="text/csv")

        else:
            resp = response.json()

            error = resp.get("Error") if "Error" in resp else resp.get('detail')
            st.error(f'Request Failed - {error}')

        response = requests.post(f"{BACKEND_URL}/delete_all_tempfiles", headers=headers)


def process_request(operation):
    """Function to process request """
    st.subheader(operation)

    source_file = st.file_uploader("Upload Source File", key=f'f_{operation}')
    data_filepath = upload_file(source_file)
    if source_file:
        st.session_state.disabled = False
    else:
        st.session_state.disabled = True
    if st.button("Submit", disabled=st.session_state.disabled):

        payload = {
            "data_filepath": data_filepath,

        }
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {st.session_state['token'].get('access_token')}"
        }

        if operation == "PDF to Text Conversion":

            with st.spinner('Converting'):
                # Send a POST request to the FastAPI server
                response = requests.request("POST", api_endpoints["PDF to Text Conversion"], headers=headers,
                                            json=payload)

            if response.status_code == 200:
                csv_data = response.content  # Extract the CSV data
                st.write('File Converted Successfully!')
                # Display the download button
                st.download_button("Download File", csv_data, file_name="my_file.txt", mime="application/octet-stream")

            else:
                resp = response.json()

                error = resp.get("Error") if "Error" in resp else resp.get('detail')
                st.error(f'Request Failed - {error}')

        if operation == "Data Quality Checks":

            with st.spinner('Processing'):
                # Send a POST request to the FastAPI server
                response = requests.request("POST", api_endpoints["Data Quality Checks"], headers=headers,
                                            json=payload)

            if response.status_code == 200:
                st.subheader('Response as Records')
                response = response.json()
                # st.json(response.json())
                csv_filepath = response["csv_filepath"]
                del response["csv_filepath"]
                del response['row_count']
                del response['column_count']

                df2 = pd.DataFrame(response)
                st.dataframe(df2)
                # st.table(df2)
                download_data = response
                st.download_button(
                    label="Download JSON",
                    data=json.dumps(download_data).encode("utf-8"),
                    file_name='data.json',
                    mime='application/json'
                )

                with open(csv_filepath, "r") as file:
                    st.download_button(
                        label="Download CSV",
                        data=file,
                        file_name='data.csv',
                        mime='text/csv'
                    )

            else:
                resp = response.json()
                error = resp.get("error") if "error" in resp else resp.get("detail")
                st.error(f'Request Failed - {error}')

        if operation == "Data Profiling":

            with st.spinner('Processing'):
                # Send a POST request to the FastAPI server
                response = requests.request("POST", api_endpoints["Data Profiling"], headers=headers,
                                            json=payload)

            if response.status_code == 200:
                data = response.content  # Extract the CSV data
                st.write('File Processed Successfully!')
                # Display the download button
                st.download_button("Download File", data, file_name="my_file.html", mime="application/octet-stream")

            else:
                resp = response.json()
                error = resp.get("Error") if "Error" in resp else resp.get('detail')
                st.error(f'Request Failed - {error}')

        response = requests.post(f"{BACKEND_URL}/delete_all_tempfiles", headers=headers)


def generate_data():
    """Function to generate data"""
    st.subheader("Data Generator")
    # Initialize session state

    # Number of records to generate
    no_of_records = st.number_input('Number of records to generate', min_value=1, max_value=3000000, value=100)
    st.session_state.generate_record_no = no_of_records

    # Method (faker/random)
    method = st.radio('Method', ['faker', 'random'])
    st.session_state.method = method

    # format to save
    format_to_save = st.selectbox('Output format',
                                  options=['txt', 'parquet', 'json', 'xml',
                                           "avro", "csv", "binary", "excel",
                                           "html", "pdf"],
                                  key=f'box_s')

    func_obj = Function_DataGenerator()
    st.subheader('Inputs')

    func_obj.display_records()

    # Data keys, types of values, and ranges of values
    i = func_obj.record_no

    if i >= 0:
        # Data key
        data_key = st.text_input('Label/Json Key Name',
                                 placeholder='Enter the value to be used as a key/column name - Ensure it is unique',
                                 key=f'key_{i}')

        # Type of value
        type_of_value = st.selectbox('Type of Value for the parameter',
                                     options=['id', 'date', 'time', 'float', 'int', 'string', 'percentage',
                                              'boolean',
                                              'name', 'choice', 'word'],
                                     key=f'box_{i}')
        st.session_state.type_of_value = type_of_value

        if st.session_state.type_of_value == 'id':
            range_type = "None"

        else:
            # Range of values
            range_type = st.selectbox('Range of values', options=['List', 'Dict', 'None'],
                                      key=f'range_{i}', index=2)

        if range_type == 'List':
            range_of_values_input = st.text_input('Enter a comma-separated list', key=f'range_input_{i}')
            range_of_values = range_of_values_input.split(",") if range_of_values_input else []

        elif range_type == 'Dict':
            min_value = st.text_input('Enter min value', key=f'min_{i}',
                                      help="Enter both min and max if method is random")
            max_value = st.text_input('Enter max value', key=f'max_{i}',
                                      help="Enter both min and max if method is random")

            if min_value and not max_value:
                range_of_values = {'min': min_value}
            elif not min_value and max_value:
                range_of_values = {'max': max_value}
            elif min_value and max_value:
                range_of_values = {'min': min_value, 'max': max_value}
            else:
                st.write('Min/max cannot be blank')
                range_of_values = {}
        else:
            range_of_values = []

        add_records = st.button(label='Add New Record')
        remove_record = st.button(label='Remove Last Record', disabled=st.session_state.disabled)

        # Button to add new record
        if add_records:
            if (data_key != '') and (st.session_state.type_of_value != '') and (range_type != '') and (
                    range_of_values != ''):
                i = func_obj.add_new_record()
                if data_key in st.session_state.data['keys']:
                    i = func_obj.remove_last_record()
                    func_obj.record_no = i if i > 0 else 0
                    st.write('Key already exists - ignored addition')

                else:
                    st.session_state.data['keys'][i] = data_key
                    st.session_state.data['type_indices'][i] = st.session_state.type_of_value
                    st.session_state.data['range_indices'][i] = range_type
                    st.session_state.data['range_values'][i] = range_of_values

                st.rerun()
            else:
                st.write('please fill all values')
        # Button to remove last record
        if remove_record:
            i = func_obj.remove_last_record()
            func_obj.record_no = i if i > 0 else 0
            st.rerun()

    # Button to generate data
    if st.button('Generate Data', key='generate_data', disabled=st.session_state.disabled):

        send_data = {
            'no_of_records': st.session_state.generate_record_no,
            'method': st.session_state.method,
            'format_to_save': format_to_save,
            'data': [
                {
                    'key': st.session_state.data['keys'][i],
                    'type_of_value': st.session_state.data['type_indices'][i],
                    'range_values': st.session_state.data['range_values'][i] if st.session_state.data['range_values'][
                                                                                    i] != [] else 'None'
                }
                for i in range(len(st.session_state.data['keys']))
            ]
        }

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {st.session_state['token'].get('access_token')}"
        }
        with st.spinner('Generating Data'):
            # Send a POST request to the FastAPI server
            response = requests.request("POST", f'{api_endpoints["Data Generator"]}/generate_dataset/', headers=headers,
                                        json=send_data)

            # Check the response
            if response.status_code == 200:

                resp = response.json()
                file_path = resp.get('output_path')
                data = resp.get('sample_output', {})

                st.write('Data Generated successfully!')

                st.subheader('SAMPLE RESPONSE')
                df2 = pd.DataFrame(data) if file_path else pd.DataFrame()
                st.table(df2)

                # Add a download button
                download_response = requests.request("GET", f'{api_endpoints["Data Generator"]}/download/',
                                                     headers=headers,
                                                     params={'folder_path': file_path})

                if download_response.status_code == 200:
                    download_data = download_response.content
                    st.download_button(
                        label="Download File",
                        data=download_data,
                        file_name=f'data.{file_path.split(".")[-1]}',
                        mime='application/json'
                    )


                else:
                    st.error(f'Download Failed - {download_response.json().get("error")}')
            else:
                resp = response.json()
                error = resp.get("error") if "error" in resp else resp.get("detail")
                st.error(f'Request Failed - {error}')


def display():
    """Function to display side bar"""
    # Sidebar for API selection
    st.sidebar.title("Select an Operation")
    api_selection = st.sidebar.selectbox("Choose an Operation:", list(api_endpoints.keys()))

    # Render the appropriate form based on user selection
    if api_selection == "File to File Comparison":
        file_to_file_comparison_form()
    elif api_selection == "File to Database Comparison":
        file_to_db_comparison_form()
    elif api_selection == "Database to Database Comparison":
        db_to_db_comparison_form()
    elif api_selection == "File Conversion to CSV":
        file_conversion_to_csv_form()
    elif api_selection == "PDF to Text Conversion":
        process_request("PDF to Text Conversion")
    elif api_selection == "Data Profiling":
        process_request("Data Profiling")
    elif api_selection == "Data Quality Checks":
        process_request("Data Quality Checks")
    elif api_selection == "Data Generator":
        generate_data()


def get_login_url():
    """Function to get login url"""
    response = requests.get(f"{BACKEND_URL}/login")
    if response.status_code == 200:
        return response.json().get("login_url")
    else:
        st.error("Failed to get login URL")
        return None


def handle_callback(code):
    """Function to handle callbacks"""
    response = requests.get(f"{BACKEND_URL}/callback", params={"code": code})
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Unable to Authenticate - Login to proceed")
        return None


def main():
    """Main function"""
    try:

        col1, col2, col3 = st.columns([6, 1, 1])  # Create columns

        with col1:
            # Streamlit UI
            st.header("Infosys Data Validation Extensible(IDVEX)")

        # Handle OAuth2 callback
        if "userinfo" not in st.session_state:
            if 'code' in st.query_params:
                code = st.query_params['code']
                auth_data = handle_callback(code)
                if auth_data:
                    st.session_state["token"] = auth_data["token"]
                    st.session_state["userinfo"] = auth_data["userinfo"]
                    st.write(f"Welcome, {st.session_state['userinfo']['preferred_username']}!")
                    st.write("You have successfully authenticated.")
                    display()

        else:
            display()

        if "userinfo" in st.session_state:
            with col3:
                if st.button('Logout'):

                    for key in list(st.session_state.keys()):
                        del st.session_state[key]

                    st.markdown(f'<meta http-equiv="refresh" content="0; url={REDIRECT_URI}" />',
                                unsafe_allow_html=True)
        else:
            login_url = get_login_url()

            with col1:

                if st.button('Please log in'):
                    st.markdown(f'<meta http-equiv="refresh" content="0; url={login_url}" />', unsafe_allow_html=True)

    except requests.exceptions.ConnectionError or ConnectionError:
        st.write('Backend Server not reachable , please try after sometime')
    except Exception as e:

        st.write(f'Error - {e}')


if __name__ == '__main__':
    main()
