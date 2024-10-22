'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

FILE_TO_FILE = {
    "data": {
        "sourceFilePath": "SOURCE FILE PATH",
        "sourceFileType": "txt",
        "sourceDelimiter": ",",
        "sourceTag": "",
        "sourceColumns": "",
        "targetFilePath": "TARGET FILE PATH",
        "targetFileType": "txt",
        "targetTag": "",
        "targetColumns": "",
        "targetDelimiter": ",",
        "sourceStorageAlias": "",
        "targetStorageAlias": "",
        "primaryKey": "",
        "reportType": "summary",
        "columnMapping": [
            {},
            {
                "sourceColumn": "COL1",
                "targetColumn": "COL1"
            },
            {
                "sourceColumn": "COL2",
                "targetColumn": "COL2"
            }

        ],
        "sourceDatabaseAlias": "",
        "sourceTableName": "",
        "sourceTableQuery": "",
        "sourceDatabase": "",
        "targetDatabaseAlias": "",
        "targetTableName": "",
        "targetTableQuery": "",
        "targetDatabase": "",
        "record_or_column": "record",
        "comparisonType": "file_to_file",
        "testCaseOpType": "file_to_file",
        "isRemotePath": False
    }
}

FILE_TO_DB = {
    "data": {
        "sourceFilePath": "SOURCE FILE PATH",
        "sourceFileType": "txt",
        "sourceDelimiter": ",",
        "sourceTag": "",
        "sourceColumns": "",
        "targetFilePath": "",
        "targetFileType": "",
        "targetTag": "",
        "targetColumns": "",
        "targetDelimiter": "",
        "sourceStorageAlias": "",
        "targetStorageAlias": "",
        "primaryKey": "",
        "reportType": "summary",
        "columnMapping": [
            {},
            {
                "sourceColumn": "COL1",
                "targetColumn": "COL1"
            },
            {
                "sourceColumn": "COL2",
                "targetColumn": "COL2"
            }
        ],
        "sourceDatabaseAlias": "",
        "sourceTableName": "",
        "sourceTableQuery": "",
        "sourceDatabase": "",
        "targetDatabaseAlias": "TARGET DATABASE ALIAS EG:postgres",
        "targetTableName": "TARGET TABLE NAME",
        "targetTableQuery": "QUERY TO FETCH DATA",
        "targetDatabase": "TARGET DATABASE NAME",
        "record_or_column": "record",
        "comparisonType": "file_to_db",
        "testCaseOpType": "file_to_db",
        "operationType": "compare",
        "target_connection_details": {
            "hostname_or_url": "HOST",
            "portNumber": "PORT",
            "userName": "USER NAME",
            "password": "PASSWORD",
            "databaseType": "SOURCE DB TYPE EG:Postgresql"
        }
    }
}

DB_TO_DB = {
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
        "primaryKey": "",
        "reportType": "summary",
        "columnMapping": [
            {},
            {
                "sourceColumn": "COL1",
                "targetColumn": "COL1"
            },
            {
                "sourceColumn": "COL2",
                "targetColumn": "COL2"
            }
        ],
        "sourceDatabaseAlias": "SOURCE DATABASE TYPE ALIAS (EG postgres)",
        "sourceTableName": "SOURCE TABLE NAME",
        "sourceTableQuery": "ENTER QUERY TO FETCH DATA",
        "sourceDatabase": "SOURCE DATABASE NAME",
        "targetDatabaseAlias": "TARGET DATABASE TYPE ALIAS (EG postgres)",
        "targetTableName": "TARGET TABLE NAME",
        "targetTableQuery": "ENTER QUERY TO FETCH DATA",
        "targetDatabase": "TARGET DATABASE NAME",
        "record_or_column": "record",
        "comparisonType": "db_to_db",
        "testCaseOpType": "db_to_db",
        "source_connection_details": {
            "hostname_or_url": "HOST",
            "portNumber": "PORT",
            "userName": "USER NAME",
            "password": "PASSWORD",
            "databaseType": "SOURCE DB TYPE EG:Postgresql"
        },
        "target_connection_details": {
            "hostname_or_url": "HOST",
            "portNumber": "PORT",
            "userName": "USER NAME",
            "password": "PASSWORD",
            "databaseType": "SOURCE DB TYPE EG:Postgresql"
        }
    }
}

CONVERT_TO_CSV = {
    "data_filepath": "<INPUT PATH>",
    "format": "<FORMAT OF INPUT>"
}

DATA_PATH_INP = {
    "data_filepath": "<INPUT PATH>"
}
