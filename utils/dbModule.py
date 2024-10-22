'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import json

from pydrill.client import PyDrill

from utils.ServerLogs import logger
from utils.db_connect import connect_mysql, DBConnectionModel, connect_mongo, connect_sql_server, \
    connect_postgresql, connect_oracle_wallet, connect_oracle, connect_hive, connect_hana, add_remote_server


def drill_storage_plugin_create(connectionAlias, storagePluginConfig, host='localhost', port=8047):
    """Function to create drill plugins for connection"""
    drill_conn = PyDrill(host=host, port=port)
    logger.info("Drill connection is successful.")
    connection_status = {}
    try:
        connection_status = drill_conn.storage_update(connectionAlias, storagePluginConfig)
    except:
        connection_status = {"Placeholder": "Placeholder"}
    logger.info("connection Tested successfully")
    logger.info(connection_status)
    if drill_conn:

        logger.info(f"storagePluginConfig - {storagePluginConfig}")
        try:
            connection_status_config = json.loads(storagePluginConfig)
        except:
            connection_status_config = {"Placeholder": "Placeholder"}
        user_Name, pass_word = "Placeholder", "Placeholder"
        if 'config' in connection_status_config and 'username' in connection_status_config['config']:
            user_Name = connection_status_config['config']['username']
        if 'config' in connection_status_config and 'password' in connection_status_config['config']:
            pass_word = connection_status_config['config']['password']
        databaseObject = DBConnectionModel(connectionAlias=connectionAlias,
                                           databaseType="Drill",
                                           hostname_or_url=host,
                                           userName=user_Name,
                                           password=pass_word,
                                           portNumber=port,
                                           remotePath=None,
                                           hostName="Host")

        return connection_status


def add_connection(databaseType, hostname_or_url,
                   port, userName, password, connectionAlias, storagePlugin=None, **sslData, ):
    """Function to establish a DB Connection"""
    logger.info("Coming inside add connection")

    if (sslData != {}):
        logger.info("***********Inside sslData value check*****")
        cert = sslData.get('cert')
        keyVal = sslData.get('key1')
        root = sslData.get('root')
        wallet = sslData.get('wallet')
        logger.info(f"Wallet is ------- {wallet}")
    else:
        cert = ''
        keyVal = ''
        root = ''

    status, obj = check_for_duplicate_connection(connectionAlias)
    if not (status):
        if databaseType == 'Mysql':
            connectionStatus = connect_mysql(
                hostname_or_url, userName, password, port)
        elif databaseType == "Drill":
            drill_storage_plugin_create(
                connectionAlias, storagePlugin, host=hostname_or_url, port=port)
            return True
        elif databaseType == 'MongoDB':

            connectionStatus = connect_mongo(
                hostname_or_url, port)

        elif databaseType == 'MsSql':
            connectionStatus = connect_sql_server(
                hostname_or_url, userName, password, port)
        elif databaseType == 'Postgresql':
            if (sslData != {}):

                connectionStatus = connect_postgresql(
                    hostname_or_url, userName, password, port, cert=cert, keyVal=keyVal, root=root)
            else:

                connectionStatus = connect_postgresql(
                    hostname_or_url, userName, password, port)

            logger.info(f"Connection status aay ****************** - {connectionStatus}")

        elif databaseType == 'Oracle':
            if (sslData != {}):
                logger.info("Inside wallet connectivity")
                connectionStatus = connect_oracle_wallet(hostname_or_url, userName, port, wallet)
            else:
                connectionStatus = connect_oracle(
                    hostname_or_url, userName, password, port)
            logger.info(connectionStatus)
        elif databaseType == 'Linux':
            connectionStatus = add_remote_server(
                hostname_or_url, userName, password, )
        elif databaseType == "Hive":
            connectionStatus = connect_hive(
                hostname_or_url, port, userName, password)
        elif databaseType == "Hana":
            connectionStatus = connect_hana(
                hostname_or_url, port, userName, password)
        else:
            raise Exception('Invalid Database Type')

        if connectionStatus:
            databaseObject = DBConnectionModel(databaseType=databaseType,
                                               hostname_or_url=hostname_or_url,
                                               userName=userName,
                                               password=password,
                                               portNumber=port,
                                               connectionAlias=connectionAlias,
                                               )

            return True, databaseObject
    else:
        logger.error("Duplicate Connection exists")
    return True, obj


def check_for_duplicate_connection(connectionAlias):
    """Function to check for duplicate Connection"""

    connection = DBConnectionModel().get(connectionAlias=connectionAlias)

    if connection:
        return True, connection
    else:
        return False, ''
