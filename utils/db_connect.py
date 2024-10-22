import os

import oracledb
import oracledb as cx_Oracle
import pandas as pd
import paramiko
import psycopg2
import pyhdb
import pymysql
import pyodbc
from pydrill.client import PyDrill
from pyhive import hive

'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

from pymongo import MongoClient
from configs import config

from utils.ServerLogs import logger

DRILL_ALLOWED_SET = {"hbase"}
message = ''


def postgres_db_obj(database, connection_details):
    """Function to get postgresql connection object"""
    dbDetailsObject = get_db_details(connection_details)

    try:
        if (dbDetailsObject['sslCert'] != None):

            connection = psycopg2.connect(
                host=dbDetailsObject['hostname_or_url'], user=dbDetailsObject['userName'],
                port=dbDetailsObject['portNumber'], sslmode="require", sslcert=dbDetailsObject['sslCert'],
                sslkey=dbDetailsObject['sslKey'], sslrootcert=dbDetailsObject['sslRootCert'])
        else:
            logger.info(f"inside postgres_db_obj username- {dbDetailsObject['userName']}")
            connection = psycopg2.connect(user=dbDetailsObject['userName'],
                                          password=dbDetailsObject['password'],
                                          host=dbDetailsObject['hostname_or_url'],
                                          port=dbDetailsObject['portNumber'],
                                          database=database)
    except Exception as e:
        raise Exception(f'Unable to connect to Db -{e}')

    return connection


def mysql_db_Obj(db, connectionAlias):
    """Function to get mysql connection object"""
    dbDetailsObject = get_db_details(connectionAlias)
    try:
        connection = pymysql.connect(host=dbDetailsObject['hostname_or_url'],
                                     user=dbDetailsObject['userName'],
                                     password=dbDetailsObject['password'],
                                     database=db)
        return connection
    except Exception as e:
        raise Exception(f'Unable to connect to Db -{e}')


def sql_server_db_obj(db, connectionAlias):
    """Function to get SQL Server Connection object"""
    dbDetailsObject = get_db_details(connectionAlias)

    try:
        connection = pyodbc.connect(driver='{SQL Server}',
                                    Server=dbDetailsObject['hostname_or_url'],
                                    Database=db,
                                    UID=dbDetailsObject['userName'],
                                    PWD=dbDetailsObject['password'])

        return connection
    except Exception as e:
        raise Exception(f'Unable to connect to Db -{e}')


def oracle_obj(connectionAlias):
    """function to get oracle connection object"""
    dbDetailsObject = get_db_details(connectionAlias)
    initiator = client_initiator()
    logger.info(f"Initiator inside login ---- {initiator}")

    try:
        host = dbDetailsObject['hostname_or_url'].split("/")[0]
        service = dbDetailsObject['hostname_or_url'].split("/")[1]
        if (dbDetailsObject['walletFile'] != None):
            connection = oracledb.connect(host=host, user=dbDetailsObject["userName"],
                                          port=dbDetailsObject["portNumber"],
                                          service_name=service, wallet_location=dbDetailsObject['walletFile'])
        else:
            dsn_tns = cx_Oracle.makedsn(host, dbDetailsObject['portNumber'], service_name=service)

            connection = cx_Oracle.connect(dbDetailsObject['userName'],
                                           dbDetailsObject['password'],
                                           dsn_tns)
        return connection
    except Exception as e:
        logger.info(f"Error Occured inside oracle_obj--  {e}")
        raise Exception(f'Unable to connect to Db -{e}')


def drill_obj(connectionAlias):
    """Function to get drill connection object"""
    dbDetailsObject = get_db_details(connectionAlias)
    try:
        drill_conn = PyDrill(
            host=dbDetailsObject['hostname_or_url'], port=dbDetailsObject['portNumber'])

        return drill_conn
    except Exception as e:
        logger.info(f"This exception has happened while connecting to drill: {e}")
        raise Exception(f'Unable to connect to Db -{e}')


def mongo_client_obj(db, connectionAlias):
    """Function to get monodb client object"""
    dbDetailsObject = get_db_details(connectionAlias)
    try:
        client = MongoClient('mongodb://localhost:27017/')
        return client[db]
    except Exception as e:
        raise Exception(f'Unable to connect to Db -{e}')


def s4_hana_obj(connectionAlias):
    """Function to hana connection object"""
    dbDetailsObject = get_db_details(connectionAlias)
    try:
        connection = pyhdb.connect(dbDetailsObject['hostname_or_url'],
                                   dbDetailsObject['portNumber'],
                                   dbDetailsObject['userName'],
                                   dbDetailsObject['password'])
        return connection
    except Exception as E:
        raise Exception(
            "This exception happened while trying to utils to hana:" + str(E))


def connect_mysql(mysql_server, user, pwd, port):
    """Function to connect mysql db"""
    host = mysql_server

    try:
        connection = pymysql.connect(host=host, user=user, password=pwd, port=port)
        if connection is None:
            raise Exception('Unable to connect to the database')

        cursor = connection.cursor()
        connection.commit()
        if (connection):

            cursor.execute("show databases")
            mysql_return_file = 'filefrommysql.csv'
            mysql_df = pd.read_sql("show databases", connection)
            databasename_list = mysql_df['Database'].values

            mysql_table_df = {}

            for values in databasename_list:
                cursor.execute("use " + values)
                cursor.execute("show tables")
                mysql_table_df[values] = (pd.read_sql("show tables", connection))[
                    'Tables_in_' + values].values

            message = "Connection Successfull"
            return message, mysql_df, mysql_table_df
        else:
            message = "Connection Failed!!"
            raise Exception(f'Unable to connect to Db ')

    except Exception as E:
        message = "Connection Failed!! The reason:" + str(E)
        logger.info(str(E))
        raise Exception(f'Unable to connect to Db -{E}')


def connect_hive(hive_server, port, user, pwd):
    """Function to connect hive db"""
    try:
        connection = hive.Connection(host=hive_server, port=port,
                                     username=user, password=pwd, auth='CUSTOM')

        if connection is None:
            raise Exception('Unable to connect to the database')

        cursor = connection.cursor()
        connection.commit()
        if (connection):
            message = "Connection Successfull"
            cursor.execute("show databases")
            hive_df = pd.read_sql("show databases", connection)
            databasename_list = hive_df['database_name'].values

            hive_table_df = {}

            for values in databasename_list:
                cursor.execute("use " + values)
                cursor.execute("show tables")
                hive_table_df[values] = (pd.read_sql("show tables", connection))[
                    'tab_name'].values

            message = "Connection Successfull"
            return message, {"Database": databasename_list}, hive_table_df
        else:
            message = "Connection Failed!!"
            raise Exception(f'Unable to connect to Db')
    except Exception as e:
        message = "Connection Failed!!"
        raise Exception(f'Unable to connect to Db -{e}')


def connect_oracle(server, user, password, port):
    """Function to connect oracle db"""
    logger.info("Came inside connect_oracle function")

    initiator = client_initiator()
    logger.info(f"Initiator inside login 2--------{initiator}")

    table_dict = {}
    host = server.split("/")[0]
    logger.info(f"Host is --{host}")
    service = server.split("/")[1]
    logger.info(f"Service is -- {service}")
    dsn_tns = cx_Oracle.makedsn(host, port, service_name=service)
    try:
        if (initiator):
            connection = cx_Oracle.connect(user, password, dsn_tns)
        else:
            connection = cx_Oracle.connect(user, password, dsn_tns)

        if connection is None:
            raise Exception('Unable to connect to the database')

        cursor = connection.cursor()
        database_frame = pd.read_sql(
            "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') as dbname FROM DUAL", connection)
        tables_frame = pd.read_sql(
            "SELECT table_name FROM user_tables", connection)
        database_name = database_frame['DBNAME'].values
        tables_name = tables_frame['TABLE_NAME'].values
        table_dict[database_name[0]] = tables_name
        return "ok", {"Database": database_name}, table_dict

    except Exception as e:
        logger.info(str(e))

    return "failure", [], []


def connect_oracle_wallet(hostUrl, user, port, wal_loc):
    """Function to connect oracle wallet db"""
    try:
        table_dict = {}
        urlSplit = hostUrl.split("/")
        host = urlSplit[0]
        service = urlSplit[1]
        connection = oracledb.connect(user=user, host=host, port=port,
                                      service_name=service, wallet_location=wal_loc)
        database_frame = pd.read_sql(
            "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') as dbname FROM DUAL", connection)
        tables_frame = pd.read_sql(
            "SELECT table_name FROM user_tables", connection)
        database_name = database_frame['DBNAME'].values
        tables_name = tables_frame['TABLE_NAME'].values
        table_dict[database_name[0]] = tables_name
        return "ok", {"Database": database_name}, table_dict

    except Exception as e:
        logger.info(f"Can not utils to oracle{e}")
        return "failure", [], []


def connect_mongo(server, port):
    """Function to connect mongo db"""
    connection_string = prepare_mongo_url(server, port)
    client = MongoClient(connection_string)

    database_list = client.database_names()

    d = dict((db, [collection for collection in client[db].collection_names()])
             for db in client.database_names())

    return "", {"Database": database_list}, d


def connect_sql_server(server, user, pwd, port):
    """Function to connect to SQL DB"""
    try:

        connection = pyodbc.connect(driver='{SQL Server}',
                                    server=server,
                                    UID=user,
                                    PWD=pwd)

        if connection is None:
            raise Exception('Unable to connect to the database')

        cursor = connection.cursor()
        connection.commit()
        logger.info("DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';UID='+user+';PWD='+ pwd")

        if (connection):
            message = "Connection Successfull"
            mysql_df = pd.read_sql("select * from sys.databases", connection)
            databasename_list = mysql_df['name'].values
            mysql_table_df = prepare_sql_server_db_table_mapping(
                databasename_list, connection)
            message = "Connection Successfull"
            return message, {"Database": databasename_list}, mysql_table_df

        else:
            message = "Connection Failed!!"
            raise Exception(f'Unable to connect to Db ')

    except Exception as E:
        logger.info(str(E))
        message = "Connection Failed!!"
        raise Exception(f'Unable to connect to Db -{E}')


def prepare_sql_server_db_table_mapping(databasename_list, connection):
    """Function to get data from tables using SQL Query"""
    mysql_table_df = {}
    for database in databasename_list:
        if " " in database:
            query = "SELECT TABLE_NAME FROM " + '"' + \
                    database + '"' + ".INFORMATION_SCHEMA.TABLES"
        elif "_" in database:
            logger.info(f"{database} *&*&*&*&*&*&&*&*&")
            query = "SELECT TABLE_NAME FROM " + '"' + \
                    database + '"' + ".INFORMATION_SCHEMA.TABLES"
        else:
            query = "SELECT TABLE_NAME FROM " + database + ".INFORMATION_SCHEMA.TABLES"

        try:
            mysql_table_df[database] = pd.read_sql(query, connection)[
                'TABLE_NAME'].values
        except Exception:
            continue
    logger.info(f"Tables are - {mysql_table_df}")

    return mysql_table_df


def get_db_details(connectionAlias):
    """Function to get Database Connection object"""
    dbDetailsObject = DBConnectionModel().get(
        connectionAlias=connectionAlias)
    return dbDetailsObject


def get_database_type(connectionAlias):
    """Function to get the database type"""
    databaseType = get_db_details(connectionAlias)['databaseType']
    return databaseType


def connect_postgresql(server, user, password, port, **sslData):
    """Function to connect postgres db"""
    if (sslData != {}):

        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=server,
                                      port=port,
                                      sslmode="require", sslcert=sslData.get('cert'), sslkey=sslData.get('keyVal'),
                                      sslrootcert=sslData.get('root'))
    else:
        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=server,
                                      port=port)

    return connection


def connect_hana(server, port, user, pwd):
    """Function to connect hana db"""
    try:
        connection = pyhdb.Connection(host=server,
                                      port=port,
                                      user=user,
                                      password=pwd)
        return connection
    except Exception as E:
        raise Exception(
            "This exception happened while trying to utils to hana:", str(E))


def add_remote_server(hostname_or_url, userName, password, ):
    """Function to add remote server connection"""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(hostname_or_url, username=userName, password=password)
        return True
    except Exception as E:
        logger.error(f"Exception - {str(E)}")
        raise Exception(f'Exception -{E}')


# newdb connection class
class DBConnectionModel:
    """Database connection class"""
    connection = {}

    def __init__(self, connectionAlias=None, databaseType=None, hostname_or_url=None,
                 userName=None, password=None, portNumber=None, remotePath=None, hostName=None,
                 sslCert=None, sslKey=None, sslRoot=None, walletFile=None):
        """Initialisation function"""

        if (connectionAlias != None) and connectionAlias != '' and (connectionAlias not in self.connection):
            self.connection[connectionAlias] = {
                'databaseType': databaseType,
                'hostname_or_url': hostname_or_url,
                'userName': userName,
                'password': password,
                'portNumber': portNumber,
                'remotePath': remotePath,
                'hostName': hostName,
                'sslCert': sslCert,
                'sslKey': sslKey,
                'sslRootCert': sslRoot,
                'walletFile': walletFile
            }
        elif connectionAlias in self.connection:
            if databaseType != None:
                self.connection[connectionAlias]['databaseType'] = databaseType

            if hostName != None:
                self.connection[connectionAlias]['hostname_or_url'] = hostname_or_url

            if userName != None:
                self.connection[connectionAlias]['userName'] = userName

            if password != None:
                self.connection[connectionAlias]['password'] = password

            if portNumber != None:
                self.connection[connectionAlias]['portNumber'] = portNumber

            if remotePath != None:
                self.connection[connectionAlias]['remotePath'] = remotePath

            if hostName != None:
                self.connection[connectionAlias]['hostName'] = hostName

            if sslCert != None:
                self.connection[connectionAlias]['sslCert'] = sslCert

            if sslKey != None:
                self.connection[connectionAlias]['sslKey'] = sslKey

            if sslRoot != None:
                self.connection[connectionAlias]['sslRootCert'] = sslRoot

            if walletFile != None:
                self.connection[connectionAlias]['walletFile'] = walletFile

    @classmethod
    def get(cls, connectionAlias):
        """Get connection method"""

        if connectionAlias in cls.connection:
            return cls.connection[connectionAlias]
        else:
            return None

    @classmethod
    def get_hostname(cls, connectionAlias):
        """method to get hostname"""
        if connectionAlias not in cls.connection:
            raise ConnectionAbortedError('Connection not Found')
        elif cls.connection[connectionAlias]['hostName'] == '' or cls.connection[connectionAlias]['hostName'] is None:
            raise ConnectionError('Hostname not found')
        else:
            return cls.connection[connectionAlias]

    @classmethod
    def get_databaseType(cls, connectionAlias):
        """method to get database type"""

        if connectionAlias not in cls.connection:
            raise ConnectionAbortedError('Connection not Found')
        elif cls.connection[connectionAlias]['databaseType'] == '' or cls.connection[connectionAlias][
            'databaseType'] is None:
            raise ConnectionError('databaseType not found')
        else:
            return cls.connection[connectionAlias]


def client_initiator():
    """Function to initiate an oracle client connection"""
    try:
        ORACLE_CLIENT = os.path.join(config.BASE_DIR, config.ORACLE_CLIENT_ID)
        cx_Oracle.init_oracle_client(lib_dir=ORACLE_CLIENT)
        return True
    except Exception as e:
        logger.error(f'Exception - {e}')
        return False


##mongo credentials
def prepare_mongo_url(server, port):
    """Function to prepare mongodb connection url"""
    connection_string = "mongodb://" + server + ':' + str(port)

    return connection_string
