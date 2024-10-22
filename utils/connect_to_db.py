'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

from utils.dbModule import add_connection


def connect(connectionData, alias):
    """Function to get Database connection parameters and establish connection"""

    name = connectionData.get('userName')
    host = connectionData.get('hostname_or_url')
    password = connectionData.get('password')
    databaseType = connectionData.get('databaseType')
    connectionAlias = alias
    port = connectionData.get('portNumber')

    if "storagePluginConfig" in connectionData:
        storagePlugin = connectionData['storagePluginConfig']
    else:
        storagePlugin = None

    sslCert = connectionData.get('sslCert')
    sslKey = connectionData.get('sslKey')
    sslRootCert = connectionData.get('sslRootCert')
    walletFile = connectionData.get('walletFile')

    if sslCert != None or walletFile != None:
        bool_val = add_connection(databaseType, host,
                                  port,
                                  name,
                                  password,
                                  connectionAlias, storagePlugin,
                                  cert=sslCert, key1=sslKey,
                                  root=sslRootCert,
                                  wallet=walletFile)
    else:
        bool_val = add_connection(databaseType, host,
                                  port,
                                  name,
                                  password,
                                  connectionAlias,
                                  storagePlugin)

    return bool_val
