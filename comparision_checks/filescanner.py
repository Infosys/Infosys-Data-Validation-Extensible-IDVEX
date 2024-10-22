'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''


def check_for_csv_injection(data_frame):
    """Function to check for csv injection"""
    for column in data_frame.columns.values:
        if data_frame[data_frame[column].astype(str).str.startswith(('=', '@', '+', '-'))].shape[0] > 0:
            return True

    return False
