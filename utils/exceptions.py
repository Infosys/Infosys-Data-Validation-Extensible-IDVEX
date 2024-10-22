'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''


class Error(Exception):
    """generic exception"""
    pass


class ColumnsLengthMismatch(Error):
    """Raised when the length of columns to compare are different"""
    pass


class ColumnsNamesMismatch(Error):
    """Raised when the names of columns to compare are different"""
    pass


class DataFrameReadError(Error):
    """Raised when the Dataframe from table could not be read"""
    pass


class CSVInjectionError(Error):
    """Raised when the CSV Contains Malicious data"""
    pass
