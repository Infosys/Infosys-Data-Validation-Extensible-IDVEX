'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import os


def delete_temp_file(filepath):
    """Function to delete temp files"""
    if os.path.exists(filepath):
        os.remove(filepath)
    else:
        pass
