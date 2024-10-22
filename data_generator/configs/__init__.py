'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import importlib
import os

curr_dir = os.path.dirname(os.path.abspath(__file__))

# Importing configuration files if exists
default_filepath = os.path.join(curr_dir, "config.py")
default = None
default_module_path = 'data_generator.configs.config'

if os.path.exists(default_filepath):
    default = importlib.import_module(default_module_path, __package__)


class Config(object):
    """Config class to read the common configs"""
    _config = {}  # Dictionary to store attribute-value pairs

    def __getattr__(self, attr):
        """Function to get config attributes

        Args:
            attr: Attribute name to get

        Returns:
            None

        Raises:
            AttributeError : If configuration is missing

        """
        # Check if the attribute exists in the dictionary
        if attr in self._config:  # Precedence-1: dynamic variables
            return self._config[attr]
        # If the attribute is not found in the dictionary, try to get it from environment variables
        if os.getenv(attr) is not None:  # Precedence-2: env variables
            return os.getenv(attr)
        if hasattr(default, attr):  # Precedence-4: default.py
            return getattr(default, attr)

        # If the attribute is not found in environment variables or the dictionary, raise an error
        raise AttributeError("Configuration key missing: {}".format(attr))

    def __setattr__(self, attr, value):
        """Function to get config attributes

        Args:
            attr: Attribute name to set
            value: value of the attribute to set

        Returns:
            None

        Raises:
            None

        """
        # Allow setting attributes dynamically using dot notation
        self._config[attr] = value


config = Config()
