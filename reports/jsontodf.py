'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import json

import pandas as pd


class JsonToCsv:
    """class to convert json to csv"""

    def __init__(self, JsonPath):
        """Init function"""
        self.JsonPath = JsonPath
        self.dataframe = pd.DataFrame()
        self.main(self.JsonPath)

    def main(self, filepath):
        """Main function Load the JSON data from the file"""
        # Load the JSON data from the file (assuming the file is named "myJson.json")
        try:
            with open(filepath) as data_file:
                data = json.load(data_file)
        except json.decoder.JSONDecodeError:
            # If direct loading fails, assume it's a line-delimited JSON
            with open(filepath, 'r') as f:
                lines = f.readlines()
                data = [json.loads(line) for line in lines]

        rows = []
        if isinstance(data, dict):
            data = [data]

        for row in data:
            # Flatten the nested JSON into a dictionary
            flat_data = self.flatten_json(row)
            rows.append(flat_data)

        # Create a DataFrame from the flattened dictionary
        self.dataframe = pd.DataFrame(rows)

    def get_df(self):
        """function to get dataframe"""
        return self.dataframe

    def flatten_json(self, json_data, parent_key='', sep='.'):
        """
        Flatten a nested JSON object into a flat dictionary.
        """
        items = []
        for k, v in json_data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_json(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if all(isinstance(item, dict) for item in v):
                    for idx, item in enumerate(v):
                        items.extend(self.flatten_json(item, f"{new_key}[{idx}]", sep=sep).items())
                else:
                    items.append((new_key, v))
            else:
                items.append((new_key, v))
        return dict(items)
