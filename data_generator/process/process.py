'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import json

import fastavro
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from reportlab.lib.pagesizes import A4
from reportlab.platypus import PageTemplate, Frame, BaseDocTemplate

from data_generator.process.report import add_title, on_page, add_table


class Process:
    """Class to process input data"""

    def get_file_extension(self, file_type):
        """Function to get the file extension"""
        if file_type == 'txt':
            extension = '.txt'
        elif file_type == 'parquet':
            extension = '.parquet'
        elif file_type == 'json':
            extension = '.json'
        elif file_type == 'xml':
            extension = '.xml'
        elif file_type == 'avro':
            extension = '.avro'
        elif file_type == 'csv':
            extension = '.csv'
        elif file_type == 'binary':
            extension = '.bin'
        elif file_type == 'excel':
            extension = '.xlsx'  # use '.xls' for older versions
        elif file_type == 'html':
            extension = '.html'
        elif file_type == 'pdf':
            extension = '.pdf'
        else:
            extension = ''

        return extension

    def build_avro_schema(self, df):
        """Function to build avro schema"""
        fields = []
        for column in df.columns:
            dtype = df[column].dtype
            if pd.api.types.is_integer_dtype(dtype):
                avro_type = 'int'
            elif pd.api.types.is_float_dtype(dtype):
                avro_type = 'double'
            else:
                avro_type = 'string'  # Default to string for non-numeric types
            fields.append({'name': column, 'type': avro_type})
        return {'type': 'record', 'name': 'my_record', 'fields': fields}

    def save_json(self, output_json, file_format, output_path):
        """Function to save json"""

        df = pd.DataFrame(output_json, columns=output_json.keys())

        if file_format == 'csv':
            df.to_csv(output_path, index=False)

        elif file_format == 'json':
            json_to_save = df.to_dict(orient='records')

            # Write the data to the file
            with open(output_path, 'w') as f:
                json.dump(json_to_save, f, ensure_ascii=False, indent=4)

        elif file_format == 'avro':

            records = df.to_dict('records')
            # Modify schema to fit Avro format

            schema = self.build_avro_schema(df)

            with open(output_path, 'wb') as out:
                fastavro.writer(out, schema, records)

        elif file_format == 'parquet':
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_path)

        elif file_format == 'txt':
            df.to_csv(output_path, sep='\t', index=False)

        elif file_format == 'pdf':

            self.generate_pdf(output_path, df)

        elif file_format == 'binary':
            with open(output_path, 'wb') as f:
                df.to_pickle(f)

        elif file_format == 'excel':
            df.to_excel(output_path, index=False)

        elif file_format == 'html':
            df.to_html(output_path, index=False)

        elif file_format == 'xml':
            df.to_xml(output_path, index=False)

        else:
            raise ValueError("Unsupported file format!")

        sample = df.head(10).astype('str')
        return output_path, sample

    def generate_pdf(self, output_path, df):
        """Function to generate pdf"""
        padding = dict(
            leftPadding=50,
            rightPadding=50,
            topPadding=30,
            bottomPadding=10)

        portrait_frame = Frame(0, 0, *A4, **padding)

        portrait_template = PageTemplate(
            id='portrait',
            frames=portrait_frame,
            onPage=on_page,
            pagesize=A4)

        doc = BaseDocTemplate(
            output_path,
            pageTemplates=[
                portrait_template
            ]
        )

        story = []  # flowables

        # add title
        story = add_title(story=story, title='Infosys | Data Quality Engineering')

        story = add_table(story=story, input_data=df)

        doc.build(story)

        return output_path
