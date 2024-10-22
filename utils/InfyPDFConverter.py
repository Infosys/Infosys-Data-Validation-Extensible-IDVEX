'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import datetime
import os

import PyPDF2

from utils.ServerLogs import logger


def createTempFolder():
    """Function to create temp folder"""
    prj_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    temp_folder = os.path.join(prj_dir, r"tempfiles")
    os.makedirs(temp_folder, exist_ok=True)


def createFileName(ext):
    """Function to create file name"""
    try:
        utc_timestmp = datetime.datetime.now()
        timestr = utc_timestmp.strftime("%Y%m%d-%H%M%S")
        utc_timestmp = str(utc_timestmp)[:-3]
        filename = f"temp_{timestr}.{ext}"
        return filename
    except Exception as e:
        logger.error(f"Error in createFileName() : {e}")


def validate_pdf(file_path):
    """Checks if the selected file is a valid PDF."""
    try:
        with open(file_path, 'rb') as f:
            pdf_reader = PyPDF2.PdfReader(f)
            return True, ""
    except FileNotFoundError:
        logger.error(f"File not Found at : {file_path}")
        return False, f"File not Found at : {file_path}"
    except PyPDF2.PdfReaderError:
        logger.error(f"PDF invalid format : {file_path}")
        return False, "PDF invalid format"


def convert_pdf_to_format(input_file_path, output_type):
    """Converts a PDF file to the specified format."""
    createTempFolder()
    prj_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    output_folder = os.path.join(prj_dir, "tempfiles")
    if output_type == "text":
        try:
            output_file_path = os.path.join(output_folder, createFileName("txt"))
            with open(input_file_path, 'rb') as pdf_file, open(output_file_path, 'w') as text_file:
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    text_file.write(page.extract_text())
            return output_file_path, ""
        except Exception as e:
            logger.error(f"Error in pdf-to-text conversion : {e}")
            return None, f"Error in pdf-to-text conversion : {e}"
    else:
        logger.error(f"Invalid Conversion : {output_type}")
        return None, ""
