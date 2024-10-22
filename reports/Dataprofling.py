'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import datetime
import os

import pandas as pd
from bs4 import BeautifulSoup
from ydata_profiling import ProfileReport

from utils.ServerLogs import logger


def removeTagsFromHTML(filepath):
    """Function to remove tags from html"""
    try:
        # read html file
        with open(filepath, 'r', encoding='utf-8') as fb:
            html_content = fb.read()
        # update title as per the uploaded file name
        soup = BeautifulSoup(html_content, "html.parser")
        # update header of the reaport
        a_tags = soup.findAll("a")
        for a in a_tags:
            if a.text == "Reproduction":
                a.string.replace_with("Summary")
                break
        p_tags = soup.find_all("p")
        for p in p_tags:
            if p.text == "Reproduction":
                p.string.replace_with("Report Creation Details")
                break
        # write the modified content to the html file
        with open(filepath, 'w', encoding="utf-8") as fb:
            fb.write(str(soup))
    except Exception as e:
        logger.error(f"Error in removeTagsFromHTML() : {e}")


def createDataFrameFromSource(datafile):
    """Function to create dataframe from source data"""
    df = pd.DataFrame([])
    encoding_modes = ['CP1252', 'UTF-8', 'ISO-8859-1', 'Windows-1252', 'UTF-16LE', 'ISO-8859-2', 'UTF-16BE', 'ignore']
    try:
        if datafile.endswith(".csv"):
            for e in encoding_modes:
                try:
                    if e == 'ignore':
                        df = pd.DataFrame(pd.read_csv(datafile), index=None)
                    else:
                        df = pd.DataFrame(pd.read_csv(datafile, encoding=e), index=None)
                    break
                except:
                    logger.info(f"Encoding failed : {e}")
                    continue
        elif datafile.endswith(".xlsx"):
            df = pd.DataFrame(pd.read_excel(datafile), index=None)
        else:
            logger.error(f"Error in createDataFrameFromSource() : Unsupported File Type : {datafile}")
            return False
        return df
    except Exception as e:
        logger.error(f"Error in createDataFrameFromSource() : {e}")
        return df


def createFileName(ext="html"):
    """Function to create filename"""
    try:
        utc_timestmp = datetime.datetime.now()
        timestr = utc_timestmp.strftime("%Y%m%d-%H%M%S")
        utc_timestmp = str(utc_timestmp)[:-3]
        filename = f"temp_{timestr}.{ext}"
        return filename
    except Exception as e:
        logger.error(f"Error in Dataprofiling:createFileName() : {e}")
        return ""


def create_data_profile(data_file):
    """function to create data profile"""
    try:
        prj_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        # Read data file
        data_df = createDataFrameFromSource(data_file)
        # Generate exploration report
        data_profile = ProfileReport(data_df, minimal=True, explorative=True)
        folder_path = os.path.join(prj_dir, 'tempfiles')
        save_filename = os.path.join(folder_path, createFileName())
        data_profile.to_file(save_filename)
        removeTagsFromHTML(save_filename)
        return save_filename
    except Exception as e:
        logger.error(f"Error in Dataprofiling feature: explore_data() : {e}")
        return None
