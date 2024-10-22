'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

from json import JSONEncoder


class RecordResponse(JSONEncoder):
    """class to get response object"""

    def __init__(self):
        """Init function"""
        self.source_records = None
        self.target_records = None
        self.Mismatched_records = None
        self.Additional_records_in_source = None
        self.Additional_records_in_target = None

        self.source_only = None
        self.target_only = None

        self.Matched_records = None
        self.similiar_rows = None
        self.attribute_names = None

        self.matched_file = None
        self.mismatch_file = None
        self.source_only_file = None
        self.target_only_file = None

    def set_source_records(self, source_records):
        """get source records"""
        self.source_records = source_records

    def set_target_records(self, target_records):
        """set target records"""
        self.target_records = target_records

    def set_Mismatched_records(self, Mismatched_records):
        """Set mismatched records"""
        self.Mismatched_records = Mismatched_records

    def set_Additional_records_in_source(self, Additional_records_in_source):
        """Set only source records"""
        self.Additional_records_in_source = Additional_records_in_source

    def set_Additional_records_in_target(self, Additional_records_in_target):
        """set only target records"""
        self.Additional_records_in_target = Additional_records_in_target

    def set_source_only(self, source_only):
        """set common records in source target"""
        self.source_only = source_only

    def set_target_only(self, target_only):
        """set common records in target and source"""
        self.target_only = target_only

    def set_similiar_rows(self, similiar_rows):
        """set similar records"""
        self.similiar_rows = similiar_rows

    def set_Matched_records(self, Matched_records):
        """set matched records"""
        self.Matched_records = Matched_records

    def set_attribute_names(self, attribute_names):
        """set attribute names"""
        self.attribute_names = attribute_names

    def set_matched_file(self, matched_file):
        """set matched file path"""
        self.matched_file = matched_file

    def set_mismatch_file(self, mismatch_file):
        """set mismatched file path"""
        self.mismatch_file = mismatch_file

    def set_source_only_file(self, source_only_file):
        """set source  file path"""
        self.source_only_file = source_only_file

    def set_target_only_file(self, target_only_file):
        """set target file path"""
        self.target_only_file = target_only_file

    def get_instantiated_instance(self,
                                  source_records,
                                  target_records,
                                  Mismatched_records,
                                  Additional_records_in_source,
                                  Additional_records_in_target,
                                  source_only,
                                  target_only,
                                  Matched_records,
                                  similiar_rows,
                                  attribute_names,
                                  matched_file,
                                  mismatch_file,
                                  source_only_file,
                                  target_only_file):
        """get instance"""
        self.set_source_records(source_records)
        self.set_target_records(target_records)
        self.set_Mismatched_records(Mismatched_records)
        self.set_Additional_records_in_source(Additional_records_in_source)
        self.set_Additional_records_in_target(Additional_records_in_target)
        self.set_source_only(source_only)
        self.set_target_only(target_only)
        self.set_Matched_records(Matched_records)
        self.set_similiar_rows(similiar_rows)
        self.set_attribute_names(attribute_names)
        self.set_matched_file(matched_file)
        self.set_mismatch_file(mismatch_file)
        self.set_source_only_file(source_only_file)
        self.set_target_only_file(target_only_file)

    def get_json_representaion(self):
        """get json"""
        return self.__dict__


class ColumnResponse(JSONEncoder):
    """Class for column response object"""

    def __init__(self):
        """Init function"""
        self.total_mismatch_count = 0
        self.no_of_records_source = 0
        self.no_of_records_target = 0
        self.column_wise_mismatch = {}  # 1000
        self.matched_file = {}
        self.mismatch_file = {}

    def set_total_mismatch_count(self, total_mismatch_count):
        """set total mismatch count"""
        self.total_mismatch_count = total_mismatch_count

    def set_no_of_records_source(self, no_of_records_source):
        """set number of source records"""
        self.no_of_records_source = no_of_records_source

    def set_no_of_records_target(self, no_of_records_target):
        """set number of target records"""
        self.no_of_records_target = no_of_records_target

    def set_mismatch_dict(self, mismatch_dict):
        """set mismatch column dict"""
        self.mismatch_dict = mismatch_dict

    def set_column_wise_additional_in_source_dict(self, column_wise_additional_in_source_dict):
        """set only source columns"""
        self.column_wise_additional_in_source_dict = column_wise_additional_in_source_dict

    def set_column_wise_additional_in_target_dict(self, column_wise_additional_in_target_dict):
        """set only target columns"""
        self.column_wise_additional_in_target_dict = column_wise_additional_in_target_dict

    def set_column_wise_mismatch(self, column_wise_mismatch):
        """set mismatch columns"""
        self.column_wise_mismatch = column_wise_mismatch

    def set_matched_file(self, matched_file):
        """set matched file path"""
        self.matched_file = matched_file

    def set_mismatch_file(self, mismatch_file):
        """set mismatched file path"""
        self.mismatch_file = mismatch_file

    def get_instantiated_instance(self, total_count,
                                  column_wise_mismatch_dict,
                                  matched_csv,
                                  mismatched_excel,
                                  no_of_records_source,
                                  no_of_records_target):
        """Get report instance"""

        self.set_no_of_records_source(no_of_records_source)
        self.set_no_of_records_target(no_of_records_target)
        self.set_total_mismatch_count(total_count)
        self.set_column_wise_mismatch(column_wise_mismatch_dict)
        self.set_matched_file(matched_csv)
        self.set_mismatch_file(mismatched_excel)

    def get_json_representaion(self):
        """get json"""
        return self.__dict__
