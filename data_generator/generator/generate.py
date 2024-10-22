'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import random
import string
import sys
import uuid
from datetime import datetime, timedelta

from faker import Faker

from data_generator.configs import config
from utils.ServerLogs import logger


class Generate:
    """Class to generate synthetic data"""

    def random_time(self):
        """Function to define random time"""
        return timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        ).seconds

    # Generate data using faker
    def generate_data_faker(self, json_input, n):
        """Function to generate data using Faker"""
        fake = Faker()
        data = {}

        for value_dict in json_input:

            key = value_dict['key']
            range_values = value_dict['range_values']
            type_of_value = value_dict['type_of_value']

            logger.info(f'Generating for key - {key}')

            if type_of_value in config.KEYWORDS:
                if type_of_value.lower() == 'id':
                    data[key] = [fake.uuid4() for _ in range(n)]

                elif type_of_value.lower() == 'date':
                    if str(range_values).lower() == 'none':
                        data[key] = [fake.date_this_decade().strftime("%Y-%m-%d") for _ in range(n)]

                    elif isinstance(range_values, list):
                        range_values = [datetime.strptime(i, "%Y-%m-%d") for i in range_values]
                        data[key] = [fake.random_element(elements=range_values).strftime("%Y-%m-%d") for _ in range(n)]

                    else:
                        min_date = datetime.strptime(range_values['min'], "%Y-%m-%d") if 'min' in range_values else None
                        max_date = datetime.strptime(range_values['max'], "%Y-%m-%d") if 'max' in range_values else None

                        # Handle cases where only one of min or max is given
                        if min_date and not max_date:
                            max_date = datetime.today()
                        elif max_date and not min_date:
                            min_date = datetime.min

                        # Generate n random dates within the range
                        data[key] = [fake.date_between(min_date, max_date).strftime("%Y-%m-%d") for _ in range(n)]

                elif type_of_value.lower() == 'time':
                    if str(range_values).lower() == 'none':
                        data[key] = [fake.time_object().strftime("%H:%M:%S") for _ in range(n)]

                    elif isinstance(range_values, list):
                        range_values = [datetime.strptime(i, "%H:%M:%S") for i in range_values]
                        data[key] = [fake.random_element(elements=range_values).strftime("%H:%M:%S") for _ in range(n)]

                    else:
                        min_date = datetime.strptime(range_values['min'], "%H:%M:%S") if 'min' in range_values else None
                        max_date = datetime.strptime(range_values['max'], "%H:%M:%S") if 'max' in range_values else None

                        # Handle cases where only one of min or max is given
                        if min_date and not max_date:
                            max_date = datetime.today()
                        elif max_date and not min_date:
                            min_date = datetime.min

                        # Generate n random times within the range
                        data[key] = [fake.date_time_between(min_date, max_date).strftime("%H:%M:%S") for _ in range(n)]

                elif type_of_value.lower() == 'float':

                    if isinstance(range_values, list):
                        data[key] = [float(fake.random_element(elements=range_values)) for _ in range(n)]
                    elif isinstance(range_values, dict):
                        if range_values.get('min', None) == '' or range_values.get('min', None) is None:
                            min_val = None
                        else:
                            min_val = int(range_values.get('min', 0))

                        if range_values.get('max', None) == '' or range_values.get('max', None) is None:
                            max_val = None
                        else:
                            max_val = int(range_values.get('max', None))

                        data[key] = [fake.pyfloat(min_value=min_val, max_value=max_val, right_digits=2) for _ in
                                     range(n)]
                    else:
                        data[key] = [fake.pyfloat(min_value=0, right_digits=2) for _ in range(n)]

                elif type_of_value.lower() == 'int':

                    if isinstance(range_values, list):
                        data[key] = [int(fake.random_element(elements=range_values)) for _ in range(n)]
                    elif isinstance(range_values, dict):

                        if (range_values.get('min', None) != '' and range_values.get('min', None) is not None) and \
                                (range_values.get('max', None) != '' and range_values.get('max', None) is not None):

                            min_val = int(range_values.get('min', None))
                            max_val = int(range_values.get('max', None))
                            data[key] = [fake.random_int(min=min_val, max=max_val) for _ in range(n)]

                        elif (range_values.get('min', None) == '' or range_values.get('min', None) is None) and \
                                (range_values.get('max', None) != '' and range_values.get('max', None) is not None):
                            val = int(range_values.get('max', None))
                            data[key] = [fake.random_int(max=val) for _ in range(n)]

                        elif (range_values.get('min', None) != '' and range_values.get('min', None) is not None) and \
                                (range_values.get('max', None) == '' or range_values.get('max', None) is None):
                            val = int(range_values.get('min', 0))
                            data[key] = [fake.random_int(min=val) for _ in range(n)]

                        else:
                            data[key] = [fake.random_int() for _ in range(n)]

                    else:
                        data[key] = [fake.random_int(min=0) for _ in range(n)]

                elif type_of_value.lower() == 'percentage':

                    if isinstance(range_values, list):
                        range_values = [int(i) for i in range_values]
                        if any(i > 100 for i in range_values):
                            data[key] = [fake.random_int(0, 100) for _ in range(n)]
                        else:
                            data[key] = [int(fake.random_element(elements=range_values)) for _ in range(n)]
                    elif isinstance(range_values, dict):
                        min_val = int(range_values.get('min', 0))
                        max_val = int(range_values.get('max', 100))
                        data[key] = [fake.random_int(min=min_val, max=max_val) for _ in range(n)]
                    else:
                        data[key] = [fake.random_int(min=0, max=100) for _ in range(n)]

                elif type_of_value.lower() == 'boolean':
                    if len(range_values) == 2 and str(range_values).lower() != 'none':
                        data[key] = [fake.random_element(elements=range_values) for _ in range(n)]
                    else:
                        data[key] = [fake.random_element(elements=[True, False]) for _ in range(n)]

                elif type_of_value.lower() == 'name':

                    if str(range_values).lower() != 'none':
                        data[key] = [fake.random_element(elements=range_values) for _ in range(n)]
                    else:

                        data[key] = [fake.first_name() for _ in range(n)]

                elif type_of_value.lower() == 'choice' or type_of_value.lower() == 'word':

                    if str(range_values).lower() != 'none':
                        data[key] = [fake.random_element(elements=range_values) for _ in range(n)]
                    else:
                        data[key] = [fake.word() for _ in range(n)]

                elif type_of_value.lower() == 'string':

                    if isinstance(range_values, list):
                        data[key] = [fake.random_element(elements=range_values) for _ in range(n)]
                    elif isinstance(range_values, dict):
                        min_len = int(range_values.get('min', 1))
                        max_len = int(range_values.get('max', 10))
                        data[key] = [fake.pystr(min_chars=min_len, max_chars=max_len) for _ in range(n)]
                    else:
                        data[key] = [fake.pystr() for _ in range(n)]

            else:
                data[key] = [fake.word() for _ in range(n)]

        return data

    def generate_data_random(self, json_input, n):
        """Function to generate data using random"""
        data = {}

        for value_dict in json_input:
            key = value_dict['key']
            range_values = value_dict['range_values']
            type_of_value = value_dict['type_of_value']

            if type_of_value in config.KEYWORDS:
                if type_of_value.lower() == 'id':
                    data[key] = data[key] = [str(uuid.uuid4()) for _ in range(n)]

                elif type_of_value.lower() == 'date':

                    if str(range_values).lower() == 'none':
                        start_date = datetime.today() - timedelta(days=365 * 10)  # Start of this decade
                        data[key] = [(start_date + timedelta(days=random.randint(0, 365 * 10))).strftime("%Y-%m-%d")
                                     for _ in range(n)]

                    elif isinstance(range_values, list):
                        range_values = [datetime.strptime(i, "%Y-%m-%d") for i in range_values]
                        data[key] = [random.choice(range_values).strftime("%Y-%m-%d") for _ in range(n)]


                    else:
                        min_date = datetime.strptime(range_values['min'],
                                                     "%Y-%m-%d") if 'min' in range_values else datetime.min
                        max_date = datetime.strptime(range_values['max'],
                                                     "%Y-%m-%d") if 'max' in range_values else datetime.today()

                        # Calculate the difference in days between min_date and max_date
                        date_diff = (max_date - min_date).days

                        # Generate n random dates within the range
                        data[key] = [(min_date + timedelta(days=random.randint(0, date_diff))).strftime("%Y-%m-%d")
                                     for _ in range(n)]

                elif type_of_value.lower() == 'time':

                    if str(range_values).lower() == 'none':
                        data[key] = [(datetime.min + timedelta(seconds=self.random_time())).time().strftime("%H:%M:%S")
                                     for _ in range(n)]
                    elif isinstance(range_values, list):
                        range_values = [datetime.strptime(i, "%H:%M:%S") for i in range_values]
                        data[key] = [random.choice(range_values).strftime("%H:%M:%S") for _ in range(n)]
                    else:
                        min_time = datetime.strptime(range_values['min'],
                                                     "%H:%M:%S").time() if 'min' in range_values else datetime.min.time()
                        max_time = datetime.strptime(range_values['max'],
                                                     "%H:%M:%S").time() if 'max' in range_values else datetime.max.time()

                        # Calculate the difference in seconds between min_time and max_time
                        time_diff = (datetime.combine(datetime.today(), max_time) - datetime.combine(datetime.today(),
                                                                                                     min_time)).seconds

                        # Generate n random times within the range
                        data[key] = [
                            (datetime.min + timedelta(seconds=(self.random_time() % time_diff))).time().strftime(
                                "%H:%M:%S")
                            for _ in range(n)]

                elif type_of_value.lower() == 'float':
                    if isinstance(range_values, list):
                        data[key] = [random.choice(range_values) for _ in range(n)]
                    elif isinstance(range_values, dict):
                        min_val = range_values.get('min', 0)
                        max_val = range_values.get('max', None)
                        data[key] = [round(random.uniform(min_val, max_val), 2) for _ in range(n)]
                    else:
                        data[key] = [round(random.random(), 2) for _ in range(n)]

                elif type_of_value.lower() == 'int':
                    if isinstance(range_values, list):
                        data[key] = [random.choice(range_values) for _ in range(n)]
                    elif isinstance(range_values, dict):
                        min_val = range_values.get('min', 0)
                        max_val = range_values.get('max', None)
                        data[key] = [random.randint(min_val, max_val) for _ in range(n)]
                    else:
                        data[key] = [random.randint(0, sys.maxsize) for _ in range(n)]

                elif type_of_value.lower() == 'percentage':

                    if isinstance(range_values, list):
                        range_values = [int(i) for i in range_values]
                        if any(i > 100 for i in range_values):
                            data[key] = [random.randint(0, 100) for _ in range(n)]
                        else:
                            data[key] = [random.choice(range_values) for _ in range(n)]
                    elif isinstance(range_values, dict):
                        min_val = int(range_values.get('min', 0))
                        max_val = int(range_values.get('max', 100))
                        data[key] = [random.randint(min_val, max_val) for _ in range(n)]
                    else:
                        data[key] = [random.randint(0, 100) for _ in range(n)]

                elif type_of_value.lower() == 'boolean':

                    if len(range_values) == 2 and str(range_values).lower() != 'none':
                        data[key] = [random.choice(range_values) for _ in range(n)]
                    else:
                        data[key] = [random.choice([True, False]) for _ in range(n)]

                elif type_of_value.lower() == 'name':

                    if str(range_values).lower() != 'none':
                        data[key] = [random.choice(range_values) for _ in range(n)]
                    else:

                        data[key] = [random.choice(config.NAMES) for _ in range(n)]

                elif type_of_value.lower() == 'choice' or type_of_value.lower() == 'word':

                    if str(range_values).lower() != 'none':
                        data[key] = [random.choice(range_values) for _ in range(n)]
                    else:

                        data[key] = [''.join(random.choices(string.ascii_letters + string.digits, k=10)) for _ in
                                     range(n)]

                elif type_of_value.lower() == 'string':

                    if isinstance(range_values, list):
                        data[key] = [random.choice(range_values) for _ in range(n)]
                    elif isinstance(range_values, dict):
                        min_len = range_values.get('min', 1)
                        max_len = range_values.get('max', 10)
                        data[key] = [''.join(
                            random.choices(string.ascii_letters + string.digits, k=random.randint(min_len, max_len)))
                            for _ in range(n)]
                    else:
                        data[key] = [''.join(random.choices(string.ascii_letters + string.digits, k=10)) for _ in
                                     range(n)]

            else:
                data[key] = [''.join(random.choices(string.ascii_letters + string.digits, k=10)) for _ in
                             range(n)]

        return data
