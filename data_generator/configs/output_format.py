'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

INP_FORMAT = {
    "no_of_records_to_generate": 100,
    "method": "faker/random",
    "format_to_save": "<FORMAT_TO_SAVE> #json,csv,txt,avro,parquet,binary,excel,html,pdf,xml",
    "data": [
        {
            "key": "KEY1",
            "type_of_value": "id,date,time,float,int,string,percentage,boolean,name,choice,word",
            "range_values": "None/{'min': '','max': ''}/[]"
        },
        {
            "key": "KEY2",
            "type_of_value": "name",
            "range_values": "None"
        },
        {
            "key": "KEY2",
            "type_of_value": "name",
            "range_values": {
                "min": "",
                "max": ""
            }
        },

    ]
}
