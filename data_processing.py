'''
For processing tables and merging dataframes
'''
from os import path
import pandas as pd
from api_connect import url_dict
import unittest

def test_files_exist(url_dict):
    '''
    Create file names and check existence
    '''
    # create file names
    file_names = [f'{num}. {name}.csv' for num, name in enumerate(url_dict.keys())] 

    for file in file_names:
        result = path.exists(f'raw_data/{file}')
        if result == False:
            raise Exception(f'{file} does not exist!')

        # print(file + ' exists: ' + str(result))
    return file_names


if __name__ == '__main__':
    file_names = test_files_exist(url_dict)