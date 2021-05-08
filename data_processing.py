'''
For processing tables and merging dataframes
'''
from os import path
import pandas as pd
import re
from api_connect import url_dict


def file_name_existence(url_dict):
    '''
    Create file names and check existence
    '''
    # create file names
    file_names = [f'{num}. {name}.csv' for num, name in enumerate(url_dict.keys())] 

    for file in file_names:
        result = path.exists(f'./raw_data/{file}')
        if result == False:
            raise Exception(f'{file} does not exist!')

        # print(file + ' exists: ' + str(result))
    return file_names

def clean_columns(df):
    ''' Removes 'properties.' quality from each column '''
    df = df.rename(columns=lambda x: re.sub('properties.', '', x))
    return df


def create_main_df(file_names, file_path='./raw_data/'):

    df = clean_columns(pd.read_csv(file_path + file_names[0], dtype=object)) # specified dtype as object so df's merge properly
    print('Main df has shape: ', len(df)) # 40523 unique police stops

    for file in file_names[1:6]:
        df2 = clean_columns(pd.read_csv(file_path + file, dtype=object))
        df = df.merge(df2, on=['StopID', 'PID'])

    print(df.head(2))
    print(df.info())
    return df

def df_to_csv(df, file_path='./processed_data/'):
    df.to_csv(file_path + 'PROCESSED_0_to_5.csv', index=False)


if __name__ == '__main__':
    file_names = file_name_existence(url_dict)
    df = create_main_df(file_names)
    df_to_csv((df))
    
    