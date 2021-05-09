'''
For processing tables and merging dataframes
'''
from os import path
import pandas as pd
import re
from api_connect import url_dict

out_file_path = './processed_data/'


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

    
def load_process_tables(file_name, file_path='./raw_data/'):
    ''' Reads tables and performs initial column sweep 
        Also removes 'properties.' quality from column
    '''
    df = pd.read_csv(file_path + file_name, dtype=object)
    df.rename(columns=lambda x: re.sub('properties.', '', x), inplace=True)
    return df


def clean_table_0():
    '''
    Cleans mainStopDateTime
    '''
    df_main = load_process_tables('0. mainStopDateTime.csv')
    df_main['stopDate'] = pd.to_datetime(df_main['stopDate']).dt.date
    df_main['stopTime'] = pd.to_datetime(df_main['stopTime']).dt.time
    cols_to_drop = ['highwayExit','isSchool', 'schoolName', 'isStudent']
    df_main.drop(cols_to_drop, axis=1, inplace=True)
    df_main.to_csv(out_file_path + 'PROCESSED_0_main.csv', index=False)


def clean_table_1():
    '''
    Cleans perceivedRace table
    '''
    df_race = load_process_tables('1. perceivedRace.csv')
    df_race = pd.get_dummies(df_race).groupby(['StopID', 'PID']).sum()
    df_race.rename(columns= lambda x: re.sub('perceivedRace_', 'perceived_', x))
    df_race.to_csv(out_file_path + 'PROCESSED_1_race.csv')


def clean_table_2():
    '''
    Cleans perceived disability table
    '''
    disability_df = load_process_tables('2. perceivedKnownDisability.csv')
    disability_df.rename(columns= lambda x: re.sub('perceivedOrKnown', '', x), inplace=True)
    disability_df = pd.get_dummies(disability_df)
    disability_df.drop('Disability_None', axis=1, inplace=True)
    disability_df.rename(columns=lambda x: re.sub('Disability_', 'DA_', x), inplace=True)
    disability_df.to_csv(out_file_path + 'PROCESSED_1_disability.csv')


def clean_table_3():
    '''
    Cleans reason for stop data
    '''

if __name__ == '__main__':
    pass

### CREATES MAIN DF BELOW ###

# def create_main_df(file_names, file_path='./raw_data/'):

#     df = clean_columns(pd.read_csv(file_path + file_names[0], dtype=object)) # specified dtype as object so df's merge properly
#     print('Main df has shape: ', len(df)) # 40523 unique police stops

#     for file in file_names[1:6]:
#         df2 = clean_columns(pd.read_csv(file_path + file, dtype=object))
#         df = df.merge(df2, on=['StopID', 'PID'])

#     print(df.head(2))
#     print(df.info())
#     return df

# def df_to_csv(df, file_path='./processed_data/'):
#     df.to_csv(file_path + 'PROCESSED_0_to_5.csv', index=False)


# if __name__ == '__main__':
#     file_names = file_name_existence(url_dict)
#     df = create_main_df(file_names)
#     df_to_csv((df))
    
    