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
    if '0. ' in file_name:
        df = pd.read_csv(file_path + file_name, dtype=object)
    else:
        df = pd.read_csv(file_path + file_name)
    df.rename(columns=lambda x: re.sub('properties.', '', x), inplace=True)
    return df


def clean_table_0():
    '''
    Cleans mainStopDateTime
    '''
    df_main = load_process_tables('0. mainStopDateTime.csv')
    print('Table 0 loaded...')
    df_main['stopDate'] = pd.to_datetime(df_main['stopDate']).dt.date
    df_main['stopTime'] = pd.to_datetime(df_main['stopTime']).dt.time
    cols_to_drop = ['highwayExit','isSchool', 'schoolName', 'isStudent']
    df_main.drop(cols_to_drop, axis=1, inplace=True)
    df_main.to_csv(out_file_path + 'PROCESSED_0_main.csv', index=False)
    print('Table 0 processed\n')



def clean_table_1():
    '''
    Cleans perceivedRace table
    '''
    df_race = load_process_tables('1. perceivedRace.csv')
    print('Table 1 loaded...')
    df_race = pd.get_dummies(df_race).groupby(['StopID', 'PID']).sum()
    df_race.rename(columns= lambda x: re.sub('perceivedRace_', 'perceived_', x))
    df_race.to_csv(out_file_path + 'PROCESSED_1_race.csv')
    print('Table 1 processed\n')


def clean_table_2():
    '''
    Cleans perceived disability table
    '''
    disability_df = load_process_tables('2. perceivedKnownDisability.csv')
    print('Table 2 loaded...')
    disability_df.rename(columns= lambda x: re.sub('perceivedOrKnown', '', x), inplace=True)
    disability_df = pd.get_dummies(disability_df)
    disability_df.drop('Disability_None', axis=1, inplace=True)
    disability_df.rename(columns=lambda x: re.sub('Disability_', 'DA_', x), inplace=True)
    disability_df.to_csv(out_file_path + 'PROCESSED_2_disability.csv')
    print('Table 2 processed\n')


def clean_table_3():
    '''
    Cleans reason for stop data
    '''
    rfs_df = load_process_tables('3. reasonForStop.csv')
    print('Table 3 loaded...')
    rfs_df = pd.get_dummies(rfs_df.drop(['ObjectId','reasonForStopcode', 'reasonForStopCodeText'], axis=1)).groupby(['StopID','PID']).sum()
    rfs_df.rename(columns=lambda x: re.sub('reasonForStop_', 'RFS_', x), inplace=True)
    rfs_df.rename(columns=lambda x: re.sub('reasonForStopDetail_', 'RFS_Detail_', x), inplace=True)
    rfs_df.to_csv(out_file_path + 'PROCESSED_3_reasonForStop.csv')
    print('Table 3 processed\n')


def clean_table_4():
    '''
    Cleans actions during stop
    '''
    ads_df = load_process_tables('4. actionsDuringStop.csv')
    print('Table 4 loaded...')
    
    # First half
    ads_df1 = pd.get_dummies(ads_df[['StopID', 'PID', 'actionTakenDuringStop']]).groupby(
        ['StopID', 'PID']
    ).sum().reset_index()
    ads_df1.rename(columns= lambda x: re.sub('actionTakenDuringStop_', 'Action_', x), inplace=True)
    ads_df1.drop('Action_None', axis=1, inplace=True)

    # Non-dummy columns to keep
    ads_df2 = ads_df.drop(['ObjectId', 'actionTakenDuringStop'], axis=1).drop_duplicates()
    ads_df2['personSearchConsentGiven'] = ads_df2['personSearchConsentGiven'].apply(float)
    ads_df2['propertySearchConsentGiven'] = ads_df2['propertySearchConsentGiven'].apply(float)
    ads_df2 = ads_df2.goupby(['StopID', 'PID']).sum().reset_index()
    ads_df2['personSearchConsentGiven'] = ads_df2['personSearchConsentGiven'].apply(int)
    ads_df2['propertySearchConsentGiven'] = ads_df2['propertySearchConsentGiven'].apply(int)

    final_ads_df = ads_df1.merge(ads_df2, on=['StopID', 'PID'])
    final_ads_df.to_csv(out_file_path + 'PROCESSED_4_actionDurintStop.csv')
    print('Table 4 processed\n')


if __name__ == '__main__':
    clean_table_0()
    clean_table_1()
    clean_table_2()
    clean_table_3()
    print('='*30)
    print('All tables processed')

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
    
    