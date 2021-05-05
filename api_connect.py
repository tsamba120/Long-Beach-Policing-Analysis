'''
Connect to Long Beach, CA's OpenData API through ArcGIS
'''

import requests
import json
import pandas as pd

url_dict = {
    'mainStopDateTime': 'https://opendata.arcgis.com/datasets/3d57257946ab46908440f0daa134043c_0.geojson',
    'perceivedRace': 'https://opendata.arcgis.com/datasets/a50a4c8861cb4c1a99fdb068965d8488_0.geojson',
    'perceivedKnownDisability': 'https://opendata.arcgis.com/datasets/cd6179a9d2fa44bbb33fcd2337190fdc_0.geojson',
    'reasonForStop':'https://opendata.arcgis.com/datasets/7a06d90ad71e4342961b0252d7814b14_0.geojson',
    'actionsDuringStop': 'https://opendata.arcgis.com/datasets/f3542945e81d43ee96fa9b8a77582510_0.geojson',
    'resultOfStop': 'https://opendata.arcgis.com/datasets/9a36bd8c5c1c46468757c67a3311710a_0.geojson',
    'basisForSearch': 'https://opendata.arcgis.com/datasets/93bc69329e364f64ac4b7c0feef1e22f_0.geojson',
    'basisForPropertySeizure': 'https://opendata.arcgis.com/datasets/4d1e3fb4f8d1478e8984877d542258b0_0.geojson',
    'contrabandEvidenceDiscovered': 'https://opendata.arcgis.com/datasets/425d4f0f345e4aa0bebf8de941846c1e_0.geojson'
}

def callAPI(url_dict) -> None:
    '''
    Loop through REST API urls (in url_dict) and load to tables/save to csv's for staging
    '''
    for num, key in enumerate(url_dict):
        print(f'Processing table: "{key}"')
        response = requests.get(url_dict[key]).json()
        df = pd.json_normalize(response, record_path=['features'])
        df.drop(['type', 'geometry'], axis=1, inplace=True)
        df.to_csv(f'raw_data/{num}. {key}.csv', index=False)
        print(f'"{key}" saved to csv!\n')
        print('='*20, '\n')
    print('API request complete!')
    print(f'{len(url_dict)} tables loaded to staging directory.')

# drop 'type' and 'geometry' columns!

if __name__ == '__main__':
    callAPI(url_dict)