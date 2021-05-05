# File will connect to LB gov API for policing stop data

import urllib3
from urllib3 import request
import requests
import json
import pandas as pd

http = urllib3.PoolManager()
# api_url = 'https://services6.arcgis.com/yCArG7wGXGyWLqav/arcgis/rest/services/Stop_Date_Time_Location_Other/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
api_url = 'https://opendata.arcgis.com/datasets/3d57257946ab46908440f0daa134043c_0.geojson' # geojson / open data link
response = requests.get(api_url)
# print(response.status_code)
# res_json = json.dumps(response.json(), sort_keys=True, indent=5)

df = pd.json_normalize(response.json(), record_path=['features'])
print(df.shape)
print(df.info())
print(df.head())