#####
# Runs API and data processing modules
#####

from api_connect import url_dict, callAPI
import data_processing as dp

if __name__ == "__main__":
    callAPI(url_dict) # connects to API
    dp.main() # processes raw tables