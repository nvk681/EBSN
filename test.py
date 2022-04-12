import requests
import time
import json
import sys
import codecs
import csv


class MeetUpScraper:

    api_call_type=""
    config_file="meet_up_config.json"

    def get_results(self,params,config_data):
        try:
            request = requests.get(config_data[self.api_call_type]['api_endpoint'],params=params)
            data = request.json()
            return data
        except:
            print("None")
    
    def main(self,p_config_file):
        cities=[("Seattle","WA")]
        api_key="APIKEY"

        for (city,state) in cities:
            per_page=200
            results_we_got = per_page
            offset=0
            while(results_we_got==per_page):

                response = self.get_results(
                {"sign":"true","country":"US","city":city,"state":state,"radius":10,"key":api_key,"page":per_page,"offset":offset}
                ,p_config_file
                )
                time.sleep(1)
                offset+=1
                data={}
                if response:
                    results_we_got = response['meta']['count']
                    data = response['results']
                    export_file= open("data_"+self.api_call_type+"_"+str(offset)+".txt","w")
                    json.dump(data,export_file)
                    export_file.close()

    def __init__(self,api_call_type):
        self.api_call_type=api_call_type
        config=open(self.config_file)
        config_data=json.load(config)
        self.main(config_data)

MeetUpScraper("get_event") #for testing