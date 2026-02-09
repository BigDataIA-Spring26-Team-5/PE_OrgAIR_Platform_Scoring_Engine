# app/glassdoor_collector.py
import requests
class GlassdoorCollector:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.glassdoor.com/api/api.htm"

    def collect_data(self, query_params):
        headers = {
            'Authorization': f'Bearer {self.api_key}'
        }
        response = requests.get(self.base_url, headers=headers, params=query_params)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status() 
