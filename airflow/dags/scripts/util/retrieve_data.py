import requests


class DataRetrieverOverAPI:
    def __init__(self, api_url):
        self._data = requests.get(api_url).json()

    def print_data(self):
        print(self._data)

    @property
    def data(self):
        
        return self._data