import pandas as pd
import requests
from pandas.core.frame import DataFrame


class Extraction:
    def extract_to_dataframe(self, request_url: str) -> DataFrame:
        res = self.__request_sample_data(request_url)

        return pd.DataFrame.from_dict(res)

    def __request_sample_data(self, request_url: str) -> list:
        res = requests.get(request_url)

        return [r for r in res.json()["records"]]
