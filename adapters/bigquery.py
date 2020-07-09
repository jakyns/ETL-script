import os

import pandas_gbq as pdgbq
from pandas.core.frame import DataFrame


class BigQuery:
    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.dataset = os.getenv("BIGQUERY_DATASET_NAME")

    def pandas_to_bigquery(self, table: str, df: DataFrame) -> DataFrame:
        pdgbq.to_gbq(
            df,
            f"{self.dataset}.{table}",
            project_id=self.project_id,
            if_exists="append",
        )

        return df
