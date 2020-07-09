from typing import Any

from pandas.core.frame import DataFrame

from adapters.bigquery import BigQuery


class Loader:
    def load(self, df: DataFrame) -> DataFrame:
        self.__load_to_bigquery(df)

        return df

    # private

    def __load_to_bigquery(self, df: DataFrame) -> None:
        bq = BigQuery()

        self.__save_to_log_entry(bq, df)

    def __save_to_log_entry(self, adapter: Any, df: DataFrame) -> None:
        table = "log_entries"

        adapter.pandas_to_bigquery(table, df)
