from datetime import datetime
from typing import Callable, Union

import pandas as pd
from pandas.core.frame import DataFrame


class Timestamp:
    WHITELISTED_COLUMNS = ["record_timestamp"]

    def transform_rec_time_to_timestamp(self, df: DataFrame) -> DataFrame:
        for col in self.WHITELISTED_COLUMNS:
            if col in df.columns:
                df["timestamp"] = self.__apply_to_df(
                    df, col, self.__convert_to_timestamp
                )

        return df

    # private

    def __apply_to_df(self, df: DataFrame, col: str, func: Callable) -> DataFrame:
        return df.apply(lambda r: func(r[col]), axis=1)

    def __convert_to_timestamp(
        self, rec_timestamp: str
    ) -> Union[None, datetime.timestamp]:
        if isinstance(rec_timestamp, str):
            timestamp = datetime.strptime(rec_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")
            timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")

            return pd.Timestamp(timestamp)
        else:
            return None
