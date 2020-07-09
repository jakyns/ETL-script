import pandas as pd
from pandas.core.frame import DataFrame

from services.transformer.timestamp import Timestamp as TimestampTransformer


class Transformation:
    def transform(self, df: DataFrame) -> DataFrame:
        tdf = df.copy()

        tdf = TimestampTransformer().transform_rec_time_to_timestamp(tdf)
        tdf = self.__transform_df(tdf)

        return tdf

    # private

    def __transform_df(self, df: DataFrame) -> DataFrame:
        fdf = pd.DataFrame()

        fdf["dataset_id"] = df["datasetid"]
        fdf["record_id"] = df["recordid"]
        fdf["timestamp"] = df["timestamp"]

        return fdf
