from pandas.core.frame import DataFrame

from services.etl.extraction import Extraction
from services.etl.loader import Loader
from services.etl.transformation import Transformation


class ETL:
    def execute(self, request_url: str) -> DataFrame:
        df = self.__extract(request_url)
        transformed_df = self.__transform(df)
        fdf = self.__load(transformed_df)

        return fdf

    # private

    def __extract(self, file_path: str) -> DataFrame:
        extraction = Extraction()
        df = extraction.extract_to_dataframe(file_path)

        return df

    def __transform(self, df: DataFrame) -> DataFrame:
        transformation = Transformation()
        transformer_data = transformation.transform(df)

        return transformer_data

    def __load(self, df: DataFrame) -> DataFrame:
        loader = Loader()
        df = loader.load(df)

        return df


request_url = (
    "https://public-us.opendatasoft.com/api/records/1.0/search/?dataset=fablabs&rows=5"
)

etl = ETL()
etl.execute(request_url)
