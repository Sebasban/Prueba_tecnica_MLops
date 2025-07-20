from ..interfaces_strategies.transform_strategy_interface import TransformStrategy
from pyspark.sql import functions as F


class SpacesShortWordsrRemove(TransformStrategy):
    def __init__(self) -> None:
        super().__init__()

    def transform(self, df, spark=None):
        df_clean = df.withColumn(
            "text_clean",
            F.trim(F.regexp_replace(F.col("text_without_number"), r"\s+", " ")),
        )
        df_clean = df_clean.filter(F.length(F.col("text_clean")) > 0)
        df_clean = df_clean.filter(F.size(F.split(F.col("text_clean"), r"\s+")) >= 3)
        return df_clean
