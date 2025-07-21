from ..interfaces_strategies.transform_strategy_interface import TransformStrategy
from pyspark.sql import functions as F


class NumbersRemove(TransformStrategy):
    def __init__(self) -> None:
        super().__init__()

    def transform(self, df, spark=None):
        return df.withColumn(
            "text_without_number", F.regexp_replace(F.col("text_lemmatize"), r"\d+", "")
        )
