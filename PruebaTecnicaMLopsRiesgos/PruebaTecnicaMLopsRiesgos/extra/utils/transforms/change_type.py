from ..interfaces_strategies.transform_strategy_interface import TransformStrategy
from pyspark.sql.functions import col


class ChangeType(TransformStrategy):
    def __init__(self) -> None:
        super().__init__()

    def transform(self, df, spark=None, **kwargs):
        return df.withColumn("text", col("Title").cast("string"))
