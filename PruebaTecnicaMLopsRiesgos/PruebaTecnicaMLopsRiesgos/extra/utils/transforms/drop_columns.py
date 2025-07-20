from ..interfaces_strategies.transform_strategy_interface import TransformStrategy
from pyspark.sql.functions import col


class DropColumns(TransformStrategy):
    def __init__(self) -> None:
        super().__init__()

    def transform(self, df, spark=None):
        return df.drop("Body", "Title", "ClosedDate")
