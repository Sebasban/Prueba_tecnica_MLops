from ..interfaces_strategies.transform_strategy_interface import TransformStrategy
from pyspark.sql.functions import col, translate
import string


class PunctuationRemove(TransformStrategy):
    def __init__(self) -> None:
        super().__init__()

    def transform(self, df, spark=None):
        punc_to_remove = string.punctuation.replace(".", "")
        return df.withColumn(
            "punc_remove", translate(col("text_lower"), punc_to_remove, "")
        )
