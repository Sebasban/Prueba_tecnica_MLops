from ..interfaces_strategies.transform_strategy_interface import TransformStrategy
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


class FreqwordsRemove(TransformStrategy):

    def __init__(self) -> None:
        super().__init__()

    def transform(self, df: DataFrame, spark, **kwargs):
        words_df = df.select(
            F.explode(F.split(F.col("stopwords_remove"), r"\s+")).alias("word")
        )
        freq_df = words_df.groupBy("word").count().orderBy(F.col("count").desc())
        top10 = [row["word"] for row in freq_df.limit(10).collect()]

        freq_bcast = spark.sparkContext.broadcast(set(top10))

        @F.udf(returnType=StringType())
        def _remove_freqwords(text: str) -> str:
            if not text:
                return text
            return " ".join(w for w in text.split() if w not in freq_bcast.value)

        return df.withColumn(
            "freq_word_remove", _remove_freqwords(F.col("stopwords_remove"))
        )
