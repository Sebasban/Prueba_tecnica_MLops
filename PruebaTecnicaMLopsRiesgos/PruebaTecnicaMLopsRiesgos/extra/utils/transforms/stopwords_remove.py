from ..interfaces_strategies.transform_strategy_interface import TransformStrategy
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import nltk


class StopwordsrRemove(TransformStrategy):
    def __init__(self) -> None:
        super().__init__()
        nltk.download("stopwords", quiet=True)
        self.stopwords_set = set(nltk.corpus.stopwords.words("english"))

    @staticmethod
    def _clean_text(text, broadcast_stopwords):
        if text is None:
            return None
        words = str(text).split()
        filtered = [w for w in words if w.lower() not in broadcast_stopwords.value]
        return " ".join(filtered)

    def transform(self, df, spark, **kwargs):
        broadcast_stopwords = spark.sparkContext.broadcast(self.stopwords_set)
        remove_sw_udf = udf(
            lambda text: StopwordsrRemove._clean_text(text, broadcast_stopwords),
            StringType(),
        )
        return df.withColumn("stopwords_remove", remove_sw_udf(col("punc_remove")))
