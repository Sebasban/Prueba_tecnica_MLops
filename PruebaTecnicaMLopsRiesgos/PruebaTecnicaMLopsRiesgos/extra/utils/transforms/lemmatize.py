from ..interfaces_strategies.transform_strategy_interface import TransformStrategy
import nltk
from nltk.stem import WordNetLemmatizer
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

nltk.download("wordnet")


class Lemmatize(TransformStrategy):
    def __init__(self) -> None:
        super().__init__()
        self.lemmatizer = WordNetLemmatizer()

    def transform(self, df, spark=None):
        lemmatizer = self.lemmatizer

        lemmatize_udf = F.udf(
            lambda text: (
                text
                if not text
                else " ".join(lemmatizer.lemmatize(w) for w in text.split())
            ),
            StringType(),
        )

        return df.withColumn("text_lemmatize", lemmatize_udf(F.col("freq_word_remove")))
