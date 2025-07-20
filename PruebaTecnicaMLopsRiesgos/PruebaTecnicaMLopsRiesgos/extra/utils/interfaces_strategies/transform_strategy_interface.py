from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class TransformStrategy(ABC):
    @abstractmethod
    def transform(self, df: DataFrame, spark: object):
        pass
