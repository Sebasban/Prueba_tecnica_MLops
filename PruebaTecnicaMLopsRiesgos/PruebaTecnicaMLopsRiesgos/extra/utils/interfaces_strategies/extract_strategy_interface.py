from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class ExtractStrategy(ABC):
    @abstractmethod
    def extract(
        self, spark: object, path: str, name_file: str, encoding: str
    ) -> DataFrame:
        pass
