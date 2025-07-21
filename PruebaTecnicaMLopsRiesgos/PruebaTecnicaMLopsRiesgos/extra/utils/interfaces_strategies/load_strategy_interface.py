from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class LoadStrategy(ABC):
    @abstractmethod
    def load(self, df: DataFrame, format: str, mode: str, path: str, name_file: str):
        pass
