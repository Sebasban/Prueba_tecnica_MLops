from ..interfaces_strategies.transform_strategy_interface import TransformStrategy
from pyspark.sql import functions as F


class LanguajesRemove(TransformStrategy):
    def __init__(self) -> None:
        super().__init__()

    def transform(self, df, spark=None):
        permitidos = r"^[a-zA-ZáéíóúüñÁÉÍÓÚÜÑ\s¡!¿?,.;:()'\"-]+$"
        return df.filter(~F.col("text_without_number").rlike(permitidos))
