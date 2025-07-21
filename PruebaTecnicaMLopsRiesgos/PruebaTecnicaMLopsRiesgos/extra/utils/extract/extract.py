from ..interfaces_strategies.extract_strategy_interface import ExtractStrategy


class Extractor(ExtractStrategy):
    # Se lee en el local por falta de cuenta de aws, idealmente seria leer el archivo desde un bucket con archivos raw
    def extract(self, spark, path, name_file, encoding):
        return spark.read.csv(
            f"{path}/{name_file}",
            header=True,
            inferSchema=True,
            sep=",",
            dateFormat="yyyy-MM-dd'T'HH:mm:ss'Z'",
            nullValue="",
            multiLine=True,
            quote='"',
            escape='"',
        ).drop("Unnamed: 0")
