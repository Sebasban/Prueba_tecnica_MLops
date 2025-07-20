from ..interfaces_strategies.load_strategy_interface import LoadStrategy


class Loader(LoadStrategy):
    # Se escribe en el local por falta de cuenta de aws, idealmente seria escribir el archivo a un bucket con info curada
    def load(self, df, format, mode, path, name_file):
        df.coalesce(1).write.format(format).option("header", "true").mode(mode).save(
            f"{path}/{name_file}"
        )
