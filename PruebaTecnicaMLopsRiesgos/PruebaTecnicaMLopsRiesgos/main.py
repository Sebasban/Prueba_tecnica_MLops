from extra.utils.constants.constants import Constants
from extra.config.spark import Spark
from extra.utils.factory.factory import Factory
from extra.utils.interfaces_strategies.extract_strategy_interface import ExtractStrategy
from extra.utils.interfaces_strategies.transform_strategy_interface import (
    TransformStrategy,
)

# from extra.utils.interfaces_strategies.load_strategy_interface import LoadStrategy
import logging

logger = logging.getLogger(__name__)


def main(**kwargs):

    for name in kwargs["strategies_list"]:
        strategy = Factory.factory(name.strip())
        if isinstance(strategy, ExtractStrategy):
            logger.info("Extracting data...")
            df = strategy.extract(
                kwargs["spark"], kwargs["path"], kwargs["name_file"], "iso-8859-1"
            )
        elif isinstance(strategy, TransformStrategy):
            logger.info("Transforming data...")
            df = strategy.transform(df)
        else:
            logger.info("Loading data...")
            strategy.load(
                df,
                kwargs["format"],
                kwargs["mode"],
                kwargs["path"],
                kwargs["name_file_curated"],
            )


if __name__ == "__main__":
    spark_instance = Spark(
        app_name="NLP", master="local[*]", configs={"spark.executor.memory": "2g"}
    )
    spark_session = spark_instance.get_session()
    params = {
        "spark": spark_session,
        "path": Constants.path.value,
        "name_file": Constants.name_file.value,
        "strategies_list": Constants.strategies_order.value,
        "mode": Constants.mode.value,
        "format": Constants.format.value,
        "name_file_curated": Constants.name_out_file.value,
    }
    main(**params)
