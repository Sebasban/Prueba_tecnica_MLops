from pyspark.sql import SparkSession
from typing import Optional


class Spark:
    def __init__(
        self,
        app_name: str = "DefaultApp",
        master: Optional[str] = None,
        configs: Optional[dict] = None,
    ):
        self.app_name = app_name
        self.master = master
        self.configs = configs or {}

    def get_session(self) -> SparkSession:
        builder = SparkSession.builder.appName(self.app_name)

        if self.master:
            builder = builder.master(self.master)

        for key, value in self.configs.items():
            builder = builder.config(key, value)

        return builder.getOrCreate()
