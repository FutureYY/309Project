from kedro.framework.hooks import _create_hook_manager
from kedro.framework.project import settings
# from kedro.extras.datasets.spark import SparkDataSet
from kedro.config import ConfigLoader
from pathlib import Path
from pyspark.sql import SparkSession
from kedro.framework.hooks import hook_impl

class SparkHooks:
    @hook_impl
    def register_pyspark_session(self):
        return SparkSession.builder \
            .appName("kedro-spark-app") \
            .getOrCreate()

HOOKS = (SparkHooks(),)
