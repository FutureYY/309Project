# from pyspark.sql import SparkSession
# from kedro.framework.hooks import hook_impl

# class SparkHooks:
#     @hook_impl
#     def after_context_created(self, context):
#         self.spark = SparkSession.builder \
#             .appName("kedro-pyspark") \
#             .getOrCreate()
#         print("✅ SparkSession created.")
        
        
# from kedro.framework.hooks import hook_impl
# from pyspark.sql import SparkSession

# class ProjectHooks:
#     spark = None

#     @hook_impl
#     def before_pipeline_run(self, run_params):
#         if ProjectHooks.spark is None:
#             ProjectHooks.spark = (
#                 SparkSession.builder
#                 .appName("KedroFeatureEngineering")
#                 .getOrCreate()
#             )
#             print("✅ SparkSession started")

#     @hook_impl
#     def after_pipeline_run(self, run_params, pipeline_output, session):
#         if ProjectHooks.spark:
#             ProjectHooks.spark.stop()
#             ProjectHooks.spark = None
#             print("🛑 SparkSession stopped")


from kedro.framework.hooks import hook_impl
from pyspark.sql import SparkSession
from pyspark import SparkConf

# class SparkHooks:
#     @hook_impl
#     def after_context_created(self, context) -> None:
#         """Initializes a SparkSession using the config defined in project's conf folder."""
        
#         # Load Spark configuration from your project's conf/
#         spark_config = context.config_loader.get("spark*")
        
#         # Create Spark configuration
#         conf = SparkConf().setAll(spark_config.items())
        
#         # Initialize Spark Session
#         spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
        
#         # Make SparkSession available throughout your project
#         context._update_params({"spark": spark_session})\
    
    
class SparkHooks:
    @hook_impl
    def after_context_created(self, context) -> None:
        """Initialize SparkSession with fallback defaults"""
        try:
            spark_config = context.config_loader.get("spark*") or {}
        except Exception:
            spark_config = {}
        
        conf = SparkConf().setAll([
            ("spark.master", "local[*]"),  # Default fallback
            ("spark.app.name", "KedroSparkProject"),  # Default fallback
            *spark_config.items()  # Will be empty if config not found
        ])
        
        spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
        context._update_params({"spark": spark_session})