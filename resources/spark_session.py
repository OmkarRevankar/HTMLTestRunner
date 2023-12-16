from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


class get_spark:

    def getSparkSession(sparkConfig):
        app_Name = "Spark DataFrame Test Framework"
        conf = SparkConf().setAll(sparkConfig.items())
        spark = SparkSession.builder.appName(app_Name).config(conf=conf).getOrCreate()
        return spark