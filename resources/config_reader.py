class config_reader:

    def getSparkConfig():
        spark_config ={
            "spark.sql.autoBroadcastJoinThreshold":"-1",
            "spark.port.maxRetries":"100",
            "spark.sql.sources.partitionColumnTypeInference.enabled":"false",
            "spark.shuffle.partitions":"10",
            "spark.sparkContext.setLogLevel":"ERROR"
        }
        return spark_config
