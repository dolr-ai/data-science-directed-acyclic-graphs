from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HelloWorldPySpark-Changed").getOrCreate()
df = spark.createDataFrame([("Hello",), ("World",)], ["message"])
df.show()
spark.stop()