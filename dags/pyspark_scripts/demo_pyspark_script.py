from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HelloWorldPySpark").getOrCreate()
df = spark.createDataFrame([("Hello",), ("World",)], ["message"])
df.show()
spark.stop()