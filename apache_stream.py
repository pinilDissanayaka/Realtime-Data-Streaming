from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructField, IntegerType, StringType



spark_session = SparkSession.builder \
    .appName('SparkDataStreaming') \
    .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config('spark.cassandra.connection.host', 'localhost') \
    .getOrCreate()


print(spark_session)




df=spark_session.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'stream') \
            .option('startingOffsets', 'earliest') \
            .load()
            
            
print(df)