import logging.config
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructField, IntegerType, StringType,StructType
from cassandra.cluster import Cluster
import logging

logging.basicConfig(filename="log.log",
                    filemode="w")




def connect_to_spark():
    try:
        spark_session = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,"
                                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
            
        return spark_session
        
    except Exception as e:
        print(e)


    



def read_from_kafka(spark_session):
    spark_df=spark_session.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', 'localhost:9092') \
                .option('subscribe', 'stream') \
                .option('startingOffsets', 'earliest') \
                .load()
                
    return spark_df
    

                
def convert_df(spark_df):       
    schema=StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("email", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True)
    ])
    
    

    df = spark_df.selectExpr("CAST(value AS STRING)")\
        .alias("value")\
        .select(from_json(col("value"), schema).alias("data"))\
            .select("data.*")
        
    
    return df



def connect_to_cassendra():
    cluster =Cluster(['localhost'])


    connection=cluster.connect()
    
    return connection


def create_key_space(connection):
    query="""
        CREATE KEYSPACE IF NOT EXISTS users
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """
    
    connection.execute(query)
    
    
    
def create_table(connection):
    query=f"""
        CREATE TABLE IF NOT EXISTS users.users (
            id UUID PRIMARY KEY,
            name TEXT,
            city TEXT,
            country TEXT,
            email TEXT,
            dob TEXT,
            age TEXT,
            phone TEXT,
            picture TEXT
            );
    """
    
    connection.execute(query)
    
    
    
def insert_data(connection, **kwargs):
    
    id = kwargs.get("id")
    name = kwargs.get("name")
    city = kwargs.get("city")
    country =  kwargs.get("country")
    email = kwargs.get("email")
    dob = kwargs.get("dob")
    age = kwargs.get("age") 
    phone = kwargs.get("phone")
    picture = kwargs.get("picture")
    
    try:
    
        query="""
            INSERT INTO users.users (id, name, city, country, email, dob, age, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,(id, name, city, country, email, dob, age, phone, picture)
        
        connection.execute(query)
        logging.log(level=1, msg=f"Inser data {kwargs}")
    except Exception as e:
        print(e)
    
    
    
if __name__ == "__main__":
    
    spark_session=connect_to_spark()
    df=read_from_kafka(spark_session)
    df=convert_df(df)
    connection=connect_to_cassendra()
    create_key_space(connection)
    create_table(connection)
    
    
    if connection is not None:
        
        streaming_query=(df.writeStream.format("org.apache.spark.sql.cassandra").option("checkpointLocation", "checkpoint").option("keyspace", "users").option("table", "users").start())
    
        streaming_query.awaitTermination()
    
    