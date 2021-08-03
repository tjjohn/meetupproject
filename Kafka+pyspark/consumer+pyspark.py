
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, BooleanType, LongType, IntegerType, StringType
import time



kafka_topic_name = "wiki-changes"
kafka_bootstrap_servers = 'localhost:9092'




if __name__ == "__main__":
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    # sessiom for specfic users
    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .config("spark.jars", "file:///C://spark_dependency_jars//commons-pool2-2.8.1.jar,file:///C://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar,file:///C://spark_dependency_jars//kafka-clients-2.6.0.jar,file:///C://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .config("spark.executor.extraClassPath","file:///C://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///C://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///C://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///C://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .config("spark.executor.extraLibrary","file:///C://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///C://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///C://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///C://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .config("spark.driver.extraClassPath", "file:///C://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///C://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///C://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///C://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from topic ie input
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()
            # starting offset means latest messages

    df.printSchema()  # consists of key, value,offset,topic           and  value is the actual message



    #transforming ie spark engine

    df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS string)")



    new_schema = StructType([
        StructField("venue", StringType()),
        StructField("visibility", StringType())
    ])


    df2 = (df1
           # Sets schema for event data
           .withColumn("value", from_json("value", new_schema))
           .withColumn("key", from_json("key", new_schema))
           )



    #sinking

    stream1=df2 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
    # Note that you have to call start() to actually start the execution of the query
    #mongodb
    #streaming------
    stream1.awaitTermination()

    print("Stream Data Processing Application Completed.")