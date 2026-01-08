
from pyspark.sql import SparkSession
from pyspark.sql.functions import col , from_json 
from pyspark.sql.types import StructType , StructField, StringType , IntegerType , DoubleType

Bootstrap= "kafka1:9092"
main_topic= "datalake"
DLQ_topic="datalakedlq"

def consumer_spark() :
    spark =SparkSession.builder \
    .appName("data_lake") \
    .master("local[*]") \
    .config (
          "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.apache.commons:commons-pool2:2.11.1,"
            "org.slf4j:slf4j-api:1.7.36"
    ) \
    .getOrCreate()
    main_schema =StructType([
         StructField("message_id",StringType()),
         StructField("name",StringType()),
         StructField("age",IntegerType()),
         StructField("gender",StringType()),
         StructField("country",StringType()),
         StructField("email",StringType()),
         StructField("time_msg",DoubleType())

    ]    
    )
    
    df= (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers",Bootstrap)
           .option("subscribe",f"{main_topic},{DLQ_topic}")
           .option("startingOffsets","earliest")
           .option("maxOffsetsPerTrigger",1000)
           .load()
           
        
    )
      
    json_df=df.select(
        col("topic"),
        col("value").cast("string").alias("value_str") )
    
    main_df=json_df.filter(col("topic")==main_topic)\
    .select(from_json(col("value_str"),main_schema).alias("data"))\
    .select("data.*")

    dlq_df=json_df.filter(col("topic")==DLQ_topic) \
    .select((col("value_str")).alias("dlq_message"))
  
  
    main_result = "hdfs:///datalake/main_parquet"
    dlq_result = "hdfs:///datalake/dlq_parquet"

    main_ckpt = "hdfs:///datalake/_checkpoints/main"
    dlq_ckpt  = "hdfs:///datalake/_checkpoints/dlq"

    q1=( main_df.writeStream
       .format("parquet")
       .outputMode("append")
       .option("path",main_result)
       .option("checkpointLocation",main_ckpt)
       .start()
       
   )
    q2=(dlq_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path",dlq_result)
        .option("checkpointLocation",dlq_ckpt)
        .start()
        
    ) 
       
    spark.streams.awaitAnyTermination()
if __name__ == "__main__":    
   
 consumer_spark() 

    