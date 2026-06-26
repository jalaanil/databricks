
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from delta import configure_spark_with_delta_pip

import os
from dotenv import load_dotenv
load_dotenv()

root_path = os.getenv("ROOT_PATH")
print("***** root is " , root_path)
table_name = 'emp'
ckpt_path = f'{root_path}/DLake/checkpoints/{table_name}_sl'

def get_spark_session_with_delta():

    builder = (
        SparkSession.builder
        .appName("emp_sl")
        .config( "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config( "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog" )
        .config( "spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0" )                          
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

spark = get_spark_session_with_delta()
print("************************************")
print("Spark session created with Delta Lake support.")
print("************************************")


def read_deltalake_stream():
    src = f'{root_path}/DLake/tables/{table_name}_bz'
    stream_df = (
        spark.readStream
        .format("delta")
        .load(src)
        .filter( col("deptno").isNotNull() & (trim(col("deptno")) != "") )        
        .selectExpr("id", "name", "deptno")
    )
    return stream_df

def write_batch_stream_console(stream_df):
    query = stream_df.writeStream \
        .format("console") \
        .option("checkpointLocation", f"{ckpt_path}") \
        .outputMode("append") \
        .start() 
        
    query.awaitTermination()

def write_deltalake_batch(stream_df):
    target = f'{root_path}/DLake/tables/{table_name}_sl'
    query = (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option( "checkpointLocation", f"{ckpt_path}" )    
        .start(target)
    )
    print("records processed in streaming mode. Press Ctrl+C to stop the stream.")
    query.awaitTermination()

stream_df = read_deltalake_stream()
write_deltalake_batch(stream_df)
# write_batch_stream_console(stream_df)

