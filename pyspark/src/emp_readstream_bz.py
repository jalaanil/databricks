from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField ,StringType , IntegerType

import os
from dotenv import load_dotenv
load_dotenv()

root_path = os.getenv("ROOT_PATH")
print("***** root is " , root_path)
table_name = 'emp'

def get_spark_session_with_delta():

    builder = (
        SparkSession.builder
        .appName("emp_bz")
        .master("local[*]")                               
        .config( "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config( "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog" )
        .config( "spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0" )           
    )

    spark = configure_spark_with_delta_pip( builder ).getOrCreate()

    return spark

spark = get_spark_session_with_delta()

def get_emp_schema():
    emp_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("deptno", IntegerType(), True)
    ])    

    return emp_schema

def read_file_stream():
    src = f'{root_path}/data/emp/*.csv'
    df = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .schema(get_emp_schema()) \
        .load(src) \
        .selectExpr('id', 'name','cast(deptno as int) as deptno' , "_metadata.*") 
    
    return df
       
ckpt_path = f'{root_path}/DLake/checkpoints/{table_name}_bz'

def write_batch_stream_console(stream_df):
    query = stream_df.writeStream \
        .format("console") \
        .option("checkpointLocation", f"{ckpt_path}") \
        .outputMode("append") \
        .start()
    query.awaitTermination()

def write_batch_stream_to_delta(stream_df):
    save_path = f'{root_path}/DLake/tables/{table_name}_bz'

    query = stream_df.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{ckpt_path}") \
        .outputMode("append") \
        .start( f"{save_path}" )  # Specify the path to save the Delta table


    query.awaitTermination()

stream_df = read_file_stream ()
# write_batch_stream_console (stream_df)
write_batch_stream_to_delta (stream_df)
