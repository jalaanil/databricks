from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

import os
from dotenv import load_dotenv
load_dotenv()

root_path = os.getenv("ROOT_PATH")
print("***** root is " , root_path)
table_name = 'emp'
ckpt_path = f'{root_path}/DLake/checkpoints/{table_name}_gl'

def get_spark_session_with_delta():

    builder = (
        SparkSession.builder
        .appName("emp-gl")
        .config( "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config( "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog" )
        .config( "spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0" )      
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

spark = get_spark_session_with_delta()
print("************************************")
print("Spark session for GOLD created with Delta Lake support.")
print("************************************")


def read_dept_df():
    src = f'{root_path}/data/dept/*.csv'
    dept_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema("""
            deptno INT,
            dname STRING,
            location STRING
        """)
        .load(src )
    )

    return dept_df

dept_df = read_dept_df()

def read_deltalake_stream():
    src = f'{root_path}/DLake/tables/{table_name}_sl'
    stream_df = (
        spark.readStream
        .format("delta")
        .load(src)
    )
    return stream_df

def process_batch(batch_df, batch_id):
    print(f"Processing batch {batch_id}")

    target = f'{root_path}/DLake/tables/{table_name}_gl'

    # Example transformation
    processed = (
        batch_df
        .filter("id IS NOT NULL")
        .withColumnRenamed("id", "emp_id")
    )

    # Write this batch to another Delta table
    (
        processed.write
        .format("delta")
        .mode("append")
        .save(target) 
    )


def write_deltalake_batch(stream_df):

    df_joined = stream_df.join(dept_df, stream_df.deptno == dept_df.deptno, "left") \
    .select(stream_df["*"], dept_df["dname"], dept_df["location"])
    
    query = (
        df_joined.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{ckpt_path}")     
        .start()
    )
    print("GOLD records being processed in streaming mode. Press Ctrl+C to stop the stream.")
    query.awaitTermination()

def write_batch_stream_console(stream_df):

    df_joined = stream_df.join(dept_df, stream_df.deptno == dept_df.deptno, "left") \
        .select(stream_df["*"], dept_df["dname"], dept_df["location"])
    
    query = df_joined.writeStream \
        .format("console") \
        .option("checkpointLocation", f"{ckpt_path}") \
        .outputMode("append") \
        .start() 
            
    query.awaitTermination()

stream_df = read_deltalake_stream()
write_deltalake_batch(stream_df)
# write_batch_stream_console(stream_df)
