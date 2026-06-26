# pip install deltalake pyarrow

## pyspark and delta lake
from delta.tables import DeltaTable

import sys
import os
from dotenv import load_dotenv
load_dotenv()
root_path = os.getenv("ROOT_PATH")



def print_info():
    import delta
    import pyspark
    print(" pyspark v is " , pyspark.__version__)
    print(" delta v is " , delta.__version__)


def print_table_details_deltalake(table_path):
    from deltalake import DeltaTable as DeltaTable_dl
    # above is delta-rs, and it does not have detail() or Spark DataFrame operations like .select() / .show().

    delta_table = DeltaTable_dl(table_path)
 
    print ( "************* PRINTING INFO ******")

    print( "****** Parition columns  ") 
    print(delta_table.metadata().partition_columns)

    print( "****** cluster columns - Not supported in deltalake python library ") 

    print ("version " , delta_table.version())

    # print( "****** History ") 
    # history = delta_table.history()

    # for h in history:
    #     print(h)

    print ( "***** Data ****")

    dt1 = DeltaTable_dl(
    table_path
    )

    df = dt1.to_pandas()

    print(df)
    print( "coutn of rows " , len(df))

def print_table_details_delta(spark , table_path):
       
    delta_table = DeltaTable.forPath( spark, table_path)
    print ( "************* PRINTING INFO ******")

    print( "****** parition columns ") 
    delta_table.detail().select("partitionColumns").show()

    print( "****** cluster columns ") 
    delta_table.detail().select("clusteringColumns").show()
    
    print ( "***** Data ****")
    delta_table.toDF().sort("id").show()        




print_info()



table_name = sys.argv[1] if len(sys.argv) > 1 else "emp_bz"

# # table_name = "deltalake_emp_partitioned"
root_path = os.getenv("ROOT_PATH")
table_path = f'{root_path}/DLake/tables/' + table_name
print_table_details_deltalake(table_path)

