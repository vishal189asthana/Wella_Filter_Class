# Databricks notebook source
# DBTITLE 1,Filtering code for WELLA subset from COTY ADLS


# COMMAND ----------

# DBTITLE 1,Loading logger class from the logger notebook in same workspace
# MAGIC 
# MAGIC 
# MAGIC %run "/Users/vishal_asthana@cotyinc.com/logger"

# COMMAND ----------

# DBTITLE 1,connection string for ADLS and SPARK

from time import time
from datetime import timedelta
from pyspark.sql.functions import *


class T():
    def __enter__(self):
        self.start = time()
    def __exit__(self, type, value, traceback):
        self.end = time()
        elapsed = self.end - self.start
        print(str(timedelta(seconds=elapsed)))
#Establish the connection between ADLS and Databricks
#spark.conf.set("fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net","<access-key>")

spark.conf.set("fs.azure.account.key.cashanalysislake.dfs.core.windows.net",dbutils.secrets.get(scope = "dataconnections", key = "adlsconnection"))


# COMMAND ----------

# DBTITLE 1,File schema properties 
class load_ADL_raw:
    
    def __init__(self, file_type,infer_schema, first_row_is_header,delimiter,filelocation):
        self.file_type=file_type
        self.infer_schema = infer_schema
        self.first_row_is_header = first_row_is_header
        self.delimiter=delimiter
        self.filelocation=filelocation
    
    def load_schema_ADL(self):
        print(self.file_type,self.infer_schema, self.first_row_is_header,self.delimiter,self.filelocation)

# COMMAND ----------


def load_adl_raw_data():
    x = load_ADL_raw("csv", "false", "true", "ψ",
                     "abfss://archive@cashanalysislake.dfs.core.windows.net/Finance/AccountsPayables/Daily/Current/Processed/201912/*.csv")
    df = spark.read.format(x.file_type) \
        .option("inferSchema", x.infer_schema) \
        .option("header", x.first_row_is_header) \
        .option("sep", x.delimiter) \
        .option("encoding", "utf-8") \
        .load(x.filelocation)
    return df


# COMMAND ----------


def load_main_function():
    with T():
        df = load_adl_raw_data()

        df = df.withColumn("file_name", input_file_name())
        df.dropDuplicates(['file_name'])
        df.createOrReplaceTempView("temp_table")
        new_df = spark.sql("select count(*) as rows_count, file_name from temp_table group by file_name")
        new_df.show(20,False)
        logger.info("count  is  %s",new_df.show(20, False))

# COMMAND ----------

# DBTITLE 1,Main function for reading the data frame from ADLS
if __name__ == "__main__":
  load_main_function()
  

	




# COMMAND ----------

# from pyspark.sql.functions import broadcast

# print(df.join(broadcast(new_df), df.file_name == new_df.file_name).take(2))

# COMMAND ----------

