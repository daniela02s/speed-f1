import os
import pandas as pd
from tqdm import tqdm 

import pyspark
from delta import *

import os
os.environ['HADOOP_HOME'] = 'C:\\Hadoop'
os.environ['PATH'] = os.environ['HADOOP_HOME'] + '\\bin;' + os.environ['PATH']

builder = (pyspark.sql.SparkSession.builder.appName("MyApp")
                  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def consolidate_bronze(spark):

    for i in track(os.listdir("data/raw"), description="Convertendo dados..."):
        if i.endswith(".parquet"):
            path = f"data/raw/{i}"
            df = pd.read_parquet(path)
            df.to_csv(path.replace(".parquet", ".csv"), sep=';', index=False)
        

    df = (spark.read
               .csv("data/raw/*.csv",sep=";", header=True))

    (df.coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("data/bronze/results"))

def main():

    import sys
    sys.path.append(".")
    
    import spark_ops

    spark = spark_ops.new_spark_sesion()    
    consolidate_bronze(spark)


if __name__ == "__main__":
    main()


