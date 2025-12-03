import argparse
import sys

sys.path.append("")
import spark_ops

import os
os.environ['HADOOP_HOME'] = 'C:\\Hadoop'
os.environ['PATH'] = os.environ['HADOOP_HOME'] + '\\bin;' + os.environ['PATH']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", default="", type=str)
    args = parser.parse_args()
    query = args.query
    
    if query == "":
        return
    
    spark = spark_ops.new_spark_sesion()
    spark_ops.create_view_from_path("data/bronze/results", spark)
    spark_ops.create_view_from_path("data/silver/fs_drivers", spark)

    try:
        spark_ops.create_view_from_path("data/silver/champions", spark)
        
    except Exception:
        spark_ops.create_table("champions", spark)
        
    spark_ops.create_table(query, spark)


if __name__ == "__main__":
    main()