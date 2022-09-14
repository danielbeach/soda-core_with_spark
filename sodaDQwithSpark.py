from pyspark.sql import DataFrame, SparkSession
from sodaspark import scan
from pathlib import Path

spark = SparkSession.builder.getOrCreate()
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.access.key", "")
hadoop_conf.set("fs.s3a.secret.key", "")


df = spark.read.csv('s3a://confessions-of-a-data-guy/*csv', header='true')

scan_definition = Path('checks.yaml').read_text()

scan_result = scan.execute(scan_definition, df)
print(scan_result.measurements)
print(scan_result.test_results)
