import pandas as pd
import os
import sys
import glob
import pickle
from functools import reduce

from pyspark.sql.types import DateType, StructType, StructField, DoubleType, LongType, ArrayType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from graphframes import GraphFrame

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["JAVA_HOME"] = "/N/u/oseckin/jdk-17.0.12"

# Create a Spark session
spark = (
        SparkSession.builder\
        .master("local[*]")\
        .config("spark.driver.bindAddress", "0.0.0.0")\
        .config("spark.driver.memory", "64g")\
        .config("spark.executor.memory", "64g")\
        .config("spark.executor.memoryOverhead", "16g")\
        .config("spark.memory.offHeap.enabled", "true")\
        .config("spark.memory.offHeap.size", "32g")\
        .config("spark.driver.host", "localhost")\
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")\
        .config("spark.jars", "/N/u/oseckin/graphframes-0.8.4-spark3.5-s_2.12.jar")\
        .config("spark.driver.extraJavaOptions", "-XX:+UseParallelGC")\
        .getOrCreate()
    )

spark.sparkContext.setCheckpointDir("/N/project/exodusbsky/processed/network/tmp/spark-checkpoints")

# get file paths and sort them by size
file_paths = sorted([fp for fp in glob.glob('/N/project/exodusbsky/bluesky_activities/bluesky_follows.*')])

df = []

for fp in file_paths:

    # read the parquet
    temp = spark.read.parquet(fp)

    # date-time into date
    temp = temp.withColumn("date", F.to_date(F.from_unixtime(F.col("creationDate") / 1e9)))

    # take only the follow actions
    temp = temp.filter(temp.type == 'app.bsky.graph.follow')

    # dump into df
    df.append(temp)

# concatenate the files
df = reduce(lambda x,y:x.union(y), df)

# MERGE DELETE INFORMATION

# Step 1: Filter "create" actions
create_df = df.filter(F.col("action") == "create").select("uri", "subjectAuthor")

# Step 2: Join to match "delete" actions with their original "create" actions
joined_df = df.alias("d").join(
    create_df.alias("c"),
    on=(F.col("d.uri") == F.col("c.uri")) & (F.col("d.action") == "delete"),
    how="left"
)

# Step 3: Update `subjectAuthor` for "delete" actions
updated_df = joined_df.withColumn(
    "updatedSubjectAuthor",
    F.when(F.col("d.action") == "delete", F.col("c.subjectAuthor")).otherwise(F.col("d.subjectAuthor"))
).select(
    F.col("d.author"), 
    F.col("d.type"), 
    F.col("d.action"), 
    F.col("d.creationDate"), 
    F.col("d.uri"), 
    F.col("updatedSubjectAuthor").alias("subjectAuthor"),  # Use the updated column
    F.col("d.date")
)

# Step 4: order the dataframe by creationDate
updated_df = updated_df.orderBy("creationDate")

# Step 5: Export the data
# get the numbers
numbers = [int(i.split('.')[-2]) for i in file_paths]
# write as a parquet file
updated_df.write.mode("overwrite").parquet(f"/N/project/exodusbsky/processed/network/follows_unfollows/follows_unfollows_{min(numbers)}_{max(numbers)}.parquet")

# save a version without bluesky official account
# remove official bluesky account from data
filtered_df = updated_df.filter(
    ~(F.col("author") == "did:plc:z72i7hdynmk6r22z27h6tvur") & 
    ~(F.col("subjectAuthor") == "did:plc:z72i7hdynmk6r22z27h6tvur")
    )

# save
filtered_df.write.mode("overwrite").partitionBy("date").parquet(f'/N/project/exodusbsky/processed/network/follows_unfollows_wout_bsky_{min(numbers)}_{max(numbers)}.parquet')