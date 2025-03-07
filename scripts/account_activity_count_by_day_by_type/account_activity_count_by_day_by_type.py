import json

import pandas as pd
import numpy as np

import time
import glob

import os

from datetime import date, timedelta
import datetime
from collections import defaultdict, Counter
import pickle

from functools import reduce

import multiprocessing as mp

import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["JAVA_HOME"] = "/N/u/oseckin/jdk-17.0.12"

from pyspark.sql.types import DateType
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F


# Create a Spark session
spark = (
        SparkSession.builder\
        .master("local[1]")\
        .config("spark.driver.bindAddress", "localhost")\
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")\
        #.config("spark.driver.memory", "8g") \
        #.config("spark.executor.memory", "8g") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseParallelGC")
        .getOrCreate()
    )


fp_list = sorted([(os.path.getsize(fp)/(1024*1024),fp) for fp in glob.glob('/N/project/exodusbsky/bluesky_activities/bluesky_activities.*.parquet')])
fp_list = [fp[1] for fp in fp_list]

numbers = sorted([int(i.split('.')[1]) for i in glob.glob('/N/project/exodusbsky/bluesky_activities/bluesky_activities.*.parquet')])

# read all activity files and dump into a list
df = []
for fp in fp_list:
    temp = spark.read.parquet(fp)
    df.append(temp)

# concatenate all
df = reduce(lambda x,y:x.union(y), df)

# extract the date info from creation date (date+time)
df = df.withColumn("date", F.to_date(F.from_unixtime(F.col("creationDate") / 1e9)))

# pivot table to find the daily activity count by user
pivot_table = df.groupBy("author", "date", "action", "type").agg(F.count("*").alias("count"))

# cache and write as a parquet file
df.unpersist()
pivot_table.write.mode("overwrite").parquet(f"/N/project/exodusbsky/processed/account_activity_count_by_day/account_activity_count_by_day_{min(numbers)}_{max(numbers)}.parquet")

print('Account activity count by day parquet file saved')
del pivot_table, df
import gc
gc.collect()

per_user_activity = pd.read_parquet(f'/N/project/exodusbsky/processed/account_activity_count_by_day/account_activity_count_by_day_{min(numbers)}_{max(numbers)}.parquet', engine='pyarrow')
print('Account activity count by day parquet read back with pandas')

# Add user group information
with open('/N/project/INCAS/bluesky/profile_creations/user_groups.pickle', 'rb') as f:
    user_groups = pickle.load(f)

mapping = {'public_date_group_interval':'Public Access',
           'brazil_ban_interval':'Brazil X Ban',
           'block_policy_change_interval':'X Block Policy Change',
           'election_interval':'US Elections'}

user_to_group = {}
for group_name, i in user_groups.items():
    for u in i:
        user_to_group[u] = mapping[group_name]

per_user_activity['user_group'] = per_user_activity['author'].apply(lambda x: user_to_group.get(x, 'Other'))
print('User group information added')

check_activity = {'app.bsky.feed.like', 'app.bsky.feed.post', 'app.bsky.feed.repost'}
per_user_activity = per_user_activity[per_user_activity['type'].apply(lambda x: x in check_activity)]
print('Filtering by type done...')

per_user_activity = per_user_activity[per_user_activity['action'] == 'create']
print('Filtering by action done...')

per_user_activity_pivot = per_user_activity.pivot_table(index='date', columns='user_group', values='count', aggfunc=['count','sum'])
print('Pivot table ready...')

per_user_activity_pivot['User Count'] = per_user_activity_pivot['count'][['Brazil X Ban', 'Other', 'Public Access', 'US Elections',
                                                                   'X Block Policy Change']].sum(axis=1)
per_user_activity_pivot['Total Engagement'] = per_user_activity_pivot['sum'][['Brazil X Ban', 'Other', 'Public Access', 'US Elections',
                                                                   'X Block Policy Change']].sum(axis=1)
print('Aggregate results ready...')

per_user_activity_pivot = per_user_activity_pivot.sort_index()
print('Sorted by date...')

print('Dumping into parquet file...')
per_user_activity_pivot.to_parquet(f'/N/project/exodusbsky/processed/account_activity_count_by_day/active_user_count_per_day_{min(numbers)}_{max(numbers)}.parquet')