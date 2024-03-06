import findspark
findspark.init()

import os 
import datetime 
import time
import json

import pandas as pd
import numpy as np
import pyspark.sql.functions as sf
import mysql.connector

from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, greatest , when, concat_ws, count, sum, lit, round 
from datetime import datetime, timedelta
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.types import *
from pyspark.sql import Row
from sqlalchemy import create_engine
from confluent_kafka import Producer


def calculating_clicks(df):
    clicks_data = df.filter(df.custom_track == 'click')
    clicks_data.createOrReplaceTempView('clicks')
    clicks_output = spark.sql(""" select job_id,date(ts) as Date , hour(ts) as Hour , publisher_id , campaign_id , group_id,
    avg(bid) as bid_set , count(*) as clicks , sum(bid) as spend_hour 
    from clicks
    group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id""")
    return clicks_output

def calculating_conversion(df):
    conversion_data = df.filter(df.custom_track == 'conversion')
    conversion_data.createOrReplaceTempView('conversion')
    conversion_output = spark.sql(""" select job_id,date(ts) as Date , hour(ts) as Hour , publisher_id , campaign_id , group_id,
    count(*) as conversion
    from conversion
    group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id""")
    return conversion_output 

def calculating_qualified(df):    
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data.createOrReplaceTempView('qualified')
    qualified_output = spark.sql(""" select job_id,date(ts) as Date , hour(ts) as Hour , publisher_id , campaign_id , group_id,
    count(*) as qualified
    from qualified
    group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id""")
    return qualified_output

def calculating_unqualified(df):
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data.createOrReplaceTempView('unqualified')
    unqualified_output = spark.sql(""" select job_id,date(ts) as Date , hour(ts) as Hour , publisher_id , campaign_id , group_id,
    count(*) as unqualified
    from unqualified
    group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id""")
    return unqualified_output

def process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output):
    final_data = clicks_output.join(conversion_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
    join(qualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
    join(unqualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full')
    return final_data 

def process_cassandra_data(df):
    clicks_output = calculating_clicks(df)
    conversion_output = calculating_conversion(df)
    qualified_output = calculating_qualified(df)
    unqualified_output = calculating_unqualified(df)
    final_data = process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output)
    return final_data

def max_time_cassandra():
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table = "tracking",keyspace = "project").load()
    max_time = df.agg({'ts':'max'}).take(1)[0][0] 
    return max_time 

def max_time_mysql():
    host = 'localhost'
    user = 'root'
    password = '1'
    database = 'project'    
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
    query = 'select max(latest_update_time) from event'
    mysql_time = pd.read_sql(query, engine)
    mysql_time = mysql_time.iloc[0, 0]
    max_time =  mysql_time.to_pydatetime()
    return max_time


producer_conf = {
    'bootstrap.servers': 'localhost',
    'queue.buffering.max.messages': 1000000,
    'queue.buffering.max.kbytes': 1024000  
}
producer = Producer(producer_conf)

def main():
    spark = SparkSession.builder.config("spark.jars.packages",'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0').getOrCreate()
    mysql_time = max_time_mysql()
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table = "tracking",keyspace = "project").load().where(col('ts')>mysql_time)
    data_transform_cassandra = process_cassandra_data(df)
    json_df = data_transform_cassandra.toJSON()
    for row in json_df.collect():
        print(row)
        producer.produce('project', value=row)

    
main()  
