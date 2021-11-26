# Databricks notebook source
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
database = "flights_db"
print(current_user)

# COMMAND ----------

url_dict = {'airports.csv':'http://stat-computing.org/dataexpo/2009/airports.csv',
             'carriers.csv':'http://stat-computing.org/dataexpo/2009/carriers.csv',
             'plane-data.csv':'http://stat-computing.org/dataexpo/2009/plane-data.csv'}

path = "/tmp/airlines_data/"

from pathlib import Path
Path(path).mkdir(parents=True, exist_ok=True)

import urllib 
for url in url_dict : 
  #print( "key: {}  value: {}".format(url,url_dict[url] )) 
  urllib.request.urlretrieve(url_dict[url], path+url)
  
dbutils.fs.mv( "file:"+path, "dbfs:/Users/"+current_user+"/airlines_data/" , recurse = True)

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls dbfs:/Users/matthieu.lamairesse@databricks.com/airlines_data/

# COMMAND ----------

spark.sql('''
  CREATE DATABASE IF NOT EXISTS {database}
  LOCATION "dbfs:/Users/{user}/flights_db"
'''.format(user=current_user, database=database ))


# COMMAND ----------

spark.read.option("header", "true") \
      .option("inferSchema", "true") \
      .csv("dbfs:/Users/"+current_user+"/airlines_data/airports.csv") \
      .write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .option("nullValue", "NA") \
      .saveAsTable(database+".airports")

spark.read.option("header", "true") \
      .option("inferSchema", "true") \
      .csv("dbfs:/Users/"+current_user+"/airlines_data/carriers.csv") \
      .write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .option("nullValue", "NA") \
      .saveAsTable(database+".carriers")

spark.read.option("header", "true") \
      .option("inferSchema", "true") \
      .csv("dbfs:/Users/"+current_user+"/airlines_data/plane-data.csv") \
      .write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .option("nullValue", "NA") \
      .saveAsTable(database+".planes")


# COMMAND ----------

initial_df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NA") \
      .csv("dbfs:/databricks-datasets/airlines/part-00000"))
df_schema = initial_df.schema

#remaining_df = (spark.read
#      .option("header", "false")
#     .option("nullValue", "NA") \
#      .schema(df_schema)
#      .csv("dbfs:/databricks-datasets/airlines/part-000{0[1-9],[1-9][0-9]}"))

#df = initial_df.union(remaining_df)
#df.createOrReplaceTempView("flights")

display(initial_df)

# COMMAND ----------

# def delay(s):
#   if(s > 15): 
#     return 1
#   else : 
#     return 0

# from pyspark.sql.functions import udf
# from pyspark.sql.types import IntegerType

# delayUDF = udf(delay, IntegerType())

df_filtered = df.filter("year =< 2008")
df_filtered = df_filtered.withColumn("ArrDelay",df.ArrDelay.cast('integer')) \
                         .withColumn("DepDelay",df.DepDelay.cast('integer')) \
                         .withColumn("Distance",df.Distance.cast('integer'))

display(df_filtered)

# COMMAND ----------



# COMMAND ----------

df_filtered.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable(database+".FLIGHTS_RAW")

# COMMAND ----------

spark.sql('''
              CREATE TABLE IF NOT EXISTS {database}.FLIGHT_GOLD
              USING DELTA 
              PARTITIONED BY (Year)
              COMMENT "Partition Year - zorder "
              AS (
                 SELECT *
                 FROM {database}.FLIGHTS_RAW
                 ORDER BY Month, DayofMonth
                 )'''.format(database=database)
         )

# COMMAND ----------

display(spark.sql('''OPTIMIZE {database}.FLIGHT_GOLD ZORDER BY (Month, Origin)'''.format(database=database)))
#spark.sql('''OPTIMIZE {database}.airports'''.format(database=database))
#spark.sql('''OPTIMIZE {database}.carriers'''.format(database=database))

# COMMAND ----------

# MAGIC %sql 
# MAGIC with cancelled_origin as (
# MAGIC   select year, UniqueCarrier,
# MAGIC     sum(cancelled) as nb_cancelled,
# MAGIC     sum(1) as nb_fligts,
# MAGIC     (sum(cancelled) / sum(1)) * 100 as percent_cancelled,
# MAGIC     RANK () OVER ( ORDER BY (sum(cancelled) / sum(1)) * 100 DESC ) as canceled_rank
# MAGIC   from flights_db.FLIGHT_GOLD
# MAGIC   group by year, UniqueCarrier
# MAGIC )
# MAGIC SELECT year, UniqueCarrier, nb_cancelled, percent_cancelled, canceled_rank
# MAGIC FROM cancelled_origin
# MAGIC where canceled_rank <= 10

# COMMAND ----------

from pyspark.sql.functions import *
flight_df = spark.sql("select * from flights_db.FLIGHT_GOLD where year = 1992")

cancelled_routes_all = flight_df\
  .filter("CANCELLED == 1")\
  .withColumn("combo_hash", hash(col("ORIGIN"))+hash(col("DEST")))\
  .withColumn("combo", concat(col("ORIGIN"),col("DEST")))\
  .groupby("combo_hash")\
  .agg(count("combo_hash").alias("count"),first("combo").alias("route_alias"))\
  .sort("count",ascending=False)  

display(cancelled_routes_all)

# COMMAND ----------



# COMMAND ----------

display(all_routes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a non materialized view 
# MAGIC View definition is saved in the metastore and applied at runtime  
# MAGIC [https://docs.databricks.com/spark/2.x/spark-sql/language-manual/create-view.html](https://docs.databricks.com/spark/2.x/spark-sql/language-manual/create-view.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS MATTHIEU.FLIGHTS_96 ;
# MAGIC 
# MAGIC CREATE VIEW MATTHIEU.FLIGHTS_96
# MAGIC as (SELECT * 
# MAGIC     FROM MATTHIEU.FLIGHT_GOLD
# MAGIC     WHERE YEAR = 1996
# MAGIC     ) ;
# MAGIC     
# MAGIC DESCRIBE EXTENDED MATTHIEU.flights_96 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * 
# MAGIC from MATTHIEU.FLIGHTS_96
# MAGIC where year = 1998
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * 
# MAGIC from MATTHIEU.FLIGHT_GOLD
# MAGIC where year = 1998
# MAGIC limit 100

# COMMAND ----------

# MAGIC %fs
# MAGIC ls Users/matthieu.lamairesse@databricks.com/flights_db/

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls dbfs:/Users/matthieu.lamairesse@databricks.com/flights_db/flight_gold/

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE flights_db.TEST 
# MAGIC 
# MAGIC AS (
# MAGIC SELECT * FROM delta.`dbfs:/Users/matthieu.lamairesse@databricks.com/flights_db/flight_gold/`)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   Cast( year AS STRING ) as year,
# MAGIC   DayOfWeek,
# MAGIC   UniqueCarrier,
# MAGIC   sum(cancelled) as nb_cancelled,
# MAGIC   COUNT(CASE WHEN IsArrDelayed = "YES" THEN 1 END) as nb_delayed,
# MAGIC   sum(1) as nb_fligts,
# MAGIC   (sum(cancelled) / sum(1)) * 100 as percent_cancelled,
# MAGIC   ( COUNT(CASE WHEN IsArrDelayed = "YES" THEN 1 END) /sum(1)) *100 as percent_delayed
# MAGIC from
# MAGIC   flights_db.FLIGHT_GOLD
# MAGIC where
# MAGIC   year = 1990
# MAGIC group by
# MAGIC   year,
# MAGIC   DayOfWeek,
# MAGIC   UniqueCarrier
# MAGIC order by
# MAGIC   DayOfWeek,
# MAGIC   percent_cancelled desc
# MAGIC   

# COMMAND ----------


