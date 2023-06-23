# Databricks notebook source
# MAGIC %md 
# MAGIC ## SETUP 

# COMMAND ----------

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

#catalog = "hive_metastore"
catalog = "matthieu_lamairesse"
database = "flights_perf"
print(current_user)

# COMMAND ----------

# Setup the Database 
# Assuming the Catalog exists

catalog = "matthieu_lamairesse"

if catalog == "hive_metastore" :
  db_loc = f"dbfs:/Users/{current_user}/{database}/"
  sql_statement = f"""
                  CREATE DATABASE IF not exists `hive_metastore`.`{database}`
                  LOCATION "{db_loc}"
                  """
  spark.sql(sql_statement)

else : 
  sql_statement = f"""
                  CREATE DATABASE IF not exists `{catalog}`.`{database}`
                  """
  if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' :
    spark.sql(sql_statement)
  else :
    print ( "Not a UC enabled Cluster" )
    exit()

#debug
#print(sql_statement)   


# COMMAND ----------

# MAGIC %md
# MAGIC ### Download Dimensions.  
# MAGIC - Airports - http://stat-computing.org/dataexpo/2009/airports.csv
# MAGIC - Carriers - http://stat-computing.org/dataexpo/2009/carriers.csv
# MAGIC - Planes - 'http://stat-computing.org/dataexpo/2009/plane-data.csv'
# MAGIC

# COMMAND ----------

dbutils.fs.ls("dbfs:/Users/matthieu.lamairesse@databricks.com/")

# COMMAND ----------



# COMMAND ----------

# Download locations
url_dict = {'airports.csv':'https://raw.githubusercontent.com/frenchlam/airlines_ontime/main/Data/airports.csv',
             'carriers.csv':'http://stat-computing.org/dataexpo/2009/carriers.csv',
             'plane-data.csv':'http://stat-computing.org/dataexpo/2009/plane-data.csv'}

path = "/tmp/airlines_data/"

#Download to HDFS. 
from pathlib import Path
Path(path).mkdir(parents=True, exist_ok=True)

import urllib 
for url in url_dict : 
  #print( "key: {}  value: {}".format(url,url_dict[url] )) 
  urllib.request.urlretrieve(url_dict[url], path+url)
  
dbutils.fs.mv( "file:"+path, "dbfs:/Users/"+current_user+"/airlines_data/" , recurse = True)


# COMMAND ----------

spark.read.option("header", "true") \
      .option("inferSchema", "true") \
      .csv("dbfs:/Users/"+current_user+"/airlines_data/airports.csv") \
      .write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .option("nullValue", "NA") \
      .saveAsTable(catalog+"."+database+".airports")

spark.read.option("header", "true") \
      .option("inferSchema", "true") \
      .csv("dbfs:/Users/"+current_user+"/airlines_data/carriers.csv") \
      .write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .option("nullValue", "NA") \
      .saveAsTable(catalog+"."+database+".carriers")

spark.read.option("header", "true") \
      .option("inferSchema", "true") \
      .csv("dbfs:/Users/"+current_user+"/airlines_data/plane-data.csv") \
      .write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .option("nullValue", "NA") \
      .saveAsTable(catalog+"."+database+".planes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare the facts table

# COMMAND ----------

catalog = "matthieu_lamairesse"

if catalog == "hive_metastore" :
  db_loc = f"dbfs:/Users/{current_user}/{database}/"
  sql_statement = f"""
                  CREATE DATABASE IF not exists `hive_metastore`.`{database}`
                  LOCATION "{db_loc}"
                  """
else : 
  sql_statement = f"""
                  CREATE DATABASE IF not exists `{catalog}`.`{database}`
                  """

print(sql_statement)   

try : 
  spark.sql(sql_statement)
except Exception as e: 
  print(e)


# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS matthieu_lamairesse.flights_db
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF not exists flights_perf
# MAGIC LOCATION "dbfs:/Users/matthieu.lamairesse@databricks.com/flights_db"

# COMMAND ----------

# MAGIC %sql
# MAGIC create table flights_perf.FLIGHTS_RAW (
# MAGIC       Year integer,
# MAGIC       Month integer,
# MAGIC       DayofMonth integer, 
# MAGIC       DayOfWeek integer, 
# MAGIC       DepTime string, 
# MAGIC       CRSDepTime integer, 
# MAGIC       ArrTime string, 
# MAGIC       CRSArrTime integer, 
# MAGIC       UniqueCarrier string, 
# MAGIC       FlightNum integer, 
# MAGIC       TailNum string, 
# MAGIC       ActualElapsedTime string, 
# MAGIC       CRSElapsedTime integer, 
# MAGIC       AirTime string, 
# MAGIC       ArrDelay string, 
# MAGIC       DepDelay string, 
# MAGIC       Origin string, 
# MAGIC       Dest string, 
# MAGIC       Distance string, 
# MAGIC       TaxiIn string, 
# MAGIC       TaxiOut string, 
# MAGIC       Cancelled integer, 
# MAGIC       CancellationCode string, 
# MAGIC       Diverted integer, 
# MAGIC       CarrierDelay string, 
# MAGIC       WeatherDelay string, 
# MAGIC       NASDelay string, 
# MAGIC       SecurityDelay string, 
# MAGIC       LateAircraftDelay string, 
# MAGIC       IsArrDelayed string, 
# MAGIC       IsDepDelayed string )
# MAGIC USING CSV 
# MAGIC LOCATION 'dbfs:/databricks-datasets/airlines/'
# MAGIC OPTIONS ('header' = TRUE )

# COMMAND ----------

initial_df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dbfs:/databricks-datasets/airlines/part-00000"))
df_schema = initial_df.schema

df = (spark.read
      .option("header", "false")
      .schema(df_schema)
      .csv("dbfs:/databricks-datasets/airlines/"))
#      .csv("dbfs:/databricks-datasets/airlines/part-000{0[1-9],[1-9][0-9]}"))

#df = initial_df.union(remaining_df)
df.createOrReplaceTempView("flights")

display(df)

# COMMAND ----------

df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("delta.tuneFileSizesForRewrites", "true") \
  .saveAsTable("flights_perf.FLIGHT_optim")

# COMMAND ----------

df.write \
  .partitionBy("year") \
  .format("delta") \
  .mode("overwrite") \
  .option("maxRecordsPerFile", 120000) \
  .option("overwriteSchema", "true") \
  .option("delta.tuneFileSizesForRewrites", "true") \
  .saveAsTable("flights_perf.FLIGHT_partitioned")

# COMMAND ----------

df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("delta.tuneFileSizesForRewrites", "true") \
  .saveAsTable("flights_perf.FLIGHT_optim_zorder")

# COMMAND ----------

# MAGIC %sql 
# MAGIC ALTER TABLE flights_perf.FLIGHT_optim SET TBLPROPERTIES ('delta.tuneFileSizesForRewrites' = True);
# MAGIC ALTER TABLE flights_perf.FLIGHT_optim_partitioned SET TBLPROPERTIES ('delta.tuneFileSizesForRewrites' = True);
# MAGIC
# MAGIC --Change default file size
# MAGIC --ALTER TABLE flights_perf.FLIGHT_optim SET TBLPROPERTIES (delta.targetFileSize = 33554432);
# MAGIC --ALTER TABLE flights_perf.FLIGHT_optim SET TBLPROPERTIES (delta.targetFileSize = 33554432);
# MAGIC -- Now auto-tuned cf : https://docs.databricks.com/delta/file-mgmt.html#autotune-based-on-table-size
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE flights_perf.FLIGHT_optim ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE flights_perf.FLIGHT_optim_zorder ZORDER BY (year, Month, Origin);

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE flights_perf.FLIGHT_optim_partitioned ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC ANALYZE TABLE flights_perf.FLIGHT_optim_zorder COMPUTE STATISTICS FOR ALL COLUMNS

# COMMAND ----------

# MAGIC %sql 
# MAGIC with cancelled_origin as (
# MAGIC   select year, UniqueCarrier,
# MAGIC     sum(cancelled) as nb_cancelled,
# MAGIC     sum(1) as nb_fligts,
# MAGIC     (sum(cancelled) / sum(1)) * 100 as percent_cancelled,
# MAGIC     RANK () OVER ( ORDER BY (sum(cancelled) / sum(1)) * 100 DESC ) as canceled_rank
# MAGIC   from flights_perf.FLIGHT_GOLD
# MAGIC   group by year, UniqueCarrier
# MAGIC )
# MAGIC SELECT year, UniqueCarrier, nb_cancelled, percent_cancelled, canceled_rank
# MAGIC FROM cancelled_origin
# MAGIC where canceled_rank <= 10

# COMMAND ----------


