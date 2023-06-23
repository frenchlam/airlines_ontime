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
# MAGIC - Airports - https://raw.githubusercontent.com/frenchlam/airlines_ontime/main/Data/airports.csv
# MAGIC - Carriers - https://raw.githubusercontent.com/frenchlam/airlines_ontime/main/Data/airlines.csv
# MAGIC
# MAGIC

# COMMAND ----------

# Download locations
url_dict = {'airports.csv':'https://raw.githubusercontent.com/frenchlam/airlines_ontime/main/Data/airports.csv',
             'airlines.csv':'https://raw.githubusercontent.com/frenchlam/airlines_ontime/main/Data/airlines.csv'}

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
      .csv("dbfs:/Users/"+current_user+"/airlines_data/airlines.csv") \
      .write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .option("nullValue", "NA") \
      .saveAsTable(catalog+"."+database+".airlines")


# COMMAND ----------

spark.sql(f"OPTIMIZE {catalog}.{database}.airports ZORDER BY iata")
spark.sql(f"ANALYZE TABLE {catalog}.{database}.airports COMPUTE STATISTICS FOR All COLUMNS")

spark.sql(f"OPTIMIZE {catalog}.{database}.airlines ZORDER BY UniqueCode")
spark.sql(f"ANALYZE TABLE {catalog}.{database}.airlines COMPUTE STATISTICS FOR All COLUMNS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare the facts table

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{database}.FLIGHTS_RAW")


# COMMAND ----------

sql_create_flight_raw = f"""
create table IF NOT EXISTS {catalog}.{database}.FLIGHTS_RAW (
      Year integer,
      Month integer,
      DayofMonth integer, 
      DayOfWeek integer, 
      DepTime string, 
      CRSDepTime integer, 
      ArrTime string, 
      CRSArrTime integer, 
      UniqueCarrier string, 
      FlightNum integer, 
      TailNum string, 
      ActualElapsedTime string, 
      CRSElapsedTime integer, 
      AirTime string, 
      ArrDelay string, 
      DepDelay string, 
      Origin string, 
      Dest string, 
      Distance string, 
      TaxiIn string, 
      TaxiOut string, 
      Cancelled integer, 
      CancellationCode string, 
      Diverted integer, 
      CarrierDelay string, 
      WeatherDelay string, 
      NASDelay string, 
      SecurityDelay string, 
      LateAircraftDelay string, 
      IsArrDelayed string, 
      IsDepDelayed string )
USING CSV 
LOCATION 'dbfs:/databricks-datasets/airlines/'
OPTIONS ('header' = TRUE )
"""
#spark.sql(sql_create_flight_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC #### create optimized tables

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
  .saveAsTable(f"{catalog}.{database}.FLIGHT_optim")

# COMMAND ----------

spark.sql(f"OPTIMIZE {catalog}.{database}.FLIGHT_optim")

# COMMAND ----------

df.write \
  .partitionBy("year") \
  .format("delta") \
  .mode("overwrite") \
  .option("maxRecordsPerFile", 120000) \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{database}.FLIGHT_partitioned")

# COMMAND ----------

df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .option("delta.tuneFileSizesForRewrites", "true") \
  .option("delta.enableChangeDataFeed, true")\
  .saveAsTable(f"{catalog}.{database}.FLIGHT_optim_zorder")

# COMMAND ----------

spark.sql(f"OPTIMIZE {catalog}.{database}.FLIGHT_optim_zorder ZORDER BY (year, Month, Origin)")
spark.sql(f"ANALYZE TABLE {catalog}.{database}.FLIGHT_optim_zorder COMPUTE STATISTICS FOR All COLUMNS")

# COMMAND ----------

# MAGIC %sql 
# MAGIC with cancelled_origin as (
# MAGIC   select year, UniqueCarrier,
# MAGIC     sum(cancelled) as nb_cancelled,
# MAGIC     sum(1) as nb_fligts,
# MAGIC     (sum(cancelled) / sum(1)) * 100 as percent_cancelled,
# MAGIC     RANK () OVER ( ORDER BY (sum(cancelled) / sum(1)) * 100 DESC ) as canceled_rank
# MAGIC   from flights_perf.FLIGHT_optim_zorder
# MAGIC   group by year, UniqueCarrier
# MAGIC )
# MAGIC SELECT year, UniqueCarrier, nb_cancelled, percent_cancelled, canceled_rank
# MAGIC FROM cancelled_origin
# MAGIC where canceled_rank <= 10
