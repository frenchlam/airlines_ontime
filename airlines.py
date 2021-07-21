# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/airlines/

# COMMAND ----------



# COMMAND ----------

initial_df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dbfs:/databricks-datasets/airlines/part-00000"))
df_schema = initial_df.schema

remaining_df = (spark.read
      .option("header", "false")
      .schema(df_schema)
      .csv("dbfs:/databricks-datasets/airlines/part-000{0[1-9],[1-9][0-9]}"))

df = initial_df.union(remaining_df)
df.createOrReplaceTempView("flights")

display(df)

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF not exists MATTHIEU
# MAGIC LOCATION "dbfs:/Users/matthieu.lamairesse@databricks.com/flights_db"

# COMMAND ----------

df_filtered = df.filter("year < 2008")
df_filtered.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTableformat("delta")
  .saveAsTable("MATTHIEU.FLIGHTS_RAW")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %sql 
# MAGIC use MATTHIEU ; 
# MAGIC select * from FLIGHTS_RAW

# COMMAND ----------

# MAGIC %sql 
# MAGIC USE MATTHIEU ; 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS FLIGHT_GOLD
# MAGIC USING DELTA 
# MAGIC PARTITIONED BY (Year)
# MAGIC COMMENT "Partition Year - zorder "
# MAGIC AS (
# MAGIC    SELECT *
# MAGIC    FROM FLIGHTS_RAW
# MAGIC    ORDER BY Month, DayofMonth
# MAGIC    )

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE MATTHIEU.FLIGHT_GOLD ZORDER BY (Month, Origin)

# COMMAND ----------

# MAGIC %sql 
# MAGIC with cancelled_origin as (
# MAGIC   select year, UniqueCarrier,
# MAGIC     sum(cancelled) as nb_cancelled,
# MAGIC     sum(1) as nb_fligts,
# MAGIC     (sum(cancelled) / sum(1)) * 100 as percent_cancelled,
# MAGIC     RANK () OVER ( ORDER BY (sum(cancelled) / sum(1)) * 100 DESC ) as canceled_rank
# MAGIC   from MATTHIEU.FLIGHT_GOLD
# MAGIC   group by year, UniqueCarrier
# MAGIC )
# MAGIC SELECT year, UniqueCarrier, nb_cancelled, percent_cancelled, canceled_rank
# MAGIC FROM cancelled_origin
# MAGIC where canceled_rank <= 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a non materialized view 
# MAGIC View definition is saved in the metastore and applied at runtime  
# MAGIC [https://docs.databricks.com/spark/2.x/spark-sql/language-manual/create-view.html](https://docs.databricks.com/spark/2.x/spark-sql/language-manual/create-view.html)

# COMMAND ----------



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


