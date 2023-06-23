# Databricks notebook source
# MAGIC %md 
# MAGIC ## Step 1: Receiver needs to share its metastore ID
# MAGIC
# MAGIC To get access to your provider data, you need to send him your metastore ID. This can be retrived very easily.
# MAGIC
# MAGIC As a **Receiver**, send your metastore ID to the provider

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT current_metastore();
# MAGIC -- note : Azure east = azure:eastus2:b86c6879-8c55-4e70-a585-18d16a4fa6e9

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 2: Providers creates the recipient using the receiver metastore id
# MAGIC
# MAGIC The data provider can now easily create a recipient using this metastore id:
# MAGIC
# MAGIC As a **Receiver**, send your metastore ID to the provider
# MAGIC

# COMMAND ----------

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
catalog = "matthieu_lamairesse"
database = "flights_perf"
print(current_user)
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

#Start with the share creation
spark.sql("CREATE SHARE IF NOT EXISTS mla_crosscloud_share COMMENT 'Cross Cloud Delta Sharing'")

#Set an owner to the share. Typicall an admin or an admin group.
spark.sql(f"ALTER SHARE mla_crosscloud_share OWNER TO `{current_user}`")


# COMMAND ----------

# MAGIC %sql
# MAGIC --add the table to the Share
# MAGIC --ALTER TABLE {catalog}.{database}.flight_optim_zorder SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
# MAGIC ALTER SHARE mla_crosscloud_share REMOVE  TABLE flights_perf.flight_optim_zorder;
# MAGIC ALTER SHARE mla_crosscloud_share ADD TABLE flights_perf.flight_optim_zorder WITH CHANGE DATA FEED ;

# COMMAND ----------

recipient_name = "azure-field-eng-east"
metastore_id = "azure:eastus2:b86c6879-8c55-4e70-a585-18d16a4fa6e9"

# COMMAND ----------


#Create the recipient using the metastore ID of given by the reciepient
spark.sql(f"CREATE RECIPIENT IF NOT EXISTS `{recipient_name}` USING ID '{metastore_id}' COMMENT 'Recipient for Azure Field Eng Metastore'")

#Grant select access to the share
spark.sql(f"GRANT SELECT ON SHARE mla_crosscloud_share TO RECIPIENT `{recipient_name}`")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC -- example query to copy and paste into reciepient workspace 
# MAGIC select year, `Month`, Origin, UniqueCarrier,
# MAGIC     sum(cancelled) as nb_cancelled,
# MAGIC     sum(1) as nb_fligts,
# MAGIC     (sum(cancelled) / sum(1)) * 100 as percent_cancelled,
# MAGIC     RANK () OVER (PARTITION BY year, Month ORDER BY (sum(cancelled) / sum(1)) * 100 DESC ) as canceled_rank
# MAGIC FROM  flights_perf.FLIGHT_optim_zorder
# MAGIC WHERE year = 1999
# MAGIC GROUP BY year, month, Origin, UniqueCarrier ;

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Step 3 : accept and mount the share as a receiver
# MAGIC
# MAGIC [reciever workspace - Azure FieldEng](https://adb-984752964297111.11.azuredatabricks.net/)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Step 4 : Let's play with the share 
# MAGIC
# MAGIC #### 1. let's create an table of agregates for to facilitate the querying on the receiver end 
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from flights_perf.FLIGHT_optim_zorder

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists flights_perf.AGG_airlines ( 
# MAGIC   year int, 
# MAGIC   Month int , 
# MAGIC   Origin string, 
# MAGIC   UniqueCarrier string, 
# MAGIC   nb_cancelled long, 
# MAGIC   nb_fligts long , 
# MAGIC   percent_cancelled double, 
# MAGIC   canceled_rank int )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true); -- To enable CDC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC TRUNCATE TABLE flights_perf.AGG_airlines 

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- add the table to the share
# MAGIC ALTER SHARE mla_crosscloud_share REMOVE TABLE flights_perf.AGG_airlines.flight_optim_zorder
# MAGIC ALTER SHARE mla_crosscloud_share ADD TABLE flights_perf.AGG_airlines WITH CHANGE DATA FEED;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- let's add data for the year 1998 
# MAGIC
# MAGIC Insert into flights_perf.AGG_airlines ( year, Month, Origin, UniqueCarrier, nb_cancelled, nb_fligts, percent_cancelled, canceled_rank)
# MAGIC   select year, `Month`, Origin, UniqueCarrier,
# MAGIC     sum(cancelled) as nb_cancelled,
# MAGIC     sum(1) as nb_fligts,
# MAGIC     (sum(cancelled) / sum(1)) * 100 as percent_cancelled,
# MAGIC     RANK () OVER (PARTITION BY year, Month ORDER BY (sum(cancelled) / sum(1)) * 100 DESC ) as canceled_rank
# MAGIC   FROM  flights_perf.FLIGHT_optim_zorder
# MAGIC   WHERE year = 1998
# MAGIC   GROUP BY year, month, Origin, UniqueCarrier ; 

# COMMAND ----------

# MAGIC %md 
# MAGIC check on the receiver end

# COMMAND ----------

# MAGIC %sql --let's add some new data to the share 
# MAGIC
# MAGIC Insert into flights_perf.AGG_airlines ( year, Month, Origin, UniqueCarrier, nb_cancelled, nb_fligts, percent_cancelled, canceled_rank)
# MAGIC select year, `Month`, Origin, UniqueCarrier,
# MAGIC     sum(cancelled) as nb_cancelled,
# MAGIC     sum(1) as nb_fligts,
# MAGIC     (sum(cancelled) / sum(1)) * 100 as percent_cancelled,
# MAGIC     RANK () OVER (PARTITION BY year, Month ORDER BY (sum(cancelled) / sum(1)) * 100 DESC ) as canceled_rank
# MAGIC   FROM  flights_perf.FLIGHT_optim_zorder
# MAGIC   WHERE year = 1999
# MAGIC   GROUP BY year, month, Origin, UniqueCarrier ; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- example query on reciever end 
# MAGIC SELECT year, count(1) 
# MAGIC FROM flights_perf.agg_airlines
# MAGIC GROUP BY year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- cleanup 
# MAGIC ALTER SHARE mla_crosscloud_share REMOVE TABLE flights_perf.AGG_airlines ;
# MAGIC ALTER SHARE mla_crosscloud_share REMOVE TABLE flights_perf.FLIGHT_optim_zorder ;
# MAGIC REVOKE SELECT ON SHARE mla_crosscloud_share FROM RECIPIENT `azure-field-eng-east` ; 
