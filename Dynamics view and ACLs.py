# Databricks notebook source
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
# MAGIC   flights_db.FLIGHTS
# MAGIC where
# MAGIC   year = 1990
# MAGIC group by
# MAGIC   year,
# MAGIC   DayOfWeek,
# MAGIC   UniqueCarrier
# MAGIC order by
# MAGIC   DayOfWeek,
# MAGIC   percent_cancelled desc

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW flights_db.flights_row_filtering 
# MAGIC as 
# MAGIC   select flights.* , airports.state, airports.airport
# MAGIC   from flights_db.flights join flights_db.airports on flights.Origin = airports.iata 
# MAGIC   where 
# MAGIC       CASE
# MAGIC          WHEN current_user() = "matthieu.lamairesse+uc@databricks.com" THEN airports.state = "CO"
# MAGIC          ELSE TRUE
# MAGIC       END;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select flights.* , airports.state, airports.airport
# MAGIC from flights_db.flights join flights_db.airports on flights.Origin = airports.iata 
# MAGIC where
# MAGIC       CASE
# MAGIC          WHEN current_user() = "matthieu.lamairesse+ucadmin@databricks.com" THEN airports.state = "CO"
# MAGIC          ELSE TRUE
# MAGIC       END
# MAGIC limit 10 

# COMMAND ----------


