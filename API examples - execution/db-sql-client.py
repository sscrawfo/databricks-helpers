# Databricks notebook source
# MAGIC %md ### Example of using python client for DB SQL

# COMMAND ----------

# MAGIC %pip install databricks-sql-connector

# COMMAND ----------

from databricks import sql
import os

import ssl

# You can get these settings from the UI (and generate an access_token)
with sql.connect(server_hostname = "adb-XXXXX.1.azuredatabricks.net",
                 http_path       = "/sql/1.0/warehouses/73cad4cbe691fb16",
                 access_token    = "dapXXXXXXXXXXXXXXXXXX") as connection:

  with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM default.diamonds LIMIT 2")
    result = cursor.fetchall()

    for row in result:
      print(row)


# COMMAND ----------

# MAGIC %sql drop table default.diamonds_copy_sql

# COMMAND ----------

with sql.connect(server_hostname = "adb-XXXXX.1.azuredatabricks.net",
                 http_path       = "/sql/1.0/warehouses/73cad4cbe691fb16",
                 access_token    = "dapXXXXXXXXXXXXXXXXXX") as connection:
  
  with connection.cursor() as cursor:
    cursor.execute("create table default.diamonds_copy_sql as select * from default.diamonds")
    result = cursor.fetchall()

    for row in result:
      print(row)

# COMMAND ----------

# MAGIC %sql select * from default.diamonds_copy_sql
