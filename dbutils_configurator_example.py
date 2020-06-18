from pyspark.sql import SparkSession
import dbutils_configurator

spark = SparkSession \
    .builder \
    .appName("db-connect-test") \
    .config("spark.metrics.namespace", "db-connect-test") \
    .getOrCreate()

# This code is intended for use with databricks-connect
# for real usage, set forceReload=False (or omit it altogether, as it defaults to False)
# dbUserId <must> be an active user in your Databricks workspace 
# (the one db-connect is configured for)
dbutils = dbutils_configurator \
    .getConfiguredDbutils(spark,
                          dbUserId="steven.crawford@databricks.com",
                          testScope="ssc-secrets",
                          testKey="ssc-app-key",
                          forceReload=True)

theSecret = dbutils.secrets.get("ssc-secrets", "ssc-app-key")
