# databricks-helpers
Misc Code for use in Databricks

## dbutils_configurator

dbutils_configurator.py provides a function for returning an instance of dbutils that will be properly configured for use of the secrets API regardless if run via databricks-connect, or on cluster.

If the secrets API does not have a valid apiToken set, this helper will run a one-line python notebook on your cluster to fetch a new token and set it on the dbutils instance.  The dbUserId will be used to determine a safe location to upload the notebook to (a /tmp folder under the user dir).  The test scope and key are used to determine whether a valid token has already been set (they are good for 48 hours).  forceReload will always invalidate the token, and fetcha new one, and should be ommitted normally (or set to False)

This requires the DatabricksAPI, and to use it, you must install the databricks-api PyPI package

The example:

```python
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
                          testKey="ssc-app-key")

theSecret = dbutils.secrets.get("ssc-secrets", "ssc-app-key")
```

## dbutils_configurator_ec

This does the same thing, but uses the execution context API rather than uploading a notebook.
This version does not require passing in a userId, and also doesn't require a scope/key for testing.
Much simpler usage, but it does depend on Databricks API v1.2.  Also, it does not rely on databricks-api package.

An Example:

```python
from pyspark.sql import SparkSession
import dbutils_configurator_ec

spark = SparkSession \
    .builder \
    .appName("db-connect-test") \
    .config("spark.metrics.namespace", "db-connect-test") \
    .getOrCreate()

# This code is intended for use with databricks-connect, but can run on cluster too
# for real usage, set forceReload=False (or omit it altogether, as it defaults to False)
# default timeoutSeconds is 120 seconds
dbutils = dbutils_configurator_ec.getConfiguredDbutils(spark,
                                                       timeoutSeconds=30,
                                                       forceReload=False)

theSecret = dbutils.secrets.get("ssc-secrets", "ssc-app-key")
```

