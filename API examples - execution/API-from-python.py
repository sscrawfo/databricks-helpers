# Databricks notebook source
# MAGIC %md ## Example code for using cli package to make API calls via Python
# MAGIC ### You will need internet access to run this code, don't run from a workspace, run from your laptop

# COMMAND ----------

# DBTITLE 1,Install the Databricks CLI so we can use the python SDK
# MAGIC %pip install databricks-cli

# COMMAND ----------

# DBTITLE 1,Install Azure CLI - so we can get an AAD token
# MAGIC %sh curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# COMMAND ----------

# DBTITLE 1,Authenticate our Azure CLI
# MAGIC %sh az login

# COMMAND ----------

# DBTITLE 1,use Azure CLI to generate an AAD token for access to the Databricks service
# MAGIC %sh
# MAGIC az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query "accessToken" --output tsv > /databricks/driver/aad_token.txt

# COMMAND ----------

# MAGIC %sh cat /databricks/driver/aad_token.txt

# COMMAND ----------

# DBTITLE 1,Import the APIs we want to explore, create the client, and test via list_clusters()
from databricks_cli.sdk.api_client import ApiClient

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath

with open('/databricks/driver/aad_token.txt', 'r') as file:
    aad_token = file.read().rstrip()

pat_token = "dapiXXXXXXXXXXXXXXXX"

# You can use aad_token or pat_token either one here
api_client = ApiClient(
  host  = "https://adb-XXXXXXXX.1.azuredatabricks.net",
  token = aad_token #Don't hardcode tokens in your code, use a vault or something
)

clusters_api = ClusterApi(api_client)

clusters_api.list_clusters()

# COMMAND ----------

# DBTITLE 1,Example using RunsAPI to do a Runs Submit
runs_api = RunsApi(api_client)

run_submit_json = {
  "run_name": "my spark task - instance pool",
  "new_cluster": {
    "num_workers": 0,
    "spark_conf": {
        "spark.master": "local[*, 4]",
        "spark.databricks.cluster.profile": "singleNode"
    },
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "spark_version": "11.3.x-scala2.12",
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "instance_pool_id": "0302-145232-tidal51-pool-oj32ps4f"
  },
  "spark_python_task": {
    "python_file": "dbfs:/tmp/steven.crawford@databricks.com/python_hello.py"
  }
}

run_id = runs_api.submit_run(run_submit_json)["run_id"]

print(run_id)

# COMMAND ----------

# DBTITLE 1,Get the status of a run
runs_api.get_run(run_id)

# COMMAND ----------

# DBTITLE 1,Get the output for a run
runs_api.get_run_output(run_id)

# COMMAND ----------

# DBTITLE 1,Example using DBFS API
dbfs_api = DbfsApi(api_client)

myPath = DbfsPath("dbfs:/tmp/steven.crawford@databricks.com/")

files = dbfs_api.list_files(myPath)

for file in files:
  print(file.dbfs_path)

# COMMAND ----------

# MAGIC %md #Example using DBFS api to upload code file from client to DBFS (so that it can be executed via runs_submit)

# COMMAND ----------

# DBTITLE 1,Make a local file to use as our source (client code should have a local file for code to run)
dbutils.fs.cp("dbfs:/tmp/steven.crawford@databricks.com/python_hello.py","file:/databricks/driver/python_hello.py")

# COMMAND ----------

# MAGIC %sh cat /databricks/driver/python_hello.py

# COMMAND ----------

# DBTITLE 1,Use the API to copy a local file to DBFS
dbfs_api.put_file(src_path="/databricks/driver/python_hello.py",dbfs_path=DbfsPath("dbfs:/tmp/steven.crawford@databricks.com/api/python_hello.py"),overwrite=True)

# COMMAND ----------

# DBTITLE 1,Validate the new file copied into DBFS
for file in dbutils.fs.ls("dbfs:/tmp/steven.crawford@databricks.com/api/"):
  print(file)
