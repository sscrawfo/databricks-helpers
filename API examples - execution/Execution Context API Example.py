# Databricks notebook source
# MAGIC %md ### Python Example Code for using the Execution Context API to run Spark code on a Databricks Cluster

# COMMAND ----------

# DBTITLE 1,runCommand funtition - command is pyspark code
import requests
import time

# Run a command on the configured cluster
def runCommand(command):
  
    # You'll need to set clusterId, dbHost, and pas (PAT token or AAD token)
  
    # this is the clusterID of the cluster you want to use
    clusterId = "0211-000352-p68oscp5"
    
    # URL to the Azure Databricks Workspace
    dbHost = "https://adb-XXXXXXXXXX.1.azuredatabricks.net/"
    
    # token for authentication to the workspace (use a vault)
    pas = "dapiXXXXXXXXXXXXXXXXX"
    
    Usr = "token"

    timeoutSeconds = 100

    rawResult = None

    try:
        create_cmdExec = {"language": "python", "clusterId": clusterId}

        # Create a context
        rq2 = requests.post(
            dbHost + "api/1.2/contexts/create", auth=(Usr, pas), json=create_cmdExec
        )

        contextId = rq2.json()["id"]

        print(contextId)

        status = 'null'

        keepPolling = True

        # Wait for EC to be out of pending state
        while(keepPolling):

            rq2b = requests.get(
                dbHost + "api/1.2/contexts/status?clusterId=" + clusterId + "&contextId=" + contextId, auth=(Usr, pas)
            )

            print(rq2b.json())

            status = rq2b.json()["status"]

            if status == 'Pending':
                keepPolling = True
                time.sleep(1)
            else:
                keepPolling = False

        # Run our command only if the EC is Running
        if status == 'Running':

            commandString = command

            print(commandString)

            runCmd = {
                "language": "python",
                "clusterId": clusterId,
                "contextId": contextId,
                "command": commandString,
            }

            # Submit the command execution
            rq4 = requests.post(
                dbHost + "api/1.2/commands/execute", auth=(Usr, pas), json=runCmd
            )

            commandId = rq4.json()["id"]

            keepPolling = True

            current_milli_time = lambda: int(round(time.time() * 1000))

            timeStart = current_milli_time()

            # Poll for the command to be Finished and fetch results
            while keepPolling:

                rq5 = requests.get(
                    dbHost
                    + "api/1.2/commands/status?clusterId="
                    + clusterId
                    + "&contextId="
                    + contextId
                    + "&commandId="
                    + commandId,
                    auth=(Usr, pas),
                )

                cmdStatusObject = rq5.json()

                print(cmdStatusObject)

                if cmdStatusObject["status"] == "Finished":
                    keepPolling = False
                    cmdResults = cmdStatusObject["results"]
                    rawResult = cmdResults["data"]
                else:
                    if (current_milli_time() - timeStart) / 1000 > timeoutSeconds:
                        keepPolling = False
                        rawResult = "TIMEDOUT"
                    else:
                        time.sleep(1)

        else:
            print("Execution Context status not running:" + status)

    except Exception as e:
        print("Exception while running command")
        print(e)
        rawResult = "FAILED"
    finally:
        destroyParams = {"contextId": contextId, "clusterId": clusterId}
        try:
            requests.post(
                dbHost + "api/1.2/contexts/destroy", auth=(Usr, pas), json=destroyParams
            )
        except Exception as e2:
            e2String = str(e2)
        return rawResult




# COMMAND ----------

# DBTITLE 1,Hello World Example

print(runCommand("print('hello world')"))

# COMMAND ----------

# MAGIC %md ### These examples assume you have a table named diamonds in your default catalog.schema

# COMMAND ----------

dbutils.fs.rm('/tmp/delta/diamonds_tmp',True)

# COMMAND ----------

spark.sql("select * from diamonds").write.format("delta").mode("overwrite").save("/tmp/delta/diamonds_tmp")

# COMMAND ----------

display(spark.sql("select * from delta.`/tmp/delta/diamonds_tmp`"))

# COMMAND ----------

dbutils.fs.rm('/tmp/delta/diamonds_tmp',True)

# COMMAND ----------

# DBTITLE 1,Use Execution Context to run SQL and write results
runCommand("spark.sql('select * from diamonds').write.format('delta').mode('overwrite').save('/tmp/delta/diamonds_tmp')")

# COMMAND ----------

display(spark.sql("select * from delta.`/tmp/delta/diamonds_tmp`"))

# COMMAND ----------

dbutils.fs.rm('/tmp/delta/diamonds_tmp',True)

# COMMAND ----------

runCommand("spark.sql('select * from delta.`/tmp/delta/diamonds_tmp`')")
