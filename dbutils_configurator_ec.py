import requests
import traceback
import uuid
import time

# This should work on cluster OR from databricks-connect, but will return a dbutils
# instance that has a token setup for secrets API use in either case
# This version uses v1.2 of the api, and execution context API
def getConfiguredDbutils(mySpark, timeoutSeconds=120, forceReload=False):
    print("Getting and validating dbutils")

    # get dbutils two ways, whatever works
    def getDbutils(spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

    # Run a command on the configured cluster to fetch the apiToken from
    # the notebook context
    def getSecretToken(spark):
        # get the clusterID and host db-connect is configured for
        clusterId = spark.conf.get("spark.databricks.service.clusterId")
        # service.address has the org in it too, which the API doesn't like
        dbHost = spark.conf.get("spark.databricks.service.address").split("?")[0]
        Usr = 'token'
        pas = spark.conf.get('spark.databricks.service.token')

        apiToken = None

        try:
            create_cmdExec = {
                "language": "python",
                "clusterId": clusterId
            }

            # Create a context
            rq2 = requests.post(dbHost + "api/1.2/contexts/create", auth=(Usr, pas), json=create_cmdExec)

            contextId = rq2.json()['id']

            commandString = '(".".join(list(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())))'

            runCmd = {"language": "python", "clusterId": clusterId, "contextId": contextId, "command": commandString}

            # Submit the command execution
            rq4 = requests.post(dbHost + "api/1.2/commands/execute", auth=(Usr, pas), json=runCmd)

            commandId = rq4.json()['id']

            keepPolling = True

            current_milli_time = lambda: int(round(time.time() * 1000))

            timeStart = current_milli_time()

            # Poll for the results
            while keepPolling:

                rq5 = requests.get(
                    dbHost + "api/1.2/commands/status?clusterId=" + clusterId + "&contextId=" + contextId + "&commandId=" + commandId,
                    auth=(Usr, pas))

                cmdStatusObject = rq5.json()

                if cmdStatusObject['status'] == 'Finished':
                    keepPolling = False
                    cmdResults = cmdStatusObject['results']
                    apiTokenWrapper = cmdResults['data']
                    firstTick = apiTokenWrapper.find("'")
                    lastTick = apiTokenWrapper.find("'", firstTick + 1)
                    apiTokenMunged = apiTokenWrapper[firstTick + 1:lastTick]
                    apiToken = apiTokenMunged.replace('.', '')
                else:
                    if (current_milli_time() - timeStart) / 1000 > timeoutSeconds:
                        keepPolling = False
                        apiToken = 'TIMEDOUT'

        except Exception as e:
            print("Exception while fetching new apiToken")
            print(e)
            apiToken = 'FAILED'
        finally:
            destroyParams = {"contextId": contextId, "clusterId": clusterId}
            try:
                requests.post(dbHost + "api/1.2/contexts/destroy", auth=(Usr, pas), json=destroyParams)
            except Exception as e2:
                e2String = str(e2)
            return apiToken

    # get our base dbutils instance
    myDbutils = getDbutils(mySpark)
    # the token is cached and works for 48 hours, use forceReload to break it
    if forceReload:
        try:
            myDbutils.secrets.setToken("dkeaxxxxxxxxx")
            print("forceReload=True, invalidated existing token")
        except:
            print("forceReload=True when running on cluster has no effect")

    phoneyScope = str(uuid.uuid4())
    phoneyKey = str(uuid.uuid4())

    try:
        # test our instance by inspecting the exception stacktrace from get call
        myDbutils.secrets.get(phoneyScope, phoneyKey)

    except Exception as e:
        track = traceback.format_exc()

        if "RESOURCE_DOES_NOT_EXIST" in track:
            # We made the call, but the key wasn't there - we should be good
            print("Dbutils already has a valid apiToken")
        else:
            # We failed to execute the secrets.get call, need a token most likely
            print("Fetching token to enable dbutils secrets, this will take a few seconds")

            secretToken = getSecretToken(mySpark)

            if secretToken == 'FAILED':
                print("Something went wrong, dbutils secrets api will not be configured")
            elif secretToken == 'TIMEDOUT':
                print("Took too long to get apiToken, secrets api will not be configured")
            elif secretToken[:4] != 'dkea':
                print("Malformed token received")
            else:
                print("Got refreshed apiToken for dbutils secrets api")
                myDbutils.secrets.setToken(secretToken)
    finally:
        return myDbutils
