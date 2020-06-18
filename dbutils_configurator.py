import time
import base64
from databricks_api import DatabricksAPI

# This should work on cluster OR from databricks-connect, but will return a dbutils
# instance that has a token setup for secrets API use in either case
def getConfiguredDbutils(mySpark, dbUserId, testScope, testKey, forceReload=False):
    print("Getting and validating dbutils")

    # get dbutils two ways, whatever works
    def getDbutils(myspark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(myspark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

    # Run a one line notebook on cluster to get the apiToken from
    # the notebook context
    def getSecretToken(spark, dbUserId):
        # get the clusterID and host db-connect is configured for
        clusterId = spark.conf.get("spark.databricks.service.clusterId")
        # service.address has the org in it too, which the API doesn't like
        dbHost = spark.conf.get("spark.databricks.service.address").split("?")[0]
        pat = spark.conf.get('spark.databricks.service.token')

        # get our client for running API requests
        db = DatabricksAPI(
            host=dbHost,
            token=pat
        )

        # upload the notebook code required to fetch the token to our workspace
        # create a "tmp" folder under our user folder
        # import the notebook there
        notebookDir = "/Users/{}/tmp".format(dbUserId)
        notebookPath = notebookDir + "/fetch_apiToken"

        try:
            # Create a tmp folder under the user dir in workspace, if not exists
            db.workspace.mkdirs(notebookDir)

            # Import a one line python notebook to get and return the apiToken
            notebookData = b'# Databricks notebook source\n' + \
                           b'dbutils.notebook.exit' + \
                           b'((".".join(list(dbutils.notebook.entry_point.getDbutils()' + \
                           b'.notebook().getContext().apiToken().get()))))'

            encodedNotebookData = base64.b64encode(notebookData)
            encodedNotebookDataStr = encodedNotebookData.decode("utf-8")

            db.workspace.import_workspace(content=encodedNotebookDataStr,
                                          path=notebookPath,
                                          language='PYTHON',
                                          overwrite=True,
                                          format='SOURCE')

            # Submit a "runs/submit" call for the fetch_apiToken notebook
            notebookTask = {"notebook_path": notebookPath}

            submitResponse = db.jobs.submit_run(
                run_name='fetch_apiToken',
                existing_cluster_id=clusterId,
                notebook_task=notebookTask,
                timeout_seconds=120,
            )

            # Wait for the runs/submit job to finish, so we can get the result
            runId = submitResponse['run_id']

            jobRunning = True
            jobOutput = None

            # check for results every 1 second until it's done
            while jobRunning:
                time.sleep(1)
                output = db.jobs.get_run_output(runId)
                state = output['metadata']['state']['life_cycle_state']
                # print(state)
                if state not in ['RUNNING', 'PENDING', 'TERMINATING']:
                    jobRunning = False
                    result_state = output['metadata']['state']['result_state']
                    if result_state == 'SUCCESS':
                        jobOutput = output['notebook_output']['result']
                    else:
                        jobOutput = "FAILED"

            # return our token
            # it comes with . between each char, remove them
            return jobOutput.replace('.', '')

        except Exception as e:
            print("Exception while importing and executing tmp/fetch_apiToken notebook")
            print(e)

    try:
        # get our base dbutils instance
        myDbutils = getDbutils(mySpark)
        # the token is cached and works for 48 hours, use forceReload to break it
        if forceReload:
            try:
                myDbutils.secrets.setToken("dkeaxxxxxxxxx")
                print("forceReload=True, invalidated existing token")
            except:
                print("forceReload=True when running on cluster has no effect")

        # test our instance to see if we can use it to retrieve the testKey
        myDbutils.secrets.get(testScope, testKey)
        print("Dbutils configured to use secrets already")
    except Exception as e:
        print("Fetching token to enable dbutils secrets, this will take a few seconds")
        secretToken = getSecretToken(mySpark, dbUserId)

        # TODO: grab the URL to the run and print it here
        if secretToken == 'FAILED':
            print("Something went wrong, dbutils secrets api will not be configured")
        elif secretToken[:3] != 'dke':
            print("Malformed token received")
            print("Please run the tmp/fetch_dbutils_token notebook directly to debug")
        else:
            print("Got refreshed apiToken for dbutils secrets api")
            myDbutils.secrets.setToken(secretToken)
    finally:
        return myDbutils
