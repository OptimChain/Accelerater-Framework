# MAGIC  
# MAGIC %md
# MAGIC # Master Notebook Execution List
# MAGIC ###### Author: Eddie Edgeworth 4/4/19
# MAGIC 
# MAGIC 
# MAGIC #### Usage
# MAGIC 
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Azure SQL Framework Database Metatadata must be populated 
# MAGIC 
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run ./Secrets-Databricks-Cache

# COMMAND ----------

# MAGIC %run ./Neudesic_Framework_Functions

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

#get values passed into notebook parameters and set variables. 
dbutils.widgets.text(name="containerExecutionGroupName", defaultValue="Client000000001", label="Container Execution Group Name")
dbutils.widgets.text(name="dateToProcess", defaultValue="", label="Date to Process (yyyy/mm/dd)")
containerExecutionGroupName = dbutils.widgets.get("containerExecutionGroupName")
dateToProcess = dbutils.widgets.get("dateToProcess")
print("System Execution Group Name: {0}".format(containerExecutionGroupName))
print("Date to Process: {0}".format(dateToProcess))

# COMMAND ----------

pipeLineName = "Pipeline - Master Notebook Execution List for System Execution Group Name: {0}".format(containerExecutionGroupName)
pipeLineExecutionLogKey = log_event_pipeline_start(pipeLineName,parentPipelineExecutionLogKey=0)
print("Pipeline Execution Log Key: {0}".format(pipeLineExecutionLogKey))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Notebook Execution Group List

# COMMAND ----------

try:
  notebookExecutionGroupList = get_container_notebook_execution_list(containerExecutionGroupName).collect()
  notebookExecutionGroupNames = [n.ExecutionGroupName for n in notebookExecutionGroupList]
  print(notebookExecutionGroupNames)
except Exception as e:
  sourceName = "Master Notebook Execution List: Get Notebook Execution Group List"
  errorCode = 101
  errorDescription = e.message
  log_event_pipeline_error(pipeLineExecutionLogKey,sourceName,errorCode,errorDescription)
  raise e

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Notebook Executions and Run Individual Notebooks
# MAGIC * The ThreadPool controls how many processes run concurrently.  

# COMMAND ----------

#notebookList = get_notebook_execution_list(notebookExecutionGroupNames)
#print(notebookExecutionGroupNames)


# COMMAND ----------

from multiprocessing.pool import ThreadPool
pool = ThreadPool(7)
notebook = "Master Notebook Execution"
timeout = 600

for notebookExecutionGroupName in notebookExecutionGroupNames:
  notebookExecutionList = get_notebook_execution_list(notebookExecutionGroupName).collect()
  notebookExecutionNames = [n.NotebookName for n in notebookExecutionList]
  pool.map(
  lambda notebookExecutionName: run_with_retry(notebook, timeout, args = {"parentPipeLineExecutionLogKey": pipeLineExecutionLogKey, "notebookExecutionName": notebookExecutionName, "dateToProcess": dateToProcess}, max_retries = 3), notebookExecutionNames
  )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completed

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Completed
# MAGIC val logMessage = "Completed"
# MAGIC val notebookContext = ""
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

log_event_pipeline_end(pipeLineExecutionLogKey, "SUCCEEDED", pipeLineName, "")
reset_notebook_execution_list(containerExecutionGroupName)
dbutils.notebook.exit("Succeeded")