# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Master Notebook Execution
# MAGIC ###### Author: Eddie Edgeworth 4/4/19
# MAGIC ###### Modified for CLA: Mike Sherrill 6/17/19
# MAGIC 
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

# MAGIC %run ../Framework/Secrets_Databricks_Container

# COMMAND ----------

#get values passed into notebook parameters and set variables. 
dbutils.widgets.text(name="purviewEnabled", defaultValue="No", label="Purview Enabled")

purviewEnabled = dbutils.widgets.get("purviewEnabled")

# COMMAND ----------

# MAGIC %run ../Framework/Neudesic_Framework_Functions $purviewEnabled = purviewEnabled

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

#get values passed into notebook parameters and set variables. 
dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="notebookExecutionName", defaultValue="Client000000001_Dataset1_Enrich", label="Notebook Execution Name")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
notebookExecutionName = dbutils.widgets.get("notebookExecutionName")

# COMMAND ----------

pipeLineName = "Pipeline - Master Notebook Execution for Notebook Execution Name: {0}".format(notebookExecutionName)
pipeLineExecutionLogKey = log_event_pipeline_start(pipeLineName,parentPipeLineExecutionLogKey)
print("Pipeline Execution Log Key: {0}".format(pipeLineExecutionLogKey))

# COMMAND ----------

pipeLineName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Metadata from Framework DB

# COMMAND ----------

try:
  p = get_notebook_parameters(notebookExecutionName).collect()
  notebookKey, notebookName, parametersString = p[0]
  parameters = {}
  stringifiedJson = eval(parametersString)
  for r in stringifiedJson:
    k = r['pKey']
    v = r['pValue']
    parameters[k] = v
except Exception as e:
  errorString = "Framework Database is not populated for Notebook {0}.".format(notebookExecutionName)
  sourceName = "Master Notebook Execution: Get Notebook Parameters"
  errorCode = 102
#  errorDescription = e.message
#  log_event_pipeline_error(pipeLineExecutionLogKey,sourceName,errorCode,errorDescription)
  raise ValueError(errorString)

# COMMAND ----------

containerName = parameters.get('containername')
queryZoneNotebookPath = ".." + parameters.get('queryZoneNotebookPath').replace("\\","")
summaryZoneNotebookPath = ".." + parameters.get('summaryZoneNotebookPath','').replace("\\","")
sanctionedZoneNotebookPath = ".." + parameters.get('sanctionedZoneNotebookPath','').replace("\\","")
currentStatePath = parameters.get('containername') + "/Query/CurrentState/" + parameters['queryZoneTableName']
summaryExportPath = parameters.get('containername') + "/Summary/Export/" + parameters['queryZoneTableName']
queryZoneTableName = parameters['queryZoneTableName']
fullyQualifedTableName = parameters['queryZoneSchemaName'] + "." + parameters['queryZoneTableName']
numPartitions = parameters.get('numPartitions',8)
primaryKeyColumns = parameters.get('primaryKeyColumns','')
timestampColumns = parameters.get('timestampColumns','')

# COMMAND ----------

print('')
print("Query Zone Notebook Path: {0}".format(queryZoneNotebookPath))
print("Summary Zone Notebook Path: {0}".format(summaryZoneNotebookPath))
print("Sanctioned Zone Notebook Path: {0}".format(sanctionedZoneNotebookPath))
print('')
print("Contaner Name: {0}".format(containerName))
print('')
print("Current State Path: {0}".format(currentStatePath))
print("Query Zone Table Name: {0}".format(queryZoneTableName))
print("Query Zone Fully Qualified Table Name: {0}".format(fullyQualifedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Primary Key Columns: {0}".format(primaryKeyColumns))
print("Timestamp Columns: {0}".format(timestampColumns))
print('')
print("Summary Export Path: {0}".format(summaryExportPath))
print("Summary Zone Remove HDFS Output Committer Files: {0}".format(parameters["summaryZoneRemoveHDFSOutputCommitterFiles"]))
print("Summary Zone Rename Summary Output File: {0}".format(parameters["summaryZoneRenameSummaryOutputFile"]))
print('')

print('')
print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query Zone Processing

# COMMAND ----------

if queryZoneNotebookPath != '..':
  run_with_retry(queryZoneNotebookPath, 0, {"parentPipeLineExecutionLogKey": pipeLineExecutionLogKey, "containerName": containerName, "schemaName": parameters['queryZoneSchemaName'], "tableName": parameters['queryZoneTableName'], "vacuumRetentionHours": parameters['vacuumRetentionHours'], "combineFiles": parameters['combineFiles'], "numPartitions": numPartitions, "primaryKeyColumns": primaryKeyColumns, "timestampColumns": timestampColumns})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary Zone Processing

# COMMAND ----------

if summaryZoneNotebookPath != '..':
  run_with_retry(summaryZoneNotebookPath, 0, {"parentPipeLineExecutionLogKey": pipeLineExecutionLogKey, "containerName": containerName, "schemaName": parameters['queryZoneSchemaName'], "tableName": parameters['queryZoneTableName'], "removeHDFSOutputCommitterFiles": parameters['summaryZoneRemoveHDFSOutputCommitterFiles'], "renameSummaryOutputFile": parameters['summaryZoneRenameSummaryOutputFile']})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sanctioned Zone Processing

# COMMAND ----------

if sanctionedZoneNotebookPath != '..':
  run_with_retry(sanctionedZoneNotebookPath, 1200, {"parentPipeLineExecutionLogKey": pipeLineExecutionLogKey, "containerName": containerName, "schemaName": parameters['queryZoneSchemaName'], "tableName": parameters['queryZoneTableName'], "numPartitions": numPartitions})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Completed
# MAGIC val logMessage = "Completed"
# MAGIC val notebookContext = ""
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

log_event_pipeline_end(pipeLineExecutionLogKey, "Succeeded", pipeLineName, "")
dbutils.notebook.exit("Succeeded")