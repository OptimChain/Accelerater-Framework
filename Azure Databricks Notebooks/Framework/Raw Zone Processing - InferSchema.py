# MAGIC  
# MAGIC %md
# MAGIC 
# MAGIC # Infer Schema
# MAGIC ###### Author: Eddie Edgeworth 9/6/18
# MAGIC ###### Modifed for CLA - Mike Sherrill 6/19/19
# MAGIC 
# MAGIC The objective of this notebook is to obtain the schema for a supplied data lake path and save it to the ** /schemas ** folder of the data lake so that it can be retrieved for use in subsequent imports of the table.  When Data Lake files are read into databricks, by default it will infer the schema, but this comes at the expense of extra Spark jobs.  
# MAGIC 
# MAGIC For operationalized/production data pipelines, the Best practice is to supply a schema when reading:<br>  
# MAGIC 
# MAGIC df = spark \<br>
# MAGIC   .read \<br>
# MAGIC  ** .schema(schema) \ ** <br>
# MAGIC   .json(path) <br>
# MAGIC 
# MAGIC 
# MAGIC #### Example:
# MAGIC For supplied path ** /raw/sales/{year}/{month}/{day} **
# MAGIC 
# MAGIC This notebook will infer the schema one time, and then save that to ** /schemas/raw/sales/schema.json ** so that it can be used on every subsequent query of the folder.  
# MAGIC 
# MAGIC #### Usage:
# MAGIC 
# MAGIC Call this notebook supplying the parameters at the top.  
# MAGIC   * ** Data Lake Raw Data Path: ** The path to the directory where files are located.  Can be either a single file or a directory of files. 
# MAGIC   * ** Table Name: ** The Table Name in the Hive Metastore. 
# MAGIC   * ** Sampling Ratio: ** Percentage of the file(s) to sample.  If the files are not uniform (e.g. same schema in all, increase this up to 1 to fully scan the data).
# MAGIC 
# MAGIC A function in the notebook ** Neudesic_Framework_Functions** wraps this functionality:
# MAGIC 
# MAGIC     get_table_schema(@rawDataPath, @tableName)
# MAGIC       schema = <attempt to read in the schema from /schemas/tablename/schema.json>
# MAGIC       if schema not found, run this InferSchema notebook to obtain it the first time:
# MAGIC         dbutils.notebook.run(".../InferSchema", 60, {"rawDataPath": "", "tableName": ""})
# MAGIC       returns schema
# MAGIC 
# MAGIC CurrentStateProcessing notebook
# MAGIC     @rawDataPath
# MAGIC     @tableName
# MAGIC   
# MAGIC   schema = get_table_schema(@rawDataPath, @tableName)
# MAGIC   
# MAGIC         

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

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="containerName", defaultValue="client0000001", label="Container Name")
dbutils.widgets.text(name="dataPath", defaultValue="/Raw/Dataset1", label="Data Path")
dbutils.widgets.text(name="tableName", defaultValue="Dataset1", label="Table Name")
dbutils.widgets.text(name="samplingRatio", defaultValue=".5", label="Sampling Ratio")
dbutils.widgets.text(name="delimiter", defaultValue=",", label="File Delimiter")
dbutils.widgets.text(name="hasHeader", defaultValue="False", label="Header Row")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
dataPath = dbutils.widgets.get("dataPath")
delimiter = dbutils.widgets.get("delimiter")
hasHeader = dbutils.widgets.get("hasHeader")
fullDataPath = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" + dataPath
tableName = dbutils.widgets.get("tableName")
samplingRatio = float(dbutils.widgets.get("samplingRatio"))
schemasPath = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" + "/Schemas/" + tableName
schemaFile = schemasPath + "/Schema.json"

# COMMAND ----------

schemasPath

# COMMAND ----------

notebookName = "Raw Zone Processing - InferSchema"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Data Path: {0}".format(dataPath))
print("Full Data Path: {0}".format(fullDataPath))
print("Delimiter: {0}".format(delimiter))
print("Has Header: {0}".format(hasHeader))
print("Sampling Ratio: {0}".format(samplingRatio))
print("Schemas Path: {0}".format(schemasPath))
print("Schema File: {0}".format(schemaFile))
print("Table Name: {0}".format(tableName))

# COMMAND ----------

#fullDataPath = fullDataPath + "/" + tableName + ".json"
#fullDataPath

# COMMAND ----------

# MAGIC %md
# MAGIC #### Attempt to Sample Source and Infer Schema

# COMMAND ----------

try:
  df = spark \
    .read \
    .option("Header", hasHeader) \
    .options(samplingRatio=samplingRatio) \
    .json(fullDataPath)
except Exception as e:
  sourceName = "Raw Zone Processing - InferSchema: Attempt to Sample Source and Infer Schema"
  errorCode = "200"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert Schema to JSON and Save to Data Lake

# COMMAND ----------

try:
  schema = df.schema
  schema_json = df.schema.json()
  dbutils.fs.mkdirs(schemasPath)
  dbutils.fs.put(schemaFile, schema_json, True)
except Exception as e:
  sourceName = "Raw Zone Processing - InferSchema: Convert Schema to JSON and Save to Data Lake"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

schema

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

log_event_notebook_end(notebookExecutionLogKey=notebookExecutionLogKey, notebookStatus="SUCCEEDED", notebookName=notebookName, notebookExecutionGroupName="")
dbutils.notebook.exit("Succeeded")