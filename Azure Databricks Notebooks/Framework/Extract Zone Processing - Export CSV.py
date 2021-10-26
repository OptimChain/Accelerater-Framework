# MAGIC  
# MAGIC %md
# MAGIC # Extract Zone CSV
# MAGIC 
# MAGIC This notebook takes a Enhanced Zone Table as input and creates a .csv file in the Extract Zone (for PowerBI consumption)
# MAGIC 
# MAGIC #### Usage
# MAGIC * Pass in the Table Name as a parameter. 
# MAGIC * Pass in the Extract Path as a parameter. 
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC Query Zone (Enhanced folder) processing should be complete
# MAGIC 
# MAGIC #### Details
# MAGIC This repartitions the data as a single file to facilitate querying by external tools such as Power BI, which is slower than allowing it to write files in parallel. If you are okay with partitioned output files you can modify this below (repartiton(n))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run /Framework/Secrets-Databricks-Cache

# COMMAND ----------

# MAGIC %run /Framework/Neudesic_Framework_Functions

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="client000000001", label="Container Name")
dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="schemaName", defaultValue="schema", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="Dataset1", label="Table Name")
dbutils.widgets.text(name="removeHDFSOutputCommitterFiles", defaultValue="0", label="Remove Committer Files")
dbutils.widgets.text(name="renameSummaryOutputFile", defaultValue="0", label="Rename Output File")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
fullPathPrefix = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" 

schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName

fullExtractPath = "adl://" + adlsgen1storageaccountname + ".azuredatalakestore.net" + "/" + containerName + "/Extract/" + tableName

extractFileName = fullExtractPath + tableName + "/"

removeHDFSOutputCommitterFiles = dbutils.widgets.get("removeHDFSOutputCommitterFiles")
renameExtractOutputFile = dbutils.widgets.get("renameSummaryOutputFile")

enrichedPath = fullPathPrefix + "/Query/Enriched/" + tableName


# COMMAND ----------

notebookName = "Extract Zone Processing - Export CSV"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Extract Path: {0}".format(fullExtractPath))
print("Remove HDFS Output Committer Files: {0}".format(removeHDFSOutputCommitterFiles))
print("Rename Extract Output File to Table Name: {0}".format(renameExtractOutputFile))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build Dataframe from Existing Table

# COMMAND ----------

try:
  #read currentState data path into a dataframe 
  df = spark.read \
    .format("delta") \
    .load(enrichedPath) 
except Exception as e:
  sourceName = "Extract Zone Processing - Export CSV: Read Data from Enriched"
  errorCode = "200"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

#display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write CSV To Extract Zone

# COMMAND ----------

try:
  df.repartition(1) \
    .write \
    .mode("OVERWRITE") \
    .option("header", True) \
    .csv(fullExtractPath)
except Exception as e:
  sourceName = "Extract Zone Processing - Export CSV: Write CSV To Extract Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Remove Extra Committer Files

# COMMAND ----------

try:
  if removeHDFSOutputCommitterFiles == "1" or removeHDFSOutputCommitterFiles == "True":
    removeHDFSFileOutputCommitterFiles(fullExtractPath, "csv")
except Exception as e:
  sourceName = "Extract Zone Processing - Export CSV: Remove Extra Committer Files"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename Summary File

# COMMAND ----------

try:
  if renameExtractOutputFile == "1" or renameExtractOutputFile == "True":
    files = dbutils.fs.ls(fullExtractPath)
    csv_file = [f for f in files if f.path[-3:] == "csv"]
    fileName = csv_file[0].path.split("/")[-1]
    newFileName = tableName + ".csv"
    renameHDFSPartFile(fullExtractPath, fileName, newFileName)
except Exception as e:
  sourceName = "Extract Zone Processing - Export CSV: Rename Summary File"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

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