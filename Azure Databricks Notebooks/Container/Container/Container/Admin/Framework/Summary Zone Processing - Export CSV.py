# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Summary Zone Table
# MAGIC ###### Author: Eddie Edgeworth 1/3/19
# MAGIC 
# MAGIC This notebook takes a Query Zone Table as input and creates a .csv file in the Summary Zone.    
# MAGIC 
# MAGIC #### Usage
# MAGIC * Pass in the Table Name as a parameter. 
# MAGIC * Pass in the Summary Path as a parameter. 
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC Query Zone processing for this table should be complete (or should already be a registered table in the schema catalog)
# MAGIC 
# MAGIC #### Details
# MAGIC This repartitions the data as a single file to facilitate querying by external tools such as Power BI, which is slower than allowing it to write files in parallel. If you are okay with partitioned output files you can modify this below (repartiton(n))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run ../Framework/Secrets_Databricks_Container

# COMMAND ----------

# MAGIC %run ../Framework/Neudesic_Framework_Functions

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
dbutils.widgets.text(name="summaryPath", defaultValue="/Summary/Export/", label="Summary Path")
dbutils.widgets.text(name="removeHDFSOutputCommitterFiles", defaultValue="0", label="Remove Committer Files")
dbutils.widgets.text(name="renameSummaryOutputFile", defaultValue="0", label="Rename Output File")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
fullPathPrefix = "abfss://" + containerName + "@" + adlsGen2StorageAccountName + ".dfs.core.windows.net" 

schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName

summaryPath = dbutils.widgets.get("summaryPath")
#fullSummaryPath = "adl://" + adlsGen1StorageAccountName + ".azuredatalakestore.net" + summaryPath
fullSummaryPath = fullPathPrefix + summaryPath + tableName

summaryFileName = fullSummaryPath + tableName + "/"

currentStatePath = fullPathPrefix + "/Query/CurrentState/" + tableName
badRecordsPath = "/BadRecords/" + tableName
fullBadRecordsPath = fullPathPrefix + badRecordsPath

removeHDFSOutputCommitterFiles = dbutils.widgets.get("removeHDFSOutputCommitterFiles")
renameSummaryOutputFile = dbutils.widgets.get("renameSummaryOutputFile")

enrichedPath = fullPathPrefix + "/Query/Enriched/" + tableName

# COMMAND ----------

fullSummaryPath
#summaryPath = adlbasepath + "/Summary/Export/" + tableName
#summaryFileName = adlbasepath + summaryPath + tableName + "/"

# COMMAND ----------

notebookName = "Summary Zone Processing - Export CSV"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Summary Path: {0}".format(summaryPath))
print("Remove HDFS Output Committer Files: {0}".format(removeHDFSOutputCommitterFiles))
print("Rename Summary Output File to Table Name: {0}".format(renameSummaryOutputFile))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build Dataframe from Existing Table

# COMMAND ----------

try:
  #read currentState data path into a dataframe, using the schema.  Write any rows unable to be processed to badRecordsPath
  raw_df = spark.read \
    .format("delta") \
    .load(enrichedPath) 
except Exception as e:
  sourceName = "Query Zone Processing - Enrich: Read Data from CurrentState"
  errorCode = "200"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

display(raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write CSV To Summary Zone

# COMMAND ----------

#Displaying the files that are in this location. ADLS Gen1
#dbutils.fs.ls("adl://dltestpocstoragegen1.azuredatalakestore.net/Client0000001/Extract")
fullSummaryPath
#dbutils.fs.ls(fullSummaryPath)

# COMMAND ----------

try:
  raw_df.repartition(1) \
    .write \
    .mode("OVERWRITE") \
    .option("header", True) \
    .csv(fullSummaryPath)
except Exception as e:
  sourceName = "Summary Zone Processing - Export CSV: Write CSV To Summary Zone"
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
    removeHDFSFileOutputCommitterFiles(fullSummaryPath, "csv")
except Exception as e:
  sourceName = "Summary Zone Processing - Export CSV: Remove Extra Committer Files"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename Summary File

# COMMAND ----------

try:
  if renameSummaryOutputFile == "1" or renameSummaryOutputFile == "True":
    files = dbutils.fs.ls(fullSummaryPath)
    csv_file = [f for f in files if f.path[-3:] == "csv"]
    fileName = csv_file[0].path.split("/")[-1]
    newFileName = tableName + ".csv"
    renameHDFSPartFile(fullSummaryPath, fileName, newFileName)
except Exception as e:
  sourceName = "Summary Zone Processing - Export CSV: Rename Summary File"
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