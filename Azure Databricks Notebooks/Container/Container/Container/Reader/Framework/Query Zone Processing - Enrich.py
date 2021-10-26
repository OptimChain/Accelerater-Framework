# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Enrich
# MAGIC ###### Author: Mike Sherrill 6/21/19
# MAGIC 
# MAGIC Data Lake pattern for master data or small tables that can be overwritten every time.  Takes a file from the raw data path and overwrites the table in the Query zone.      
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Data must exist in the Data Lake /query zone (current State).   
# MAGIC 
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize Framework

# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="client000000001", label="Container Name")
containerName = dbutils.widgets.get("containerName")

# COMMAND ----------

# MAGIC %run ./Framework/Secrets_Databricks_Container

# COMMAND ----------

# MAGIC %run ./Framework/Neudesic_Framework_Functions

# COMMAND ----------

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="schema", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="EE_MeterData", label="Table Name")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
fullPathPrefix = "abfss://" + containerName + "@" + adlsGen2StorageAccountName + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName
currentStatePath = fullPathPrefix + "/Query/CurrentState/" + tableName
enrichedPath = fullPathPrefix + "/Query/Enriched/" + tableName


# COMMAND ----------

notebookName = "Query Zone Processing - Enrich"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Current State Path: {0}".format(currentStatePath))
print("Enriched State Path: {0}".format(enrichedPath))

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data from Query Zone (CurrentState}

# COMMAND ----------

try:
  #read currentState data path into a dataframe, using the schema.  Write any rows unable to be processed to badRecordsPath
  raw_df = spark.read \
    .format("delta") \
    .load(currentStatePath) \
    .dropDuplicates()
except Exception as e:
  sourceName = "Query Zone Processing - Enrich: Read Data from CurrentState"
  errorCode = "200"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleanse Columns
# MAGIC * remove special characters and spaces from column names 

# COMMAND ----------

try:
  cleansed_df = column_naming_convention(raw_df)
except Exception as e:
  sourceName = "Query Zone Processing - Enrich: Cleanse Columns"
  errorCode = "300"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

#rename columns. 
enrichedState = cleansed_df \
  .withColumnRenamed('UpdateDate', 'LastUpdateDate')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Query Zone (Enriched)

# COMMAND ----------

try:
  cleansed_df \
    .repartition(numPartitions) \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save(enrichedPath)
except Exception as e:
  sourceName = "Query Zone Processing - Overwrite: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from client000000002_hospital_components

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