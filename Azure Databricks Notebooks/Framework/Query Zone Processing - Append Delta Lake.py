# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Append Delta Lake
# MAGIC ###### Author: Eddie Edgeworth 4/6/19
# MAGIC ###### Modified for CLA - Mike Sherrill 6/19/19
# MAGIC 
# MAGIC Data Lake pattern for tables with change feeds of new or updated records.  Takes a file from the raw data path and applies the updates to the Query zone.      
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Raw Data must exist in the Data Lake /raw zone in JSON format.  Incremental files must be appended to the store e.g. /raw/tablename/yyyy/mm/dd. 
# MAGIC 2. The source system must be able to produce change feeds of new or updated records. 
# MAGIC 3. The source system tables must have a primary key and this metadata must be updated in the Framework database databricks.TableMetaData
# MAGIC 
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run /Framework/Secrets-Databricks-Cache

# COMMAND ----------

# MAGIC %run /Framework/Neudesic_Framework_Functions

# COMMAND ----------

#Add the Date to the FilePath for "Day" or "Month"

dbutils.widgets.text(name="filePath", defaultValue="", label="File Path")
filePath = dbutils.widgets.get("filePath")
dbutils.widgets.text(name="filePath", defaultValue="", label="File Path")
filePath = dbutils.widgets.get("filePath")
if filePath == "Day":
  dateFilePath = str(datetime.datetime.utcnow().strftime('%Y/%m/%d/'))
elif filePath == "Month":
  dateFilePath = str(datetime.datetime.utcnow().strftime('%Y/%m/'))
else:
  dateFilePath = filePath 

dateFilePath


# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="brtl", label="Container Name")
dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")

dbutils.widgets.text(name="rawDataPath", defaultValue="/K3S/2020/", label="Raw Data Path")
dbutils.widgets.text(name="numPartitions", defaultValue="256", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="brtl", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="AssistedOrders", label="Table Name")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="", label="Vacuum Retention Hours")
dbutils.widgets.text(name="timestampColumns", defaultValue="", label="Timestamp Columns")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
fullPathPrefix = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

rawDataPath = dbutils.widgets.get("rawDataPath") + dateFilePath
fullRawDataPath = fullPathPrefix + rawDataPath
schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName
fullRawFilePath = fullRawDataPath
currentStatePath = fullPathPrefix + "/Query/CurrentState/" + tableName
badRecordsPath = "/BadRecords/" + tableName
fullBadRecordsPath = fullPathPrefix + rawDataPath
databaseTableName = containerName + "." + tableName
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")
timestampColumns = dbutils.widgets.get("timestampColumns")

# COMMAND ----------

notebookName = "Query Zone Processing - Append Delta Lake"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Raw Data Path: {0}".format(fullRawDataPath))
print("Current State Path: {0}".format(currentStatePath))
print("Bad Records Path: {0}".format(fullBadRecordsPath))

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate Raw Data Path

# COMMAND ----------

rawDataPath = validate_raw_data_path(fullRawDataPath)
if rawDataPath == "":
  dbutils.notebook.exit("Raw Data Path supplied has no valid json files to process")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Infer Schema

# COMMAND ----------

#schema = get_table_schema(containerName, rawDataPath, tableName)
#schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data From Raw Zone

# COMMAND ----------

from pyspark.sql import functions as F

if timestampColumns != '#addFileName':
  try:
  #read raw data path into a dataframe, using the schema.  Write any rows unable to be processed to badRecordsPath
    raw_df = spark.read \
      .option("badRecordsPath", badRecordsPath) \
      .json(fullRawDataPath) \
      .withColumn("dateLoaded", F.current_timestamp()) \
      .dropDuplicates()
  except Exception as e:
    sourceName = "Query Zone Processing - Merge Delta Lake: Read Data from Raw Zone"
    errorCode = "200"
    errorDescription = e.message
    log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
    raise(e)

# COMMAND ----------

from pyspark.sql.functions import input_file_name, regexp_replace

if timestampColumns == '#addFileName':
  try:
    #read currentState data path into a dataframe, using the schema.  Write any rows unable to be processed to badRecordsPath
    raw_df = spark.read \
      .json(fullRawDataPath) \
      .withColumn("filename", regexp_replace(regexp_replace(input_file_name(),fullRawDataPath+"/",""),"%20","")) \
      .withColumn("dateLoaded", F.current_timestamp())
    timeStampColumns = "filename"
  except Exception as e:
    sourceName = "Query Zone Processing - Enrich: Read Data from Raw Zone"
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
  sourceName = "Query Zone Processing - Append Databricks Delta: Cleanse Columns"
  errorCode = "300"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Query Zone

# COMMAND ----------

display(spark.sql("CREATE DATABASE IF NOT EXISTS " + containerName))

# COMMAND ----------

try:
  queryTableExists = (spark.table(databaseTableName) is not None)
except:
  queryTableExists = False

# COMMAND ----------

try:
  if queryTableExists:
    (cleansed_df.repartition(numPartitions) \
      .write \
      .mode("append") \
      .format("delta") \
      .save(currentStatePath)
    )
  else:
    (cleansed_df.repartition(numPartitions) \
      .write \
      .mode("overwrite") \
      .format("delta") \
      .save(currentStatePath)
    )
    sql = """
    CREATE OR REPLACE TABLE {0}
    USING delta
    LOCATION '{1}'
    """.format(databaseTableName, currentStatePath)
    spark.sql(sql)
except Exception as e:
  sourceName = "Query Zone Processing - Append Databricks Delta: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize & Vacuum

# COMMAND ----------

sql = """OPTIMIZE {0}""".format(databaseTableName)
spark.sql(sql)

# COMMAND ----------

if vacuumRetentionHours != '':
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
  spark.sql("VACUUM " + databaseTableName + " RETAIN " + vacuumRetentionHours + " HOURS")
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Look for Bad Records

# COMMAND ----------

try:
  dbutils.fs.ls(badRecordsPath)
  sourceName = "Query Zone Processing - Append Databricks Delta: Bad Records"
  errorCode = "500"
  errorDescription = "Processing completed, but rows were written to badRecordsPath: {0}.  Raw records do not comply with the current schema for table {1}.{2}.".format(badRecordsPath, schemaName, tableName)
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise ValueError(errorDescription)
except:
  print("success")

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