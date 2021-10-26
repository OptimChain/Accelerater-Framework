# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Load Pattern 3 - Delta Processing FULL REFRESH
# MAGIC 
# MAGIC Data Lake pattern for tables with change feeds of new or updated records.  Takes a file from the raw data path and applies the updates to the Query zone.      
# MAGIC 
# MAGIC This is called by Query Zone Processing - Merge notebook when the Query Table does not exist (either because it is a brand new table or because it was deleted).  Thus, this notebook reads in ALL raw data and uses it to process the Query Table. In normal cases you should not have to run this directly.
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Raw Data must exist in the Data Lake /raw zone in JSON format.  Incremental files must be appended to the store e.g. /raw/tablename/yyyy/mm/dd. 
# MAGIC 2. The source system must be able to produce change feeds of new or updated records. 
# MAGIC 3. The source system tables must have both a primary key (or row Id) and one or more timestamp columns and this metadata must be updated in the Framework database databricks.TableMetaData
# MAGIC 
# MAGIC #### Details
# MAGIC Note: Databricks Delta is in public preview and once it is GA this Load Pattern will be deprecated in favor of it.  
# MAGIC 
# MAGIC This Load Pattern does the following:
# MAGIC * Obtains the Raw Schema for the supplied path from ** /schemas/tablename ** if it exists (if not it samples the file and infers it, then saves the schema for use next time)
# MAGIC * Reads the Raw Data into a dataframe using the supplied path and obtained schema
# MAGIC * Queries the Azure SQL DB Framework Metadata to obtain:
# MAGIC   * Primary Key Column(s)
# MAGIC   * Timestamp Column
# MAGIC * Reads the CurrentState ** /query/currentState/table ** into a dataframe  
# MAGIC * Unions the two dataframes and generates a new dataframe consisting of the latest rows for each record, using the metadata obtained from the Metadata DB
# MAGIC * Overwrites the contents of ** /query/currentState/table ** with the results and Creates the Databricks table
# MAGIC 
# MAGIC It obtains the raw, event-sourced data from the raw zone (from the supplied path) that might look something like this:
# MAGIC 
# MAGIC <pre><code><table><tr><td>OrderID:</td><td>Timestamp</td><td>Detail</td></tr>
# MAGIC   <tr><td>1</td><td>2000-01-01 12:00:00</td><td>A</td></tr>
# MAGIC   <tr><td>1</td><td>2000-01-02 12:00:00</td><td>B</td></tr>
# MAGIC   <tr><td>2</td><td>2000-01-02 12:30:00</td><td>A</td></tr></table></code></pre>
# MAGIC 
# MAGIC And produces an updated (overwritten) daily snapshot parquet table in the Query Zone: e.g. /query/tablename consisting of the latest rows of all the raw data for a given key, based on the ordering of the timestamp:
# MAGIC 
# MAGIC <pre><code><table><tr><td>OrderID:</td><td>Timestamp</td><td>Detail</td></tr>
# MAGIC   <tr><td>1</td><td>2000-01-02 12:00:00</td><td>B</td></tr>
# MAGIC   <tr><td>2</td><td>2000-01-02 12:30:00</td><td>A</td></tr></table></code></pre>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run /Framework/Secrets-Databricks-Cache

# COMMAND ----------

# MAGIC %run /Framework/Neudesic_Framework_Functions

# COMMAND ----------

spark.conf.set("spark.databricks.delta.merge.joinBasedMerge.enabled", True)

# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="client0000001", label="Container Name")
dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")

dbutils.widgets.text(name="rawDataPath", defaultValue="client0000001/Raw/Dataset1/", label="Raw Data Path")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="schema", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="EE_MeterData", label="Table Name")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Columns")
dbutils.widgets.text(name="timestampColumns", defaultValue="", label="Timestamp Columns")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
fullPathPrefix = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

rawDataPath = dbutils.widgets.get("rawDataPath")
fullRawDataPath = fullPathPrefix + rawDataPath
schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName
fullRawFilePath = fullRawDataPath
currentStatePath = fullPathPrefix + "/Query/CurrentState/" + schemaName + "/" + tableName
badRecordsPath = "/BadRecords/" + tableName
fullBadRecordsPath = fullPathPrefix + rawDataPath
primaryKeyColumns = dbutils.widgets.get("primaryKeyColumns")
timestampColumns = dbutils.widgets.get("timestampColumns")

# COMMAND ----------

notebookName = "Query Zone Processing - Merge FULL REFRESH"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

if primaryKeyColumns == '':
  errorstring = "This loading pattern requires the primary key colum(s) to be saved as Notebook Parameters."
  raise ValueError(errorstring)

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Raw Data Path: {0}".format(rawDataPath))
print("Current State Path: {0}".format(currentStatePath))
print("Bad Records Path: {0}".format(fullBadRecordsPath))
print("Primary Key Columns: {0}".format(primaryKeyColumns))
print("Timestamp Columns: {0}".format(timestampColumns))

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

schema = get_table_schema(containerName, rawDataPath, tableName)
schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data From Raw Zone

# COMMAND ----------

try:
  #read raw data path into a dataframe, using the schema.  Write any rows unable to be processed to badRecordsPath
  raw_df = spark.read \
    .schema(schema) \
    .option("badRecordsPath", badRecordsPath) \
    .json(fullRawDataPath) \
    .dropDuplicates()
except Exception as e:
  sourceName = "Query Zone Processing - Merge Databricks Delta: Read Data from Raw Zone"
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
  sourceName = "Query Zone Processing - Merge FULL REFRESH: Cleanse Columns"
  errorCode = "300"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build Upsert Table

# COMMAND ----------

try:
  #create temp table to use with spark sql
  cleansed_df.createOrReplaceTempView("raw")

  #build spark sql query to create neu_ metadata columns
  pk_cols = primaryKeyColumns.split(",")
  pk_string = "CONCAT("
  for c in pk_cols:
    col = "CAST(COALESCE(`" + c.strip() + "`,'') AS STRING),"
    pk_string += col
  pk_string = pk_string[0:-1] + ")"
  ts = "CAST(COALESCE(`" + timestampColumns.split(",")[0] + "`,'1900-01-01 12:00:00.0000000 + 00:00') AS STRING)"
  sql = """
  SELECT 
   *
  ,{0} AS neu_pk_col
  ,{1} AS neu_timestampdatetime
  FROM raw
  """.format(pk_string, ts)

  raw_df_with_metadata = spark.sql(sql)
except Exception as e:
  sourceName = "Query Zone Processing - Merge FULL REFRESH: Build Upsert Table"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Query Zone

# COMMAND ----------

try:
  queryTableExists = (spark.table(tableName) is not None)
except:
  queryTableExists = False

# COMMAND ----------

display(spark.sql("DROP TABLE  IF EXISTS " + tableName))

# COMMAND ----------

try:
  raw_df_with_metadata.createOrReplaceTempView("u")

  #build and run query that uses windowing to determine the most recent version of each row by its timestamp
  latest_df = spark.sql("""
  SELECT * 
  FROM 
  (
    SELECT 
     *
    ,dense_rank() OVER(PARTITION BY neu_pk_col ORDER BY neu_timestampdatetime DESC) AS rank
    FROM u
  ) u
  WHERE rank < 2
  """)

  #drop the rank column so that column counts match
  new_current_df = latest_df.drop("rank")

  #write results to currentStatePath and saveAsTable so it is available in the sql catalog
  new_current_df \
    .repartition(numPartitions) \
    .write \
    .mode("OVERWRITE") \
    .option('path', currentStatePath) \
    .saveAsTable(tableName)
except Exception as e:
  sourceName = "Query Zone Processing - Merge FULL REFRESH: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

try:
  dbutils.fs.ls(badRecordsPath)
  sourceName = "Query Zone Processing - Merge FULL REFRESH: Bad Records"
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