# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Merge
# MAGIC 
# MAGIC Data Lake pattern for tables with change feeds of new or updated records.  Takes a file from the raw data path and applies the updates to the Query zone.      
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Raw Data must exist in the Data Lake /raw zone in JSON format.  Incremental files must be appended to the store e.g. /raw/tablename/yyyy/mm/dd. 
# MAGIC 2. The source system must be able to produce change feeds of new or updated records. 
# MAGIC 3. The source system tables must have both a primary key (or row Id) and one or more timestamp columns and this metadata must be updated in the Framework database (dbo.NotebookParameters)
# MAGIC 
# MAGIC #### Details
# MAGIC Note: Databricks Delta is in public preview and once it is GA this Load Pattern will be deprecated in favor of it.  
# MAGIC 
# MAGIC This Load Pattern does the following:
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

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="rawDataPath", defaultValue="/raw/schemaName/tableName/file.json", label="Raw Data Path")
dbutils.widgets.text(name="schemaName", defaultValue="schema", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="table", label="Table Name")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="", label="Primary Key Columns")
dbutils.widgets.text(name="timestampColumns", defaultValue="", label="Timestamp Columns")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName
numPartitions = int(dbutils.widgets.get("numPartitions"))
rawDataPath = adlbasepath + dbutils.widgets.get("rawDataPath")
currentStatePath = fullPathPrefix + "/Query/CurrentState/" + schemaName + "/" + tableName
badRecordsPath = adlbasepath + "/BadRecords/" + tableName
primaryKeyColumns = dbutils.widgets.get("primaryKeyColumns")
timestampColumns = dbutils.widgets.get("timestampColumns")

#the above have been customized for Ardent Mills.  Standard framework convention is below:
#rawDataPath = adlbasepath + dbutils.widgets.get("rawDataPath")
#currentStatePath = adlbasepath + "/Query/CurrentState/" + schemaName + "/" + tableName
#badRecordsPath = adlbasepath + "/BadRecords/" + schemaName + "/" + tableName

# COMMAND ----------

notebookName = "Query Zone Processing - Merge"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

if primaryKeyColumns == '' or timestampColumns == '':
  errorstring = "This loading pattern requires the primary key colum(s) and timestamp column(s) to be saved as Notebook Parameters."
  raise ValueError(errorstring)

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Raw Data Path: {0}".format(rawDataPath))
print("Current State Path: {0}".format(currentStatePath))
print("Bad Records Path: {0}".format(badRecordsPath))
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

rawDataPath = validate_raw_data_path(rawDataPath)
if rawDataPath == "":
  dbutils.notebook.exit("Raw Data Path supplied has no valid json files to process")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Infer Schema

# COMMAND ----------

schema = get_table_schema(rawDataPath, tableName)
schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data from Raw Zone

# COMMAND ----------

try:
  #read raw data path into a dataframe, using the schema.  Write any rows unable to be processed to badRecordsPath
  raw_df = spark.read \
    .schema(schema) \
    .option("badRecordsPath", badRecordsPath) \
    .json(rawDataPath) \
    .dropDuplicates()
except Exception as e:
  sourceName = "Query Zone Processing - Merge: Read Data from Raw Zone"
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
  sourceName = "Query Zone Processing - Merge: Cleanse Columns"
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
  sourceName = "Query Zone Processing - Merge: Build Upsert Table"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Table from Query Zone
# MAGIC * If the Query Zone Table doesn't exist, abort this notebook and revert to a full refresh by calling that notebook. 

# COMMAND ----------

#attempt to read in the current state path into a dataframe
try:
  ls = dbutils.fs.ls(currentStatePath)
  current_df = (spark.read.parquet(currentStatePath))
except Exception as e:
  rawDataPath = adlbasepath + "/raw/" + tableName
  log_event_notebook_end(notebookExecutionLogKey=notebookExecutionLogKey, notebookStatus="SUCCEEDED", notebookName=notebookName, notebookExecutionGroupName="")
  run_with_retry(notebook="/Framework/Query Zone Processing - Merge FULL REFRESH", timeout=600, args={"rawDataPath": rawDataPath, "schemaName": schemaName, "tableName": tableName, "numPartitions": numberOfPartitions, "primaryKeyColumns": primaryKeyColumns, "timestampColumns": timestampColumns}, max_retries=1)
  dbutils.notebook.exit("Completed")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Query Zone

# COMMAND ----------

try:
  #compare the raw and the current dfs, need to end up with the latest version of every row

  #union the 2 dataframes and create a temp table for spark sql
  union_df = current_df.union(raw_df_with_metadata).dropDuplicates()
  union_df.createOrReplaceTempView("u")

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
  sourceName = "Query Zone Processing - Merge: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

try:
  dbutils.fs.ls(badRecordsPath)
  sourceName = "Query Zone Processing - Merge: Bad Records"
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