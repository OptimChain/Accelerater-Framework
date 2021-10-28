# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC 
# MAGIC # Neudesic_Framework_Functions
# MAGIC 
# MAGIC 
# MAGIC This notebook contains all the functions and utilities used by the Neudesic Framework.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run ./Secrets_Databricks_Container

# COMMAND ----------

import pyodbc
import uuid
import datetime

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Framework Functions

# COMMAND ----------

def build_sql_odbc_connection(dbserver, dbname, username, pwd):
  conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+dbserver+';DATABASE='+dbname+';UID='+username+';PWD='+ pwd)
  return conn

# COMMAND ----------

def execute_framework_stored_procedure_with_results(execsp):
  conn = build_sql_odbc_connection(dbserver, dbname, username, pwd)
  cursor = conn.cursor()
  conn.autocommit = True
  cursor.execute(execsp)
  rc = cursor.fetchall()
  conn.close()
  return rc

# COMMAND ----------

def execute_framework_stored_procedure_no_results(execsp):
  conn = build_sql_odbc_connection(dbserver, dbname, username, pwd)
  conn.autocommit = True
  conn.execute(execsp)
  conn.close()

# COMMAND ----------

def log_event_pipeline_start (pipeLineName, parentPipelineExecutionLogKey):
  pipeLineTriggerID = str(uuid.uuid4())
  pipeLineRunId = str(uuid.uuid4())
  pipeLineTriggerName = pipeLineTriggerID
  pipeLineTriggerType = "PipelineActivity"
  pipeLineStartDate = str(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
  
  logEventPipelineStart = """EXEC dbo.uspLogEventPipelineStart
   @PipeLineName='{0}'
  ,@PipeLineTriggerID='{1}'
  ,@PipeLineRunId='{2}'
  ,@DataFactoryName=''
  ,@PipeLineTriggerName='{3}'
  ,@PipeLineTriggerType='{4}'
  ,@PipeLineStartDate='{5}'
  ,@ParentPipeLineExecutionLogKey={6};
  """.format(pipeLineName, pipeLineTriggerID, pipeLineRunId, pipeLineTriggerName, pipeLineTriggerType, pipeLineStartDate, parentPipelineExecutionLogKey)
  
  rc = execute_framework_stored_procedure_with_results(logEventPipelineStart)
  pipeLineExecutionLogKey = rc[0][0]
  return pipeLineExecutionLogKey

# COMMAND ----------

def log_event_pipeline_end (pipeLineExecutionLogKey, pipeLineStatus, pipeLineName, pipeLineExecutionGroupName):  
  logEventPipelineEnd = """EXEC dbo.uspLogEventPipelineEnd
   @PipeLineExecutionLogKey={0}
  ,@PipeLineStatus='{1}'
  ,@PipeLineName='{2}'
  ,@PipeLineExecutionGroupName='{3}';
  """.format(pipeLineExecutionLogKey, pipeLineStatus, pipeLineName, pipeLineExecutionGroupName)
  execute_framework_stored_procedure_no_results(logEventPipelineEnd)

# COMMAND ----------

def log_event_pipeline_error (pipeLineExecutionLogKey, sourceName, errorCode, errorDescription):  
  logEventPipelineError = """EXEC dbo.uspLogEventPipelineError
   @PipeLineExecutionLogKey={0}
  ,@SourceName='{1}'
  ,@ErrorCode={2}
  ,@ErrorDescription='{3}';
  """.format(pipeLineExecutionLogKey, sourceName, errorCode, errorDescription)
  execute_framework_stored_procedure_no_results(logEventPipelineError)

# COMMAND ----------

def log_event_notebook_start (notebookName, parentPipelineExecutionLogKey):
  pipeLineTriggerID = str(uuid.uuid4())
  pipeLineRunId = str(uuid.uuid4())
  pipeLineTriggerName = pipeLineTriggerID
  pipeLineTriggerType = "PipelineActivity"
  notebookStartDate = str(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
  
  logEventNotebookStart = """EXEC dbo.uspLogEventNotebookStart
   @NotebookName='{0}'
  ,@PipeLineTriggerID='{1}'
  ,@PipeLineRunId='{2}'
  ,@DataFactoryName=''
  ,@PipeLineTriggerName='{3}'
  ,@PipeLineTriggerType='{4}'
  ,@NotebookStartDate='{5}'
  ,@ParentPipeLineExecutionLogKey={6};
  """.format(notebookName, pipeLineTriggerID, pipeLineRunId, pipeLineTriggerName, pipeLineTriggerType, notebookStartDate, parentPipelineExecutionLogKey)
  
  rc = execute_framework_stored_procedure_with_results(logEventNotebookStart)
  notebookExecutionLogKey = rc[0][0]
  return notebookExecutionLogKey

# COMMAND ----------

def log_event_notebook_end (notebookExecutionLogKey, notebookStatus, notebookName, notebookExecutionGroupName):  
  logEventNotebookEnd = """EXEC dbo.uspLogEventNotebookEnd
   @NotebookExecutionLogKey={0}
  ,@NotebookStatus='{1}'
  ,@NotebookName='{2}'
  ,@NotebookExecutionGroupName='{3}';
  """.format(notebookExecutionLogKey, notebookStatus, notebookName, notebookExecutionGroupName)
  execute_framework_stored_procedure_no_results(logEventNotebookEnd)

# COMMAND ----------

def log_event_notebook_error (notebookExecutionLogKey, sourceName, errorCode, errorDescription):  
  logEventNotebookError = """EXEC dbo.uspLogEventNotebookError
   @NotebookExecutionLogKey={0}
  ,@SourceName='{1}'
  ,@ErrorCode={2}
  ,@ErrorDescription='{3}';
  """.format(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  execute_framework_stored_procedure_no_results(logEventNotebookError)

# COMMAND ----------

def reset_notebook_execution_list_old(systemExecutionGroupName):
  resetNotebookExecutionList = """
  UPDATE dbo.NotebookExecutionPlan
  SET IsRestart = 1
  FROM dbo.NotebookExecutionPlan pep
  JOIN dbo.Notebook p ON p.NotebookKey = pep.NotebookKey
  JOIN dbo.NotebookExecutionGroup peg ON pep.NotebookExecutionGroupKey = peg.NotebookExecutionGroupKey
  JOIN dbo.SystemExecutionPlan s on peg.NotebookExecutionGroupKey=s.SystemGroupKey
  JOIN dbo.SystemExecutionGroup seg ON s.SystemExecutionGroupKey=seg.SystemExecutionGroupKey
  WHERE seg.SystemExecutionGroupName = '{0}'
  AND peg.IsActive = 1
  AND pep.IsActive = 1 
  AND pep.IsRestart = 0;  
  """.format(systemExecutionGroupName)
  execute_framework_stored_procedure_no_results(resetNotebookExecutionList)

# COMMAND ----------

def reset_notebook_execution_list(containerName):
  resetNotebookExecutionList = "EXEC dbo.uspResetNotebookExecutionList @containerName='{0}';".format(containerName)
  execute_framework_stored_procedure_no_results(resetNotebookExecutionList)

# COMMAND ----------

def build_sql_jdbc_url_and_connectionProperties(dbserver, dbname, username, pwd):
  port = 1433
  jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(dbserver, port, dbname)
  connectionProperties = {
    "user" : username,
    "password" : pwd, 
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  }
  return jdbcUrl, connectionProperties

# COMMAND ----------

def build_sqldw_jdbc_url_and_connectionProperties(dbserver, dwname, username, pwd):
  sqlDwUrlSmall = "jdbc:sqlserver://" + dbserver + ":1433;database=" + dwname + ";user=" + username+";password=" + pwd
  return sqlDwUrlSmall

# COMMAND ----------

# MAGIC %scala
# MAGIC def build_sql_jdbc_url_and_connectionProperties_Scala(dbserver:String, dbname:String, username:String, pwd:String):(String, java.util.Properties) = {
# MAGIC    val port = 1433
# MAGIC    val jdbcUrl = s"jdbc:sqlserver://${dbserver}:${port};database=${dbname}"
# MAGIC    import java.util.Properties
# MAGIC    val connectionProperties = new Properties()
# MAGIC    connectionProperties.put("user",s"${username}")
# MAGIC    connectionProperties.put("password",s"${pwd}")
# MAGIC    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC    connectionProperties.setProperty("Driver", driverClass)
# MAGIC    (jdbcUrl, connectionProperties)
# MAGIC }

# COMMAND ----------

def get_table_metadata_from_framework_db (tableName):
  jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(dbserver, dbname, username, pwd)
  query = """(
  SELECT PrimaryKeyColumns,TimestampColumn 
  FROM dbo.TableMetadata 
  WHERE TableName = '{0}'
  ) t""".format(tableName)
  df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
  r = df.first()
  pk, ts = r[0], r[1]
  return pk, ts

# COMMAND ----------

#this works, but the notebook context and notebookPath can currently only be derived in Scala so I created the above _scala version.  At this time this is not in use. 
def log_to_framework_db (notebookPath, logMessage, notebookContext):
  import datetime, time
  from pyspark.sql import Row
  from collections import OrderedDict
  
  def convert_to_row(d):
    return Row(**OrderedDict(sorted(d.items())))
  
  ts = time.time()
  timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
  jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(dbserver, dbname, username, pwd)
  
  df = sc.parallelize([{"notebookPath": notebookPath, "timestamp": timestamp, "logMessage": logMessage, "notebookContext": notebookContext}]) \
    .map(convert_to_row) \
    .toDF()
  df.createOrReplaceTempView("logMessage")
  
  spark \
  .table("logMessage") \
  .write \
  .jdbc(url=jdbcUrl, table="dbo.OperationalLogging", mode="append", properties=connectionProperties)
  

# COMMAND ----------

dbname

# COMMAND ----------

# MAGIC %scala
# MAGIC def log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) {
# MAGIC   import java.util.Calendar
# MAGIC   import org.apache.spark.sql.SaveMode
# MAGIC   val timestamp = Calendar.getInstance().getTime().toString
# MAGIC   val (jdbcUrl, connectionProperties) = build_sql_jdbc_url_and_connectionProperties_Scala(dbserver, dbname, username, pwd)
# MAGIC   val m = Map[String,String]("notebookPath" -> notebookPath, "timestamp" -> timestamp, "logMessage" -> logMessage, "notebookContext" -> notebookContext)
# MAGIC   val df = m.toSeq.toDF()
# MAGIC   df.createOrReplaceTempView("logMessage")
# MAGIC   val sql = """
# MAGIC   SELECT 
# MAGIC      MAX(CASE WHEN _1 == 'logMessage' THEN _2 ELSE NULL END) AS LogMessage
# MAGIC     ,MAX(CASE WHEN _1 == 'notebookPath' THEN _2 ELSE NULL END) AS notebookPath
# MAGIC     ,MAX(CASE WHEN _1 == 'timestamp' THEN _2 ELSE NULL END) AS timestamp
# MAGIC     ,MAX(CASE WHEN _1 == 'notebookContext' THEN _2 ELSE NULL END) AS notebookContext
# MAGIC   FROM logMessage
# MAGIC   """
# MAGIC   val df_pivoted = spark.sql(sql)
# MAGIC   df_pivoted.createOrReplaceTempView("logMessage")
# MAGIC   spark.table("logMessage").write.mode(SaveMode.Append).jdbc(jdbcUrl, "dbo.OperationalLogging", connectionProperties)
# MAGIC }

# COMMAND ----------

def get_all_container_notebook_execution_lists():
  jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(dbserver, dbname, username, pwd)
  query = """(
   SELECT TOP 100 PERCENT seg.ContainerExecutionGroupName, n.NotebookExecutionGroupName AS NotebookExecutionGroupName, sep.IsActive AS IsActive, sep.IsRestart AS IsRestart, ContainerOrder
  FROM dbo.ContainerExecutionPlan sep
  JOIN dbo.NotebookExecutionGroup n ON n.NotebookExecutionGroupKey = sep.ContainerExecutionGroupKey
  JOIN dbo.ContainerExecutionGroup seg ON seg.ContainerExecutionGroupKey = sep.ContainerExecutionGroupKey
  JOIN dbo.ContainerActivityType sgt ON sgt.ContainerActivityTypeKey = sep.ContainerActivityTypeKey
  WHERE sgt.ContainerActivityTypeName = 'NoteBook'
  ORDER BY ContainerOrder
  ) t"""
  df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
  return df

# COMMAND ----------

def get_system_notebook_execution_list(systemExecutionGroupName):
  jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(dbserver, dbname, username, pwd)
  query = """(
  SELECT TOP 100 PERCENT n.NotebookExecutionGroupName AS ExecutionGroupName
  FROM dbo.SystemExecutionPlan sep
  JOIN dbo.NotebookExecutionGroup n ON n.NotebookExecutionGroupKey = sep.SystemGroupKey
  JOIN dbo.SystemExecutionGroup seg ON seg.SystemExecutionGroupKey = sep.SystemExecutionGroupKey
  JOIN dbo.SystemGroupType sgt ON sgt.SystemGroupTypeKey = sep.SystemGroupTypeKey
  WHERE seg.SystemExecutionGroupName = '{0}'
  AND sep.IsActive = 1
  AND sep.IsRestart = 1
  AND sgt.SystemGroupTypeName = 'NoteBook'
  ORDER BY SystemOrder
  ) t""".format(systemExecutionGroupName)
  df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
  return df

# COMMAND ----------

def get_container_notebook_execution_list(containerExecutionGroupName):
  jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(dbserver, dbname, username, pwd)
  query = """(
 SELECT DISTINCT n.NotebookExecutionGroupName AS ExecutionGroupName
  FROM dbo.ContainerExecutionPlan sep
  JOIN dbo.NotebookExecutionGroup n ON n.ContainerExecutionGroupKey = sep.ContainerExecutionGroupKey
  JOIN dbo.ContainerExecutionGroup seg ON seg.ContainerExecutionGroupKey = sep.ContainerExecutionGroupKey
  JOIN dbo.ContainerActivityType sgt ON sgt.ContainerActivityTypeKey = sep.ContainerActivityTypeKey
  JOIN dbo.NotebookExecutionPlan p ON n.NotebookExecutionGroupKey = p.NotebookExecutionGroupKey
  WHERE seg.ContainerExecutionGroupName = '{0}'
  AND sep.IsActive = 1
  AND sep.IsRestart = 1
  AND sgt.ContainerActivityTypeName = 'NoteBook'
  ) t""".format(containerExecutionGroupName)
  df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
  return df

# COMMAND ----------

def get_all_notebook_execution_lists():
  jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(dbserver, dbname, username, pwd)
  query = """(
  SELECT TOP 100 PERCENT p.NotebookName, peg.IsActive AS IsNotebookExecutionGroupActive, pep.IsActive AS IsNotebookExecutionPlanActive, pep.IsRestart AS IsRestart, pep.NotebookOrder
  FROM	NotebookExecutionPlan pep
  JOIN Notebook p ON p.NotebookKey = pep.NotebookKey
  JOIN NotebookExecutionGroup peg ON pep.NotebookExecutionGroupKey = peg.NotebookExecutionGroupKey
  ORDER BY pep.NotebookOrder
  ) t"""
  df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
  return df

# COMMAND ----------

def get_notebook_execution_list(notebookExecutionGroupName):
  jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(dbserver, dbname, username, pwd)
  query = """(
  SELECT TOP 100 PERCENT p.NotebookName
  FROM	NotebookExecutionPlan pep
  JOIN Notebook p ON p.NotebookKey = pep.NotebookKey
  JOIN NotebookExecutionGroup peg ON pep.NotebookExecutionGroupKey = peg.NotebookExecutionGroupKey
  WHERE peg.NotebookExecutionGroupName = '{0}' 
  AND peg.IsActive = 1
  AND pep.IsActive = 1
  AND pep.IsRestart = 1 
  ORDER BY pep.NotebookOrder
  ) t""".format(notebookExecutionGroupName)
  df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
  return df

# COMMAND ----------

def get_notebook_parameters_for_all_notebooks():
  jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(dbserver, dbname, username, pwd)
  query = """(
    SELECT 
     n.NotebookKey
    ,n.NotebookName 
    ,t.Parameters
    FROM dbo.Notebook n
    CROSS APPLY
    (
      SELECT p.NotebookParameterName AS pKey, p.NotebookParameterValue AS pValue
      FROM dbo.NotebookParameter p
      JOIN dbo.Notebook nb ON p.NotebookKey=nb.NotebookKey
      WHERE nb.NotebookKey = n.NotebookKey
      FOR JSON AUTO, WITHOUT_ARRAY_WRAPPER
    ) t (Parameters)
  ) t"""
  df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
  return df

# COMMAND ----------

def get_notebook_parameters(notebookExecutionName):
  jdbcUrl, connectionProperties = build_sql_jdbc_url_and_connectionProperties(dbserver, dbname, username, pwd)
  query = """(
    SELECT 
     n.NotebookKey
    ,n.NotebookName 
    ,t.Parameters
    FROM dbo.Notebook n
    CROSS APPLY
    (
      SELECT p.NotebookParameterName AS pKey, p.NotebookParameterValue AS pValue
      FROM dbo.NotebookParameter p
      JOIN dbo.Notebook n ON p.NotebookKey=n.NotebookKey
      WHERE n.NotebookName = '{0}'
      FOR JSON AUTO, WITHOUT_ARRAY_WRAPPER
    ) t (Parameters)
    WHERE n.NotebookName = '{0}'
  ) t""".format(notebookExecutionName)
  df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Notebook Utility Functions

# COMMAND ----------

def run_with_retry(notebook, timeout, args = {}, max_retries = 3):
  num_retries = 0
  while True:
    try:
      return dbutils.notebook.run(notebook, timeout, args)
    except Exception as e:
      if num_retries > max_retries:
        raise e
      else:
        print ("Retrying error", e)
        num_retries += 1

# COMMAND ----------

"""
Attempt to read the schema for the table from /schema/tablename.  If it is not found, run the InferSchema notebook to infer the schema and save back to the data lake.  Then attempt to read the schema again and return it to the caller.   
"""
def get_table_schema(containerName, dataPath, tableName, dataLakeZone="Query", notebookExecutionLogKey=0):
  schemaPath = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" + "/Schemas/" + tableName + "/Schema.json"
  try:
    head = dbutils.fs.head(schemaPath, 256000)
  except Exception as e:
    if dataLakeZone == "Query":
      dbutils.notebook.run("./Query Zone Processing - InferSchema", 6000, {"parentPipeLineExecutionLogKey": notebookExecutionLogKey, "containerName": containerName, "rawDataPath": dataPath, "tableName": tableName, "samplingRatio": .5})
    elif dataLakeZone == "Raw":
      dbutils.notebook.run("./Raw Zone Processing - InferSchema", 6000, {"parentPipeLineExecutionLogKey": notebookExecutionLogKey, "containerName": containerName, "dataPath": dataPath, "tableName": tableName, "samplingRatio": .5, "delimiter": ",", "hasHeader": "False"})
    head = dbutils.fs.head(schemaPath, 256000)
    
  import json
  from pyspark.sql.types import StructType
  return StructType.fromJson(json.loads(head))

# COMMAND ----------

"""
get separate lists of supplied schema by data type 
This will be used by data cleansing utilities to determine the best way to impute nulls etc. 
For example, numeric types can use pyspark.ml.feature.Imputer to replace with mean or median strategy
Other data types are limited to drop, fill, or replace
"""
def schema_to_lists_by_type(schema):
  from pyspark.sql.types import IntegerType, TimestampType, DoubleType, FloatType, StringType

  intCols = [s for s in schema if isinstance(s.dataType, IntegerType)]
  timestampCols = [s for s in schema if isinstance(s.dataType, TimestampType)]
  numericCols = [s for s in schema if isinstance(s.dataType, DoubleType) or isinstance(s.dataType, FloatType)]
  stringCols = [s for s in schema if isinstance(s.dataType, StringType)]
  allCols = [s for s in schema]
  return intCols, timestampCols, numericCols, stringCols, allCols

# COMMAND ----------

def get_column_names_from_schema (schema):
  return [c.name for c in schema]

# COMMAND ----------

def rename_columns(df, columns):
    if isinstance(columns, dict):
        for old_name, new_name in columns.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")

# COMMAND ----------

"""
impute nulls using mean or median of the non-null values
"""
def impute_nulls (df, numericCols, strategy="mean"):
  from pyspark.ml.feature import Imputer
  imputerStrategy = strategy #mean, median
  
  numericCols_out = [c.name + '_out' for c in numericCols]
  imputer = Imputer(inputCols=get_column_names_from_schema(numericCols), outputCols=numericCols_out)
  df = imputer.setStrategy(imputerStrategy).setMissingValue(1.0).fit(df).transform(df)
  df = df.drop(*get_column_names_from_schema(numericCols))
  k = numericCols_out
  v = get_column_names_from_schema(numericCols)
  rename_columns_dict = dict(zip(k, v))
  df = rename_columns(df, rename_columns_dict)
  return df

# COMMAND ----------

"""
accepts a dataframe and a column list that is expected to be unique
creates temporary columns where values converted to lower to ignore case-sensitivity
drops duplicates based on those temp columns
deletes the temp columns
"""
def deduplicate_dataframe (df, unique_cols):
  from pyspark.sql.functions import lower, col, regexp_replace
  dedup_col_list = []
  for c in unique_cols:
    c = c.strip()
    n = "dedup" + c
    dedup_col_list.append(n)
    df = (df.withColumn(n, lower(col(c))))
  df = (df \
      .drop_duplicates(dedup_col_list) \
      .drop(*dedup_col_list)
     )
  return df

# COMMAND ----------

def build_merge_SQL_Statement(deltaTableName,tableName,columns_list):
  def get_column_insert_list(columns_list):
    quotes = ["`"+c+"`" for c in columns_list]
    return ",".join(quotes)
  def get_column_insert_values_list(columns_list, deltaTableName):
    quotes = ["b.`"+c+"`" for c in columns_list]
    return ",".join(quotes)    
  def get_column_update_clause(columns_list):
    quotes = ["a.`"+c+"`=b.`"+c+"`" for c in columns_list if c != "neu_pk_col"]
    return ",".join(quotes)
  
  column_insert_list = get_column_insert_list(columns_list)
  column_insert_values_list = get_column_insert_values_list(columns_list, deltaTableName)
  column_update_clause = get_column_update_clause(columns_list)
  
  sql = """MERGE INTO {1} AS a USING {0} AS b ON b.neu_pk_col = a.neu_pk_col
  WHEN MATCHED THEN UPDATE SET {4}
  WHEN NOT MATCHED THEN INSERT ({2}) VALUES({3})
  """.format(deltaTableName,tableName,column_insert_list,column_insert_values_list,column_update_clause)
  return sql

# COMMAND ----------

"""
This function can be used to remove the _started, _committed, and _SUCCESS HDFS metadata files that are created by the FileOutputCommitter by default. 
Note that these files can be removed by global spark setting (mapreduce.fileoutputcommitter.marksuccessfuljobs) but I find these files useful for debugging purposes, especially in the Raw and Query Zones.  In the summary zone where users may be extracting data, it may make sense to run this function to remove all metadata files except for the actual data files. 

Usage: removeHDFSFileOutputCommitterFiles ("/summary/DataLakeRowCounts/OHENSORDOH", "csv")
"""
def removeHDFSFileOutputCommitterFiles (summaryPath, extension):
  files = dbutils.fs.ls(summaryPath)
  files_to_rm = [f.path for f in files if f.path[-len(extension):] != extension]
  for file in files_to_rm:
    dbutils.fs.rm(file)

# COMMAND ----------

"""
This function can be used to rename a file in the data lake store. This is done by a copy followed by a delete, so this is not recommended.  This is also not recommended except for Summary zone files that are NOT associated with catalog tables (this will require them to be refreshed).  
"""
def renameHDFSPartFile(summaryPath, fileName, newFileName):
  file_to_rename = summaryPath + "/" + fileName
  new_file_name = summaryPath + "/" + newFileName
  dbutils.fs.mv(file_to_rename, new_file_name)

# COMMAND ----------

def column_naming_convention(df):
  import re
  oldCols = raw_df.columns
  newCols = []
  for col in oldCols:
    newCol = re.sub('[^\w]', '', col)
    newCols.append(newCol)
  raw_df_columns_cleansed = raw_df.toDF(*newCols)
  return raw_df_columns_cleansed

# COMMAND ----------

def validate_raw_data_path(rawDataPath):
  try:
    files = dbutils.fs.ls(rawDataPath)
    found = [f for f in files if f.name[-4:] == "json" and f.size > 3]
    if len(found) > 0:
      return rawDataPath
    else:
      print("Raw Data Path supplied has no valid json files to process")
      return ""
  except Exception as e:
    print("Raw Data Path supplied has no valid json files to process")
    return ""

# COMMAND ----------

def validate_source_path(path, extension):
  try:
    files = dbutils.fs.ls(path)
    filesToProcess = [f for f in files if f.name[-len(extension):] == extension and f.size > 0]
    if len(filesToProcess) > 0:
      return path
    else:
      return ""
  except Exception as e:
    print(e)
    return ""