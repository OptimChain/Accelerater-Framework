# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Overwrite Delta Lake Encrypted
# MAGIC ###### Author: Mike Sherill 06/19/2019
# MAGIC 
# MAGIC Data Lake pattern for tables with change feeds of new or updated records.  Takes a file from the raw data path and applies the updates to the Query zone.      
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Raw Data must exist in the Data Lake /raw zone in JSON format.  
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

#dbutils.widgets.removeAll()

# COMMAND ----------

#Add the Date to the FilePath for "Day" or "Month"
dbutils.widgets.text(name="filePath", defaultValue="", label="File Path")
dateFilePath = ""
filePath = dbutils.widgets.get("filePath")

if filePath != '#addFileName':
  dbutils.widgets.text(name="filePath", defaultValue="", label="File Path")
  filePath = dbutils.widgets.get("filePath")
  dbutils.widgets.text(name="filePath", defaultValue="", label="File Path")
  filePath = dbutils.widgets.get("filePath")
  if filePath == "Day":
    dateFilePath = str(datetime.datetime.utcnow().strftime('%Y/%m/%d/'))
  elif filePath == "Month":
    dateFilePath = str(datetime.datetime.utcnow().strftime('%Y/%m/'))
  else:
    dateFilePath = ""

dateFilePath

# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="rsm", label="Container Name")
dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")

dbutils.widgets.text(name="rawDataPath", defaultValue="/Raw/CaseWare/TrialBalance", label="Raw Data Path")
# filePath is to pick up the schema of an actual file in the case of subfolders
dbutils.widgets.text(name="readOption", defaultValue="/*", label="Read Option")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="rsm", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="TrialBalance", label="Table Name")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="", label="Vacuum Retention Hours")
dbutils.widgets.text(name="primaryKeyColumns", defaultValue="ContentType,Name", label="Primary Key Columns")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
containerName = dbutils.widgets.get("containerName")
fullPathPrefix = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

rawDataPath = dbutils.widgets.get("rawDataPath") + "/" + dateFilePath
#rawDataPath = dbutils.widgets.get("rawDataPath") 
fullRawDataPath = fullPathPrefix + rawDataPath
schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName
fullRawFilePath = fullRawDataPath
fullReadDataPath = fullPathPrefix + rawDataPath + dbutils.widgets.get("readOption")
currentStatePath = fullPathPrefix + "/Query/CurrentState/" + tableName
badRecordsPath = "/BadRecords/" + tableName
fullBadRecordsPath = fullPathPrefix + badRecordsPath
databaseTableName = containerName + "." + tableName
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")
primaryKeyColumns = dbutils.widgets.get("primaryKeyColumns")

# COMMAND ----------

notebookName = "Query Zone Processing - Overwrite Databricks Delta"
notebookExecutionLogKey = log_event_notebook_start(notebookName,parentPipeLineExecutionLogKey)
print("Notebook Execution Log Key: {0}".format(notebookExecutionLogKey))

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Raw Data Path: {0}".format(fullRawDataPath))
print("Read Data Path: {0}".format(fullReadDataPath))
print("Current State Path: {0}".format(currentStatePath))
print("Bad Records Path: {0}".format(fullBadRecordsPath))
print("Database Table Name: {0}".format(databaseTableName))

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

print(fullRawDataPath)

# COMMAND ----------


validateRawDataPath = validate_raw_data_path(fullRawDataPath)
if validateRawDataPath == "":
  dbutils.notebook.exit("Raw Data Path supplied has no valid json files to process")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Infer Schema

# COMMAND ----------

schemaPath = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" + "/Schemas/" + tableName + "/Schema.json"
schemaPath

# COMMAND ----------

fullRawDataPath

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data From Raw Zone

# COMMAND ----------

from pyspark.sql import functions as F

if filePath != '#addFileName':
  try:
    #read raw data path into a dataframe, using the schema.  Write any rows unable to be processed to badRecordsPath
    raw_df = spark.read \
      .option("badRecordsPath", fullBadRecordsPath) \
      .json(fullRawDataPath) \
      .withColumn("dateLoaded", F.current_timestamp()) \
      .dropDuplicates()
  except Exception as e:
      sourceName = "Query Zone Processing - Overwrite Delta Lakes: Read Data from Raw Zone"
      errorCode = "200"
      errorDescription = e.message
      log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
      raise(e)

# COMMAND ----------

from pyspark.sql.functions import input_file_name, regexp_replace
from pyspark.sql import functions as F

if filePath == '#addFileName':
  try:
    #read currentState data path into a dataframe, using the schema.  Write any rows unable to be processed to badRecordsPath
    raw_df = spark.read \
      .json(fullRawDataPath) \
      .withColumn("filename", regexp_replace(regexp_replace(input_file_name(),fullRawDataPath+"/",""),"%20","")) \
      .withColumn("dateLoaded", F.current_timestamp()) 
    timeStampColumns = "filename"
  except Exception as e:
    sourceName = "Query Zone Processing - Read Data from Raw Zone"
    errorCode = "200"
    errorDescription = e.message
    log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
    raise(e)

# COMMAND ----------

raw_df.createOrReplaceTempView("test")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test order by clientid asc

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleanse Columns
# MAGIC * remove special characters and spaces from column names 

# COMMAND ----------

try:
  cleansed_df = column_naming_convention(raw_df)
except Exception as e:
  sourceName = "Query Zone Processing - Overwrite Delta Lake: Cleanse Columns"
  errorCode = "300"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

#from cryptography.fernet import Fernet
#fernetkey = Fernet.generate_key()
#fernetkey

# COMMAND ----------

from cryptography.fernet import Fernet
# >>> Put this somewhere safe!
key = Fernet.generate_key()
f = Fernet(key)
token = f.encrypt(b"A really secret message. Not for prying eyes.")
print(token)
print(f.decrypt(token))

 
def encryptValue(text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    text_b=bytes(text, 'utf-8')
    encrypt_val = f.encrypt(text_b)
    encrypt_val = str(encrypt_val.decode('ascii'))
    return encrypt_val

def decryptValue(encrypt_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    decrypted_val=f.decrypt(encrypt_text.encode()).decode()
    return decrypted_val

from pyspark.sql.functions import udf, lit, md5
from pyspark.sql.types import StringType
 
# Register UDF's
encrypt = udf(encryptValue, StringType())
decrypt = udf(decryptValue, StringType())
 
# Fetch key from secrets
encryptionKey = dbutils.preview.secret.get(scope = "framework", key = "fernetkey")
 


# COMMAND ----------

def getDataType(df,columnname):
  return [dtype for name, dtype in df.dtypes if name == columnname][0]

# COMMAND ----------

  columns = cleansed_df.columns
  for col in columns:
    #print(col)
    columnname  = "encrypted_" + col
    #print(getDataType(cleansed_df,col))
      

# COMMAND ----------

listEncryptColumns = primaryKeyColumns.split(",")


# COMMAND ----------

def encryptDataFrame(df):
  columns = df.columns
  encrypted = df
  for ec in listEncryptColumns:
    for col in columns:
      columnname  = "encrypted_" + col
      if (col == ec):
        #if getDataType(encrypted,col) == "string":
        encrypted = encrypted.withColumn(columnname, encrypt(col,lit(encryptionKey)))
  return encrypted


# COMMAND ----------

def rmissingvaluecol(dff,threshold):
    l = []
    l = list(dff.drop(dff.loc[:,list((100*(dff.isnull().sum()/len(dff.index))>=threshold))].columns, 1).columns.values)
    print("# Columns having more than %s percent missing values:"%threshold,(dff.shape[1] - len(l)))
    print("Columns:\n",list(set(list((dff.columns.values))) - set(l)))
    return l

# COMMAND ----------

display(cleansed_df)

# COMMAND ----------

cleansed_df = cleansed_df.where("clientid == 7790742")
#cleansed_df = cleansed_df.select("ClientId").toPandas()
#cleansed_df = cleansed_df.astype(str) 

#l = rmissingvaluecol(cleansed_df,100)
#final_df = cleansed_df[l]

# COMMAND ----------

cleansed_df = encryptDataFrame(cleansed_df)

# COMMAND ----------

display(cleansed_df)

# COMMAND ----------

cleansed_df.dtypes

# COMMAND ----------

a = cleansed_df.columns
for col in a:
  print(col)

# COMMAND ----------

print(a)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Query Zone

# COMMAND ----------

display(spark.sql("CREATE DATABASE IF NOT EXISTS " + containerName))

# COMMAND ----------

display(spark.sql("DROP TABLE IF EXISTS " + databaseTableName))

# COMMAND ----------

try:
  queryTableExists = (spark.table(tableName) is not None)
  metadata = spark.sql("DESCRIBE DETAIL " + databaseTableName)
  format = metadata.collect()[0][0]
  if format != "delta":
    sourceName = "Query Zone Processing - Overwrite Delta Lake: Validate Query Table"
    errorCode = "400"
    errorDescription = "Table {0}.{1} exists but is not in Databricks Delta format.".format(schemaName, tableName)
    log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
    raise ValueError(errorDescription)
except:
  queryTableExists = False

# COMMAND ----------

queryTableExists

# COMMAND ----------

try:
  if queryTableExists:
    (cleansed_df.repartition(numPartitions) \
      .write \
      .mode("overwrite") \
      .option("overwriteSchema", True) \
      .format("delta") \
      .save(currentStatePath)
    )
  else:
    (cleansed_df.repartition(numPartitions) \
      .write \
      .mode("overwrite") \
      .option("overwriteSchema", True) \
      .format("delta") \
      .save(currentStatePath)
    )
    sql = """
    CREATE TABLE {0}
    USING delta
    LOCATION '{1}'
    """.format(databaseTableName, currentStatePath)
    spark.sql(sql)
except Exception as e:
  sourceName = "Query Zone Processing - Overwrite Delta Lake: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Purview Lineage

# COMMAND ----------

sourcePath = fullRawDataPath.replace("abfss://" + containerName + "@","https://").replace("/Raw/","/" + containerName + "/Raw/")
destinationPath = currentStatePath.replace("abfss://" + containerName + "@","https://").replace("Query", containerName + "/Query") + "/{SparkPartitions}"

# COMMAND ----------

sourcePath = sourcePath + tableName + ".json"

# COMMAND ----------

print("Source Path {0}".format(sourcePath))
print("Destination Path {0}".format(destinationPath))

# COMMAND ----------

client.get_entity(qualifiedName=sourcePath,typeName="azure_datalake_gen2_path")

# COMMAND ----------

client.get_entity(qualifiedName=destinationPath,typeName="azure_datalake_gen2_resource_set")

# COMMAND ----------

#Create Lineage using function

create_purview_lineage_ingest(sourcePath, destinationPath, notebookName, tableName, str(notebookExecutionLogKey))

# COMMAND ----------

if vacuumRetentionHours != '':
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
  spark.sql("VACUUM " + databaseTableName + " RETAIN " + vacuumRetentionHours + " HOURS")
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

# COMMAND ----------

try:
  dbutils.fs.ls(badRecordsPath)
  sourceName = "Query Zone Processing - Overwrite Delta Lake: Bad Records"
  errorCode = "500"
  errorDescription = "Processing completed, but rows were written to badRecordsPath: {0}.  Raw records do not comply with the current schema for table {1}.{2}.".format(badRecordsPath, schemaName, tableName)
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise ValueError(errorDescription)
except:
  print("success")

# COMMAND ----------

#sourcePath = "https://" + adlsgen2storageaccountname + ".dfs.core.windows.net/" + containerName + externalDataPath + "/"
#destinationPath = "https://" + adlsgen2storageaccountname + ".dfs.core.windows.net/" + containerName + "/Query/CurrentState/CaseWare/"

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