# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Overwrite TaxTrialBalance (BL)
# MAGIC ###### Author: Mike Sherrill
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

dbutils.widgets.text(name="containerName", defaultValue="rsm", label="Container Name")
containerName = dbutils.widgets.get("containerName")

# COMMAND ----------

# MAGIC %run rsm/Publisher/Framework/Neudesic_Framework_Functions

# COMMAND ----------

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="rsm", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="AccountsNotAssignedToTaxCodes", label="Table Name")
dbutils.widgets.text(name="vacuumRetentionHours", defaultValue="", label="Vacuum Retention Hours")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
fullPathPrefix = "abfss://" + containerName + "@" + adlsGen2StorageAccountName + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName
currentStatePath = fullPathPrefix + "/Query/CurrentState/CaseWare/BL"
enrichedPath = fullPathPrefix + "/Query/Enriched/Tax/TaxTrialBalance"
databaseTableName = schemaName + "." + tableName
vacuumRetentionHours = dbutils.widgets.get("vacuumRetentionHours")

# COMMAND ----------

stageTableName = "caseware.BL"

# COMMAND ----------

notebookName = "Query Zone Processing - Overwrite TaxTrialBalance (BL)"
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

display(cleansed_df)

# COMMAND ----------

display(spark.sql("CREATE DATABASE IF NOT EXISTS caseware"))

# COMMAND ----------

sql = """
DROP TABLE IF EXISTS {0}
""".format(stageTableName)
spark.sql(sql)

# COMMAND ----------

  sql = """
  CREATE TABLE IF NOT EXISTS {0}
  USING delta
  LOCATION '{1}'
  """.format(stageTableName, currentStatePath)
  spark.sql(sql)

# COMMAND ----------

#Get Opening Balance by Year
sql="""
SELECT SUBSTRING(FILENAME,LOCATE("(", FILENAME)+1,LOCATE(")", FILENAME)- (LOCATE("(", FILENAME)+1)) as EngagementID,
CASE 
  WHEN YEAR = 0 THEN "CurrentYear"
  WHEN YEAR =1 THEN "PriorYear1"
  WHEN YEAR =2 THEN "PriorYear2"
  WHEN YEAR =3 THEN "PriorYear3"
  WHEN YEAR =4 THEN "PriorYear4"
END AS YEAR
,NUMBER as AccountNo
,"OpeningBalance" as Bucket, SUM(AMT) as OpeningBalance
FROM {0}
WHERE ID = 'X'
AND BUCKET = 'O'
GROUP BY SUBSTRING(FILENAME,LOCATE("(", FILENAME)+1,LOCATE(")", FILENAME)- (LOCATE("(", FILENAME)+1)), YEAR, NUMBER
""".format(stageTableName)
ob_df=(spark.sql(sql))

# COMMAND ----------

pivotObDF = ob_df.groupBy("EngagementID", "AccountNo", "Bucket").pivot("YEAR").sum("OpeningBalance")
pivotObDF.show()

# COMMAND ----------

pivotObDF.createOrReplaceTempView("OpeningBalanceView")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from OpeningBalanceView

# COMMAND ----------

#Get Adjustments by Year
sql="""
SELECT SUBSTRING(FILENAME,LOCATE("(", FILENAME)+1,LOCATE(")", FILENAME)- (LOCATE("(", FILENAME)+1)) as EngagementID,
CASE 
  WHEN YEAR = 0 THEN "CurrentYear"
  WHEN YEAR =1 THEN "PriorYear1"
  WHEN YEAR =2 THEN "PriorYear2"
  WHEN YEAR =3 THEN "PriorYear3"
  WHEN YEAR =4 THEN "PriorYear4"
END AS YEAR
, NUMBER as AccountNo, "TaxAdjustments" as Bucket, SUM(AMT) as Adjustments
FROM {0}
WHERE ID = 'X'
AND BUCKET = 'Y'
GROUP BY SUBSTRING(FILENAME,LOCATE("(", FILENAME)+1,LOCATE(")", FILENAME)- (LOCATE("(", FILENAME)+1)), YEAR, NUMBER
""".format(stageTableName)
adj_df=(spark.sql(sql))

# COMMAND ----------

pivotAdjDF = adj_df.groupBy("EngagementID", "AccountNo", "Bucket").pivot("YEAR").sum("Adjustments")
pivotAdjDF.show()

# COMMAND ----------

pivotAdjDF.createOrReplaceTempView("AdjustmentsView")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from AdjustmentsView

# COMMAND ----------

#Get FinalAmount by Year
sql="""
SELECT SUBSTRING(FILENAME,LOCATE("(", FILENAME)+1,LOCATE(")", FILENAME)- (LOCATE("(", FILENAME)+1)) as EngagementID,
CASE 
  WHEN YEAR = 0 THEN "CurrentYear"
  WHEN YEAR =1 THEN "PriorYear1"
  WHEN YEAR =2 THEN "PriorYear2"
  WHEN YEAR =3 THEN "PriorYear3"
  WHEN YEAR =4 THEN "PriorYear4"
END AS YEAR
, NUMBER as AccountNo, "AdjustedTaxBalance" as Bucket, SUM(AMT) as FinalAmount
FROM {0}
WHERE ID = 'X'
GROUP BY SUBSTRING(FILENAME,LOCATE("(", FILENAME)+1,LOCATE(")", FILENAME)- (LOCATE("(", FILENAME)+1)), YEAR, NUMBER
""".format(stageTableName)
fa_df=(spark.sql(sql))

# COMMAND ----------

pivotFaDF = fa_df.groupBy("EngagementID","AccountNo", "Bucket").pivot("YEAR").sum("FinalAmount")
pivotFaDF.show()

# COMMAND ----------

pivotFaDF.createOrReplaceTempView("FinalAmountView")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FinalAmountView

# COMMAND ----------

#Get Client No
sql=""" CREATE OR REPLACE VIEW EngagementIDView as 
SELECT DISTINCT SUBSTRING(FILENAME,LOCATE("(", FILENAME)+1,LOCATE(")", FILENAME)- (LOCATE("(", FILENAME)+1)) as EngagementID, FILENAME
FROM {0}
""".format(stageTableName)
display(spark.sql(sql))

# COMMAND ----------

#Get Engagement ID
sql="""
SELECT * FROM EngagementIDView
""".format(stageTableName)
display(spark.sql(sql))

# COMMAND ----------

#Get MapNo Description
sql=""" CREATE OR REPLACE TEMP VIEW TaxTrialBalanceView as 
SELECT * FROM OpeningBalanceView
UNION ALL
SELECT * FROM AdjustmentsView
UNION ALL
SELECT * FROM FinalAmountView
""".format()
display(spark.sql(sql))

# COMMAND ----------

sql="""
SELECT *
FROM TaxTrialBalanceView
""".format()
display(spark.sql(sql))

# COMMAND ----------

sql="""
SELECT ttb.EngagementID
,RTRIM(LTRIM(ttb.AccountNo)) as AccountNo
,Name
,Bucket
,CurrentYear
,PriorYear1
,PriorYear2
,PriorYear3
,PriorYear4
FROM TaxTrialBalanceView ttb
JOIN tax.AccountTrialBalance atb on ttb.EngagementID = atb.EngagementID and atb.AccountNo = ttb.AccountNo
""".format()
#display(spark.sql(sql))
dest_df = spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data to Query Zone (Enriched)

# COMMAND ----------

try:
  dest_df \
    .repartition(numPartitions) \
    .write \
    .mode("overwrite") \
    .option("MergeSchema",True) \
    .format("delta") \
    .save(enrichedPath)
except Exception as e:
  sourceName = "Query Zone Processing - Overwrite: Write to Query Zone"
  errorCode = "400"
  errorDescription = e.message
  log_event_notebook_error(notebookExecutionLogKey, sourceName, errorCode, errorDescription)
  raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Databricks Table

# COMMAND ----------

display(spark.sql("CREATE DATABASE IF NOT EXISTS " + schemaName.replace("-","")))

# COMMAND ----------

databaseTableName

# COMMAND ----------

sql = """
DROP TABLE IF EXISTS {0}
""".format(databaseTableName)
spark.sql(sql)

# COMMAND ----------

  sql = """
  CREATE TABLE IF NOT EXISTS {0}
  USING delta
  LOCATION '{1}'
  """.format(databaseTableName, enrichedPath)
  spark.sql(sql)

# COMMAND ----------

#sql="""OPTIMIZE {0}""".format(databaseTableName)
#spark.sql(sql)

# COMMAND ----------

sql = """
SELECT COUNT(*) FROM {0}
""".format(databaseTableName)
display(spark.sql(sql))

# COMMAND ----------

if vacuumRetentionHours != '':
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
  spark.sql("VACUUM " + databaseTableName + " RETAIN " + vacuumRetentionHours + " HOURS")
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Purview Lineage

# COMMAND ----------

sourcePath = currentStatePath.replace("abfss://" + containerName + "@","https://").replace("Query", containerName + "/Query") + "/{SparkPartitions}"
destinationPath = enrichedPath.replace("abfss://" + containerName + "@","https://").replace("Query/Enriched/Tax", containerName + "/Query/Enriched/Tax") +  "/{SparkPartitions}"
print("Source Path {0}".format(sourcePath))
print("Destination Path {0}".format(destinationPath))

# COMMAND ----------

sourceAsset = client.get_entity(qualifiedName=sourcePath,typeName="azure_datalake_gen2_resource_set")

# COMMAND ----------

sourceAsset

# COMMAND ----------

destinationAsset = client.get_entity(qualifiedName=destinationPath,typeName="azure_datalake_gen2_resource_set")

# COMMAND ----------

destinationAsset

# COMMAND ----------

if sourceAsset != {} and destinationAsset != {}:
  print('Create Purview Lineage')
  create_purview_lineage_enrich(sourcePath, destinationPath, notebookName, str(-23947544444202342345), str(notebookExecutionLogKey))

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