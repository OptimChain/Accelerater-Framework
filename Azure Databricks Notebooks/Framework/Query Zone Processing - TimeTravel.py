# MAGIC  
# MAGIC %md
# MAGIC # Query Zone Processing - Time Travel
# MAGIC 
# MAGIC Example Time Travel for Delta Lake Tables
# MAGIC 
# MAGIC #### Usage
# MAGIC Supply the parameters above and run the notebook.  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Data must exist in the Delta Lake format
# MAGIC 
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize Framework

# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="client000000001", label="Container Name")
containerName = dbutils.widgets.get("containerName")

# COMMAND ----------

# MAGIC %run /Framework/Secrets-Databricks-Cache

# COMMAND ----------

# MAGIC %run /Framework/Neudesic_Framework_Functions

# COMMAND ----------

dbutils.widgets.text(name="parentPipeLineExecutionLogKey", defaultValue="-1", label="Parent Pipeline Execution Log Key")
dbutils.widgets.text(name="numPartitions", defaultValue="8", label="Number of Partitions")
dbutils.widgets.text(name="schemaName", defaultValue="schema", label="Schema Name")
dbutils.widgets.text(name="tableName", defaultValue="HHA_SNF_Referrals", label="Table Name")

parentPipeLineExecutionLogKey = dbutils.widgets.get("parentPipeLineExecutionLogKey")
fullPathPrefix = "abfss://" + containerName + "@" + adlsgen2storageaccountname + ".dfs.core.windows.net" 

numPartitions = int(dbutils.widgets.get("numPartitions"))

schemaName = dbutils.widgets.get("schemaName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = schemaName + "." + tableName
currentStatePath = fullPathPrefix + "/Query/CurrentState/" + schemaName + "/" + tableName
enrichedPath = fullPathPrefix + "/Query/Enriched/" + tableName
databaseTableName = containerName + "." + tableName

# COMMAND ----------

print("Schema Name: {0}".format(schemaName))
print("Table Name: {0}".format(tableName))
print("Fully Qualified Table Name: {0}".format(fullyQualifiedTableName))
print("Number of Partitions: {0}".format(numPartitions))
print("Current State Path: {0}".format(currentStatePath))
print("Enriched State Path: {0}".format(enrichedPath))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get History

# COMMAND ----------

# MAGIC %md
# MAGIC #### spark.sql Examples

# COMMAND ----------

sql = """
DESCRIBE DETAIL {0}
""".format(databaseTableName)
display(spark.sql(sql))


# COMMAND ----------

sql = """
DESCRIBE HISTORY {0}
""".format(databaseTableName)
history=spark.sql(sql)

display(history)

# COMMAND ----------

sql = """
SELECT max(version) FROM (DESCRIBE HISTORY {0})
""".format(databaseTableName)

display(spark.sql(sql))


# COMMAND ----------

version1 = 1

sql = """
SELECT count(*) as Version_{1}_RecordCount FROM {0} VERSION AS OF {1}
""".format(databaseTableName,version1)
display(spark.sql(sql))



# COMMAND ----------

version1 = 1

sql = """
SELECT sum(Amount) as Version_{1}_RecordCount FROM {0} VERSION AS OF {1}
""".format(databaseTableName,version1)
display(spark.sql(sql))



# COMMAND ----------

version1 = 1

sql = """
SELECT count(*) as Version_{1}_RecordCount FROM {0} VERSION AS OF {1}
""".format(databaseTableName,version1)
display(spark.sql(sql))



# COMMAND ----------

sql = """
SELECT count(*) FROM {0} TIMESTAMP AS OF '2019-10-25T04:37:39'
""".format(databaseTableName)
result=spark.sql(sql)

display(result)

# COMMAND ----------

sql = """
SELECT count(*) as RecordCount
,sum(Amount) from {0}
""".format(databaseTableName)
df=(spark.sql(sql))
display(df)

# COMMAND ----------

sql = """
REFRESH TABLE {0}
""".format(databaseTableName)
df=(spark.sql(sql))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL Examples

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC 'Latest Version'
# MAGIC ,sum(TotalPayments) as TotalPayments
# MAGIC ,sum(TotalCharges) as Totalcharges
# MAGIC FROM client000000001.FactCostOfCare_tt VERSION AS OF 3
# MAGIC UNION
# MAGIC SELECT 
# MAGIC 'Previous Version'
# MAGIC ,sum(TotalPayments) as TotalPayments
# MAGIC ,sum(TotalCharges) as Totalcharges
# MAGIC FROM client000000001.FactCostOfCare_tt VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM client000000001.FactCostOfCare_tt
# MAGIC UNION
# MAGIC SELECT count(*) FROM client000000001.FactCostOfCare_tt VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC #### R Examples

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)

# COMMAND ----------

# MAGIC %r
# MAGIC sparkR.session()
# MAGIC 
# MAGIC results <- sql("SELECT * from client000000001.factcostofcare_tt")
# MAGIC 
# MAGIC # results is now a SparkDataFrame
# MAGIC #head(results)
# MAGIC 
# MAGIC str(results)
# MAGIC 
# MAGIC results <- filter(results, results$UniquePatients>0)

# COMMAND ----------

# MAGIC %r
# MAGIC head(summarize(groupBy(results, results$FirmSubType), count = n(results$FirmSubType)))

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC display(head(agg(cube(results, "FirmSubType"), avg(results$UniquePatients),  sum(results$TotalCharges))))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean-Up

# COMMAND ----------

sql = """
DROP TABLE IF EXISTS {0}
""".format(databaseTableName)
spark.sql(sql)