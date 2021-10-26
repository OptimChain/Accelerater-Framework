# MAGIC  
# MAGIC %md
# MAGIC 
# MAGIC # Modify Schema
# MAGIC 
# MAGIC #### Example:
# MAGIC 
# MAGIC #### Usage:        

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

# MAGIC %run ./Secrets-Databricks-Cache

# COMMAND ----------

# MAGIC %run ./Neudesic_Framework_Functions

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Starting
# MAGIC val notebookPath = dbutils.notebook.getContext.notebookPath.get
# MAGIC val logMessage = "Starting"
# MAGIC val notebookContext = dbutils.notebook.getContext().toJson
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

dbutils.widgets.text(name="dataPath", defaultValue="/2015/01/01/FactMeterIntervalReads20150101.txt", label="Data Path")
dbutils.widgets.text(name="tableName", defaultValue="EE_MeterData", label="Table Name")

dataPath = dbutils.widgets.get("dataPath")
tableName = dbutils.widgets.get("tableName")
schemasPath = adlbasepath + "/Schemas/" + tableName
schemaFile = schemasPath + "/Schema.json"

# COMMAND ----------

print("Data Path: {0}".format(dataPath))
print("Schemas Path: {0}".format(schemasPath))
print("Schema File: {0}".format(schemaFile))
print("Table Name: {0}".format(tableName))

# COMMAND ----------

originalSchema = get_table_schema(dataPath, tableName)
originalSchema

# COMMAND ----------

dataTypeSwitcher = {
  "integer":"IntegerType()",
  "double":"DoubleType()",
  "string":"StringType()",
  "long":"LongType()",
  "boolean":"BooleanType()"
                   }

def dataTypeTranslator(argument):
  returnArgument = dataTypeSwitcher.get(argument)
  return returnArgument

# COMMAND ----------

schemaStruct = "StructType(["
for structField in originalSchema:
  name = structField.jsonValue()["name"]
  dataType = dataTypeTranslator(structField.jsonValue()["type"])
  nullable = structField.jsonValue()["nullable"]
  field = "StructField('{0}',{1},{2}),".format(name, dataType, nullable)
  schemaStruct += field
schemaStruct = schemaStruct[:-1] + "])"
print(schemaStruct)

# COMMAND ----------

# MAGIC %md
# MAGIC Copy the results of cmd10 and paste them as the value of the newSchema variable in cmd 12.  Make any replacements as required

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
newSchema = StructType([StructField('UniqueId1',StringType(),True),StructField('UniqueId2',StringType(),True),StructField('ReadingDate',StringType(),True),StructField('SecondsSinceMidnight',IntegerType(),True),StructField('KWUsed',DoubleType(),True)])

# COMMAND ----------

newSchema.json()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert Schema to JSON and Save to Data Lake

# COMMAND ----------

try:
  schema_json = newSchema.json()
  dbutils.fs.mkdirs(schemasPath)
  dbutils.fs.put(schemaFile, schema_json, True)
except Exception as e:
  sourceName = "Modify Schema: Convert Schema to JSON and Save to Data Lake"
  errorCode = "400"
  errorDescription = e
  raise(e)

# COMMAND ----------

dbutils.fs.head(schemaFile)

# COMMAND ----------

review = get_table_schema(dataPath, tableName)
review

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log Completion

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Completed
# MAGIC val logMessage = "Completed"
# MAGIC val notebookContext = ""
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 