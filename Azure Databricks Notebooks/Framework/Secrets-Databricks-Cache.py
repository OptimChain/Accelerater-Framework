# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC 
# MAGIC # Secrets-Databricks-Cache
# MAGIC 
# MAGIC ###### Author: Mike Sherrill 6/10/19
# MAGIC ###### Updated: Mike Sherill 12/11/19 - removed ADLSGen1 and corresponding Credential secrets
# MAGIC 
# MAGIC Run this notebook to get the secrets from Databricks Secret Scopes saved to both Python and Scala variables. You can run this notebook from another notebook and use these variables by using the following %run magic command from that notebook: 
# MAGIC 
# MAGIC %run /Framework/Secrets-Databricks-Framework
# MAGIC 
# MAGIC For performance purposes, the secrets are cached in a global temporary view which exists at the spark application scope.  Values will be read in once and all subsequent lookups will come from the global temp view. However, this comes at the expense of security (the values can be read and used by anyone with access to the spark catalog).  
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Databricks Secret Scopes with the above secrets created (recommended: create per environment (e.g. Dev, QA, Staging, Prod)
# MAGIC     
# MAGIC ### References
# MAGIC <a href=https://docs.azuredatabricks.net/user-guide/secrets/example-secret-workflow.html#example-secret-workflow>Secret Workflow Example</a>

# COMMAND ----------

# MAGIC %md
# MAGIC #### NOTE: Cache is NOT used for Containers other than the Framework due to security 
# MAGIC This section only runs if the global secrets cache doesn't exist, otherwise it will create it.  This cache exists at the global application scope, so this table will be built once per application execution. 

# COMMAND ----------

def build_secrets_global_temp_view(dbserver, dbname, username, pwd, adlsgen2storageaccountname, adlsgen2storageaccountkey, adlstenantid, adlsclientid, adlscredential, sqldwservername, sqldwdatabasename, sqldwusername, sqldwpassword, analyticsdbname):
  from pyspark.sql.types import StructType, StructField, StringType

  secretsSchema = StructType([
  #framework db
  StructField("dbserver", StringType(), True),
  StructField("dbname", StringType(), True),
  StructField("username", StringType(), True),
  StructField("pwd", StringType(), True),
  #adls gen 2 (data lake store)
  StructField("adlsgen2storageaccountname", StringType(), True),
  StructField("adlsgen2storageaccountkey", StringType(), True),
  StructField("adlstenantid", StringType(), True),
  StructField("adlsclientid", StringType(), True),
  StructField("adlscredential", StringType(), True),
  #azure sql dw
  StructField("sqldwservername", StringType(), True),
  StructField("sqldwdatabasename", StringType(), True),
  StructField("sqldwusername", StringType(), True),
  StructField("sqldwpassword", StringType(), True),
  StructField("analyticsdbname", StringType(), True)
  ])

  secrets = {"dbserver":dbserver, 
             "dbname":dbname, 
             "username":username, 
             "pwd":pwd, 
             "adlsgen2storageaccountname": adlsgen2storageaccountname,
             "adlsgen2storageaccountkey": adlsgen2storageaccountkey,
             "adlstenantid":adlstenantid,
             "adlsclientid":adlsclientid,
             "adlscredential":adlscredential,
             "sqldwservername": sqldwservername,
             "sqldwdatabasename": sqldwdatabasename,
             "sqldwusername": sqldwusername,
             "sqldwpassword": sqldwpassword,
             "analyticsdbname": analyticsdbname
            }

  json_rdd=sc.parallelize([secrets]) 
  secretsDF = json_rdd.toDF(secretsSchema)
  secretsDF.createOrReplaceGlobalTempView("secrets_admin")

# COMMAND ----------

if len([s.name for s in spark.catalog.listTables("global_temp") if s.name=='secrets_admin']) > 0:
  found = True
else:
  found = False
  
if (found == False):
  #framework azure sql db
  dbserver = dbutils.secrets.get(scope = "framework", key = "SQLFrameworkServerName")
  dbname = dbutils.secrets.get(scope = "framework", key = "SQLFrameworkDatabaseName")
  username = dbutils.secrets.get(scope = "framework", key = "SQLFrameworkUserName")
  pwd = dbutils.secrets.get(scope = "framework", key = "SQLFrameworkPassword")
  #data lake store
  adlsgen2storageaccountname = dbutils.secrets.get(scope = "framework", key = "ADLSGen2StorageAccountName")
  adlsgen2storageaccountkey = dbutils.secrets.get(scope = "framework", key = "ADLSGen2StorageAccountKey")  
  adlstenantid = dbutils.secrets.get(scope = "framework", key = "ADLSTenantId")
  adlsclientid = dbutils.secrets.get(scope = "framework", key = "ADLSClientId")
  adlscredential = dbutils.secrets.get(scope = "framework", key = "ADLSCredential")
  sqldwservername = dbutils.secrets.get(scope = "framework", key = "SQLDWServerName")
  sqldwdatabasename = dbutils.secrets.get(scope = "framework", key = "SQLDWDatabaseName")
  sqldwusername = dbutils.secrets.get(scope = "framework", key = "SQLDWUserName")
  sqldwpassword = dbutils.secrets.get(scope = "framework", key = "SQLDWPassword")
  analyticsdbname = "" #dbutils.secrets.get(scope = "framework", key = "SQLAnalyticsDatabaseName")
  
  build_secrets_global_temp_view(dbserver, dbname, username, pwd, adlsgen2storageaccountname, adlsgen2storageaccountkey, adlstenantid, adlsclientid, adlscredential, sqldwservername, sqldwdatabasename, sqldwusername, sqldwpassword,analyticsdbname)
 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Python

# COMMAND ----------

secrets = spark.sql("""
SELECT dbserver, dbname, username, pwd, adlsgen2storageaccountname, adlsgen2storageaccountkey, adlstenantid, adlsclientid, adlscredential, sqldwservername, sqldwdatabasename, sqldwusername, sqldwpassword,analyticsdbname
FROM global_temp.secrets_admin
""").collect()

dbserver, dbname, username, pwd, adlsgen2storageaccountname, adlsgen2storageaccountkey, adlstenantid, adlsclientid, adlscredential, sqldwservername, sqldwdatabasename, sqldwusername, sqldwpassword,analyticsdbname= secrets[0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scala

# COMMAND ----------

# MAGIC %scala
# MAGIC val secrets = spark.sql("SELECT dbserver, dbname, username, pwd, adlsgen2storageaccountname, adlsgen2storageaccountkey, adlstenantid, adlsclientid, adlscredential, sqldwservername, sqldwdatabasename, sqldwusername, sqldwpassword, analyticsdbname FROM global_temp.secrets_admin").collect()
# MAGIC                         
# MAGIC val dbserver:String = secrets(0)(0).toString()
# MAGIC val dbname:String = secrets(0)(1).toString()
# MAGIC val username:String = secrets(0)(2).toString()
# MAGIC val pwd:String = secrets(0)(3).toString()
# MAGIC val adlsgen2storageaccountname:String = secrets(0)(4).toString()
# MAGIC val adlsgen2storageaccountkey:String = secrets(0)(5).toString()
# MAGIC val adlstenantid:String = secrets(0)(6).toString()
# MAGIC val adlsclientid:String = secrets(0)(7).toString()
# MAGIC val adlscredential:String = secrets(0)(8).toString()
# MAGIC val sqldwservername:String = secrets(0)(9).toString()
# MAGIC val sqldwdatabasename:String = secrets(0)(10).toString()
# MAGIC val sqldwusername:String = secrets(0)(11).toString()
# MAGIC val sqldwpassword:String = secrets(0)(12).toString()
# MAGIC val analyticsdbname:String = secrets(0)(13).toString()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Spark Conf
# MAGIC Set Spark Conf with the credentials to the ADLS Store

# COMMAND ----------

#ADLS Gen 2
storageAccountName = "fs.azure.account.key.{0}.dfs.core.windows.net".format(adlsgen2storageaccountname)
spark.conf.set(
  storageAccountName,
  adlsgen2storageaccountkey)