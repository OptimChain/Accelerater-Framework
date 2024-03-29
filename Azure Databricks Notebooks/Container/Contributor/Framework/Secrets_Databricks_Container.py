# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC 
# MAGIC # Secrets-Databricks-Container
# MAGIC 
# MAGIC Run this notebook to get the secrets from Databricks Secret Scopes saved to both Python and Scala variables. You can run this notebook from another notebook and use these variables by using the following %run magic command from that notebook: 
# MAGIC 
# MAGIC %run /Framework/Secrets-Databricks-Container
# MAGIC 
# MAGIC For security purposes, the secrets are NOT cached in a global temporary view which exists at the spark application scope.  (if using Global temporary view the values can be read and used by anyone with access to the spark cluster)
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 1. Databricks Secret Scopes with the above secrets created (recommended: create per environment (e.g. Dev, QA, Staging, Prod) and per scope (e.g. Admin, Publish, Contribute, Read))
# MAGIC     
# MAGIC ### References
# MAGIC <a href=https://docs.azuredatabricks.net/user-guide/secrets/example-secret-workflow.html#example-secret-workflow>Secret Workflow Example</a>

# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="brtl", label="Container Name")
dbutils.widgets.text(name="groupName", defaultValue="contribute", label="Security Group Name")
secretScopeName = dbutils.widgets.get("containerName") + dbutils.widgets.get("groupName")
secretScopeName

# COMMAND ----------

#Get Container Secrets

#SQLFramework

dbserver = dbutils.secrets.get(scope = secretScopeName, key = "SQLFrameworkServerName")
dbname = dbutils.secrets.get(scope = secretScopeName, key = "SQLFrameworkDatabaseName")
username = dbutils.secrets.get(scope = secretScopeName, key = "SQLFrameworkUserName")
pwd = dbutils.secrets.get(scope = secretScopeName, key = "SQLFrameworkPassword")

#ADLS
adlsTenantId = dbutils.secrets.get(scope = secretScopeName, key = "ADLSTenantId")
adlsClientId = dbutils.secrets.get(scope = secretScopeName, key = "ADLSClientId")
adlsCredential = dbutils.secrets.get(scope = secretScopeName, key = "ADLSCredential")
                                                                                                     
#ADLS Gen2
adlsGen2StorageAccountName = dbutils.secrets.get(scope = secretScopeName, key = "ADLSGen2StorageAccountName")

#variables
containerName = dbutils.widgets.get("containerName")


# COMMAND ----------

# MAGIC %scala
# MAGIC val secretScopeName:String = "brtlcontribute"
# MAGIC val dbserver = dbutils.secrets.get(scope = secretScopeName, key = "SQLFrameworkServerName")
# MAGIC val dbname = dbutils.secrets.get(scope = secretScopeName, key = "SQLFrameworkDatabaseName")
# MAGIC val username = dbutils.secrets.get(scope = secretScopeName, key = "SQLFrameworkUserName")
# MAGIC val pwd = dbutils.secrets.get(scope = secretScopeName, key = "SQLFrameworkPassword")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Spark Conf
# MAGIC Set Spark Conf with the credentials to the ADLS Store

# COMMAND ----------

# MAGIC %md
# MAGIC #### Connect to the Data Lakes
# MAGIC Set Spark Conf with the credentials to the ADLS Store

# COMMAND ----------

#Dev
#ADLS Gen2 using oauth2
adlsClientId = adlsClientId
adlsCredential = adlsCredential 

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", adlsClientId)
spark.conf.set("fs.azure.account.oauth2.client.secret", adlsCredential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/"+adlsTenantId+"/oauth2/token")