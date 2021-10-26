# MAGIC  
# MAGIC %md
# MAGIC # Create Container Database 
# MAGIC 
# MAGIC #### Usage
# MAGIC Creates Container Database
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 
# MAGIC #### Details

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize

# COMMAND ----------

dbutils.widgets.text(name="containerName", defaultValue="client000000002", label="Container Name")
containerName = dbutils.widgets.get("containerName")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Database

# COMMAND ----------

display(spark.sql("CREATE DATABASE IF NOT EXISTS " + containerName))

# COMMAND ----------

#display(spark.sql("DROP DATABASE client0000001"))