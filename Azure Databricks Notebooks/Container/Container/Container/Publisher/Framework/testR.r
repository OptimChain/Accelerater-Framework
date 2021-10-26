# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC %md
# MAGIC # testR
# MAGIC ###### Author: Mike Sherrill 10/29/19
# MAGIC 
# MAGIC Example R notebook, including SparkR, sparklyr, dplyr, and ggplot2 libraries
# MAGIC 
# MAGIC #### Usage
# MAGIC Just samples and testing
# MAGIC 
# MAGIC #### Prerequisites
# MAGIC 
# MAGIC #### Details

# COMMAND ----------

library(SparkR)

# COMMAND ----------

 # Installing latest version of Rcpp
install.packages("Rcpp") 

if (!require("sparklyr")) {
  install.packages("sparklyr")  
}

# COMMAND ----------

library(sparklyr)

# COMMAND ----------

sc <- spark_connect(method = "databricks")

# COMMAND ----------

iris_tbl <- copy_to(sc, iris)

# COMMAND ----------

library(dplyr)

# COMMAND ----------

src_tbls(sc)

# COMMAND ----------

iris_tbl %>% count

# COMMAND ----------

# Changing default plot height 
options(repr.plot.height = 600)

# COMMAND ----------

iris_summary <- iris_tbl %>% 
  mutate(Sepal_Width = ROUND(Sepal_Width * 2) / 2) %>% # Bucketizing Sepal_Width
  group_by(Species, Sepal_Width) %>% 
  summarize(count = n(), Sepal_Length = mean(Sepal_Length), stdev = sd(Sepal_Length)) %>% collect

# COMMAND ----------

library(ggplot2)

ggplot(iris_summary, aes(Sepal_Width, Sepal_Length, color = Species)) + 
  geom_line(size = 1.2) +
  geom_errorbar(aes(ymin = Sepal_Length - stdev, ymax = Sepal_Length + stdev), width = 0.05) +
  geom_text(aes(label = count), vjust = -0.2, hjust = 1.2, color = "black") +
  theme(legend.position="top")

# COMMAND ----------

str(iris)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC sql = """
# MAGIC SELECT * from client000000001.factcostofcare
# MAGIC """
# MAGIC df=(spark.sql(sql))
# MAGIC display(df)

# COMMAND ----------

sparkR.session()

results <- sql("SELECT * from client000000001.factcostofcare")

# results is now a SparkDataFrame
#head(results)

str(results)

results <- filter(results, results$UniquePatients>0)

# COMMAND ----------


head(summarize(groupBy(results, results$FirmSubType), count = n(results$FirmSubType)))


# COMMAND ----------

#head(agg(cube(results, "FirmSubType"), avg(results$UniquePatients),  sum(results$TotalCharges)))

display(head(agg(cube(results, "FirmSubType"), avg(results$UniquePatients),  sum(results$TotalCharges))))

# COMMAND ----------

#library(SparkR)

#sparkR.session()

providers <- sql("SELECT * from client000000001.dimProvider")
costofcare <- sql("SELECT * from client000000001.factcostofcare")

joined <- join(providers, costofcare, providers$ProviderKey == costofcare$ProviderKey)

str(joined)

#display(select(joined,"totalpayments"))