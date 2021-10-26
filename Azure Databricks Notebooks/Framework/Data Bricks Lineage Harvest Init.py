# MAGIC %md
# MAGIC # Start harvest data lineage

# COMMAND ----------

# MAGIC %run /Framework/Secrets-Databricks-Cache

# COMMAND ----------

# MAGIC %run /Framework/Neudesic_Framework_Functions

# COMMAND ----------

from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import PurviewClient,AtlasEntity

auth = ServicePrincipalAuthentication(
      tenant_id = adlstenantid,
      client_id = adlsclientid,
      client_secret = adlscredential
)

  
client = PurviewClient(
    account_name = "rsmedpovpa",
    authentication = auth
)

# COMMAND ----------

all_type_defs = client.get_all_typedefs()
all_type_defs

# COMMAND ----------

entity1 = client.get_entity(qualifiedName=,typeName=)

entity2 = client.get_entity(qualifiedName=,typeName=)

# COMMAND ----------

from pyapacheatlas.core import AtlasProcess
import json

notebook_name = ""
notebook_type_name = "process"

newLineage = AtlasProcess(
  name = "Notebook Processing",
  typeName = notebook_type_name,
  qualified_name = notebook_name,
  inputs = [entity1["entities"][0]],
  outputs = [entity2["entities"][0]],
  guid = 102
)

results = client.upload_entities(
    batch = [newLineage])
print(json.dumps(results,indent=2))

# COMMAND ----------

# MAGIC %scala
# MAGIC import za.co.absa.spline.harvester.conf.StandardSplineConfigurationStack
# MAGIC import za.co.absa.spline.harvester.extra.UserExtraMetadataProvider
# MAGIC import za.co.absa.spline.harvester.HarvestingContext
# MAGIC import org.apache.commons.configuration.Configuration
# MAGIC import za.co.absa.spline.harvester.SparkLineageInitializer._
# MAGIC import za.co.absa.spline.harvester.conf.DefaultSplineConfigurer
# MAGIC import za.co.absa.spline.producer.model._
# MAGIC import scala.util.parsing.json.JSON
# MAGIC 
# MAGIC val splineConf: Configuration = StandardSplineConfigurationStack(spark)
# MAGIC 
# MAGIC spark.enableLineageTracking(new DefaultSplineConfigurer(splineConf) {
# MAGIC   //override protected def userExtraMetadataProvider = new UserExtraMetaDataProvider {
# MAGIC   //val test = dbutils.notebook.getContext.notebookPath
# MAGIC   val notebookInformationJson = dbutils.notebook.getContext.toJson
# MAGIC   val outerMap = JSON.parseFull(notebookInformationJson).getOrElse(0).asInstanceOf[Map[String,String]]
# MAGIC   val tagMap = outerMap("tags").asInstanceOf[Map[String,String]]
# MAGIC 
# MAGIC   val extraContextMap = outerMap("extraContext").asInstanceOf[Map[String,String]]
# MAGIC   val notebookPath = extraContextMap("notebook_path").split("/")
# MAGIC   
# MAGIC   val notebookURL = tagMap("browserHostName")+"/?o="+tagMap("orgId")+tagMap("browserHash")
# MAGIC   val user = tagMap("user")
# MAGIC   val name = notebookPath(notebookPath.size-1)
# MAGIC   
# MAGIC   val notebookInfo = Map("notebookURL" -> notebookURL,  
# MAGIC                 "user" -> user, 
# MAGIC                 "name" -> name,
# MAGIC                 "mounts" -> dbutils.fs.ls("/mnt").map(_.path),
# MAGIC                 "timestamp" -> System.currentTimeMillis)
# MAGIC   val notebookInfoJson = scala.util.parsing.json.JSONObject(notebookInfo)
# MAGIC   
# MAGIC   override protected def userExtraMetadataProvider: UserExtraMetadataProvider = new UserExtraMetadataProvider {
# MAGIC     override def forExecEvent(event: ExecutionEvent, ctx: HarvestingContext): Map[String, Any] = Map("foo" -> "bar1")
# MAGIC     override def forExecPlan(plan: ExecutionPlan, ctx: HarvestingContext): Map[String, Any] = Map("notebookInfo" -> notebookInfoJson) // tilføj mount info til searchAndReplace  denne funktion indeholder infoen
# MAGIC     override def forOperation(op: ReadOperation, ctx: HarvestingContext): Map[String, Any] = Map("foo" -> "bar3")
# MAGIC     override def forOperation(op: WriteOperation, ctx: HarvestingContext): Map[String, Any] = Map("foo" -> "bar4")
# MAGIC     override def forOperation(op: DataOperation, ctx: HarvestingContext): Map[String, Any] = Map("foo" -> "bar5")
# MAGIC   }
# MAGIC })