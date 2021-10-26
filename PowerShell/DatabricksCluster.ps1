param
(
    [Parameter(Position=0)]
    [string]$tokensecret = "",
    [Parameter(Position=1)]    
    [string]$clustername = "customername-environmentname-projectname-cls01",
    [Parameter(Position=2)]
    [string]$apiversion = "2.0",
    [Parameter(Position=3)]
    [string]$sparkversion = "6.4.x-scala2.11",
    [Parameter(Position=4)]
    [string]$machine = "",
    [Parameter(Position=5)]
    [string]$nodetype = "Standard_DS3_v2",
    [Parameter(Position=6)]
    [string]$drivernodetype = "Standard_DS3_v2", 
    [Parameter(Position=7)]
    [int]$num_workers = 3,
    [Parameter(Position=8)]
    [int]$autoterminationminutes = 60,
    [Parameter(Position=9)]
    [string]$RGName = "",
	[Parameter(Position=10)]
    [string]$TagName1 = "",
    [Parameter(Position=11)]
    [string]$TagName1Value = "",
	[Parameter(Position=12)]
    [string]$TagName2 = "",
    [Parameter(Position=13)]
    [string]$TagName2Value = "",
    [Parameter(Position=14)]
    [string]$TagName3 = "",
    [Parameter(Position=15)]
    [string]$TagName3Value = "",
    [Parameter(Position=16)]
    [string]$TagName4 = "",
    [Parameter(Position=17)]
    [string]$TagName4Value = ""        	
)
function get-DatabricksClusters
{
        $clusterlisturi = "$uribase/clusters/list"
        $clustersjson = Invoke-WebRequest -Uri $clusterlisturi `
                -Method Get `
                -Headers @{"Authorization"="Bearer $tokensecret"} `
                -ContentType "application/json" 
        return $clustersjson
}

function new-DatabricksCluster
{
        $clustersjson = get-DatabricksClusters
        $clusters = ConvertFrom-Json $clustersjson
        $clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $clustername} | Select-Object "cluster_id" | ConvertTo-Json
        if(!$clusterdata)
        {

            $clusterinfo = @{}
            $clusterinfo.cluster_name = $clustername
            $clusterinfo.spark_version = $sparkversion
            $clusterinfo.node_type_id = $nodetype
            $clusterinfo.driver_node_type_id = $drivernodetype
            $clusterinfo.num_workers = $num_workers
            $clusterinfo.autotermination_minutes = $autoterminationminutes
            $clusterinfo.spark_conf = @{"spark.databricks.service.server.enabled"="true"; "spark.extraListeners"="com.databricks.backend.daemon.driver.DBCEventLoggingListener,org.apache.spark.listeners.UnifiedSparkListener";"spark.databricks.service.port"="8787"; "spark.databricks.delta.preview.enabled"="true"; "spark.unifiedListener.logBlockUpdates"="false"}
            $clusterinfo.spark_env_vars = @{"PYSPARK_PYTHON"="/databricks/python3/bin/python3"}
            $clusterinfo.custom_tags = @{$TagName1=$TagName1Value; $TagName2=$TagName2Value; $TagName3=$TagName3Value; $TagName4=$TagName4Value}
			$clusterinfo.init_scripts = @()
            $clusterinfo = New-Object -TypeName psobject -Property $clusterinfo
            $clusterjson = $clusterinfo | ConvertTo-Json -Depth 10
            $clusterJson

            $createclusteruri = "$uribase/clusters/create"
            $createclusterresponse = Invoke-WebRequest -Uri $createclusteruri `
                -Method Post `
                -Headers @{"Authorization"="Bearer $tokensecret"} `
                -ContentType "application/json" `
                -Body $clusterJson

            if ($createclusterresponse.StatusCode -ne 200)
            {
                Write-Host "Create Cluster Failed" -ForegroundColor Red
            }
            return $createclusterresponse			


        }
        else {
                Write-Host "cluster already exists"
        }
}

$uribase = "$machine/api/$apiversion"

#Create a new databricks cluster
$response = new-DatabricksCluster
$response