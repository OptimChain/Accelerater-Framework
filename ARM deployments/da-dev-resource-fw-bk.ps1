# -----------------------------------------------------------------------
# Bartell Drugs  - Azure Deployment Automation
#   Azure Platform
#   BI and Analytics - Framework Databrick
#   Development Environment Deployment
#
# 23 August 2019
# Chris Kurt
# Neudesic, LLC
# -----------------------------------------------------------------------

#Ensure Subscription is set to Bartell Drugs Development Instance
Set-AzContext -Subscriptionid "a9736b54-71f3-4a19-bd5b-d3979b8ce04f"

$filePath = "\"
$templateFilePath = $filePath + "da-dev-wus2-analytics-fw-bk.template.json"
$templateParameterFilePath = $filePath + "da-dev-wus2-analytics-fw-bk.parameters.json"

$resourceGroupName = "da-dev-wus2-analytics-rg"
$resourceGroupLocation = "westus2"

#Create or check for existing resource group
$resourceGroup = Get-AzResourceGroup -Name $resourceGroupName -ErrorAction SilentlyContinue
if (!$resourceGroup) {
    Write-Host "Creating resource group '$resourceGroupName' in location '$resourceGroupLocation'";
    New-AzResourceGroup -Name $resourceGroupName -Location $resourceGroupLocation
}
else {
    Write-Host "Using existing resource group '$resourceGroupName'";
}

# Set deployment parameters
$workspaceList = "da-dev-wus2-analytics-fw-bk", "da-dev-wus2-data001-bk"

foreach ($workspaceName in $workspaceList) {
    Write-Host "Starting deployment..." $workspaceName;
    $timestamp = ((Get-Date).ToString("MM-dd-yyyy-hh-mm-ss"))
    $deploymentName = $workspaceName + $timestamp
     
    if (Test-Path $templateParameterFilePath) {
        New-AzResourceGroupDeployment -Name $deploymentName -ResourceGroupName $resourceGroupName -TemplateFile $templateFilePath -TemplateParameterFile $templateParameterFilePath -Mode Incremental -Verbose -workspaceName $workspaceName;
    }
    else {
        New-AzResourceGroupDeployment -Name $deploymentName -ResourceGroupName $resourceGroupName -TemplateFile $templateFilePath -Mode Incremental -Verbose  -workspaceName $workspaceName;
    }
}
