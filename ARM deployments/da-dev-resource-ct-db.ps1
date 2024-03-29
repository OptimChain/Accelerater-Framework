# -----------------------------------------------------------------------
# Azure Deployment Automation
#   Azure Platform
#   BI and Analytics - Framework Database and Server
#   Development Environment Deployment
#
# -----------------------------------------------------------------------

function Get-UniqueString ([string]$id, $length = 5) {
    $hashArray = (new-object System.Security.Cryptography.SHA512Managed).ComputeHash($id.ToCharArray())
    -join ($hashArray[1..$length] | ForEach-Object { [char]($_ % 26 + [byte][char]'a') })
}
#******************************************************************************
#Ensure Subscription is set to Bartell Drugs da-dev Subscription
$subscriptionId = "57b50573-d245-486d-b3b3-7a5e83dacbb8"
Set-AzContext -Subscriptionid $subscriptionId

$filePath = "C:\Users\Jason.Bian\Documents\GitHub\Accelerater-Framework\ARM deployments\"

$resourceName = "da-dev-resource-ct-db"
$serverName = "da-dev-resource-dbs"

$uniqueName = $serverName + "-" + (Get-UniqueString -id $(Get-AzResourceGroup $resourceGroupName).ResourceID)

$templateFilePath = $filePath + $resourceName + ".template.json"
$templateParameterFilePath = $filePath + $resourceName + ".parameters.json"

$resourceGroupName = "da-dev-wus2-analytics-rg"
$resourceGroupLocation = [string]"westus2"

#Create or check for existing resource group
$resourceGroup = Get-AzResourceGroup -Name $resourceGroupName -ErrorAction SilentlyContinue
if (!$resourceGroup) {
    Write-Host "Creating resource group '$resourceGroupName' in location '$resourceGroupLocation'";
    New-AzResourceGroup -Name $resourceGroupName -Location $resourceGroupLocation
}
else {
    Write-Host "Using existing resource group '$resourceGroupName'";
}

# Start the deployment
Write-Host "Starting deployment...";
$timestamp = ((Get-Date).ToString("MM-dd-yyyy-hh-mm-ss"))
$deploymentName = "da-dev-wus2-analytics-fw-db" + $timestamp

if (Test-Path $templateParameterFilePath) {
    New-AzResourceGroupDeployment -Name $deploymentName -ResourceGroupName $resourceGroupName -sqlservername $uniqueName -TemplateFile $templateFilePath -TemplateParameterFile $templateParameterFilePath -Mode Incremental -Verbose;
}
else {
    New-AzResourceGroupDeployment -Name $deploymentName -ResourceGroupName $resourceGroupName -sqlservername $uniqueName -TemplateFile $templateFilePath -Mode Incremental -Verbose;
}
