# -----------------------------------------------------------------------
# Bartell Drugs  - Azure Deployment Automation
#   Azure Platform
#   BI and Analytics - Data Lake Gen 2
#   Development Environment Deployment
#
# 22 August 2019
# Chris Kurt
# Neudesic, LLC
# -----------------------------------------------------------------------

function Get-UniqueString ([string]$id, $length = 4) {
    $hashArray = (new-object System.Security.Cryptography.SHA512Managed).ComputeHash($id.ToCharArray())
    -join ($hashArray[1..$length] | ForEach-Object { [char]($_ % 26 + [byte][char]'a') })
}
#******************************************************************************
#Ensure Subscription is set to Bartell Drugs da-dev Subscription
$subscriptionId = "a9736b54-71f3-4a19-bd5b-d3979b8ce04f"
Set-AzContext -Subscriptionid $subscriptionId

$filePath = "\"
$resourceName = "dadevwus2analyticsdl"
$uniqueName = $resourceName + (Get-UniqueString -id $(Get-AzResourceGroup $resourceGroupName).ResourceID)
$templateFilePath = $filePath + $resourceName + ".template.json"
$templateParameterFilePath = $filePath + $resourceName + ".parameters.json"

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

# Start the deployment
Write-Host "Starting deployment...";
$timestamp = ((Get-Date).ToString("MM-dd-yyyy-hh-mm-ss"))
$deploymentName = "dadevwus2analyticsdl" + $timestamp

if (Test-Path $templateParameterFilePath) {
    New-AzResourceGroupDeployment -Name $deploymentName -ResourceGroupName $resourceGroupName -storageAccountName $uniqueName -TemplateFile $templateFilePath -TemplateParameterFile $templateParameterFilePath -Mode Incremental -Verbose;
}
else {
    New-AzResourceGroupDeployment -Name $deploymentName -ResourceGroupName $resourceGroupName -storageAccountName $uniqueName -TemplateFile $templateFilePath -Mode Incremental -Verbose;
}
