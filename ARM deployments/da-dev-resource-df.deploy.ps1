# -----------------------------------------------------------------------
#Azure Deployment Automation
#   Azure Platform
#   BI and Analytics - Data and Analytics Data Factory
#   Development Environment Deployment
#
# -----------------------------------------------------------------------

#******************************************************************************
function Get-UniqueString ([string]$id, $length = 5) {
    $hashArray = (new-object System.Security.Cryptography.SHA512Managed).ComputeHash($id.ToCharArray())
    -join ($hashArray[1..$length] | ForEach-Object { [char]($_ % 26 + [byte][char]'a') })
}
#******************************************************************************
#Ensure Subscription is set to Bartell Drugs da-dev Subscription
$subscriptionId = "a9736b54-71f3-4a19-bd5b-d3979b8ce04f"
Set-AzContext -Subscriptionid $subscriptionId

$filePath = "\"
$resourceName = "da-dev-wus2-analytics-df"
$uniqueName = $resourceName + "-" + (Get-UniqueString -id $(Get-AzResourceGroup $resourceGroupName).ResourceID)
$templateFilePath = $filePath + $resourceName + ".template.json"
$templateParameterFilePath = $filePath + $resourceName + ".parameters.json"

$resourceGroupName = "da-dev-wus2-analytics-rg"
$resourceGroupLocation = [string]"westus2"

#Create or check for existing resource group
$resourceGroup = Get-AzResourceGroup -Name $resourceGroupName -ErrorAction SilentlyContinue
if (!$resourceGroup) {
    Write-Host "Resource group '$resourceGroupName' does not exist. To create a new resource group, please enter a location.";
    if (!$resourceGroupLocation) {
        $resourceGroupLocation = Read-Host "resourceGroupLocation";
    }
    Write-Host "Creating resource group '$resourceGroupName' in location '$resourceGroupLocation'";
    New-AzResourceGroup -Name $resourceGroupName -Location $resourceGroupLocation

    $resourceTags = @{ `
            "Group Name"      = ""; `
            "Budget Code"     = ""; `
            "Project ID"      = ""; `
            "Deployed By"     = "Template@template.onmicrosoft.com"; `
            "Internal Owner"  = ""; `
            "Support Contact" = ""; `
            "SL"              = "1 High Priority"; `
            "PL"              = "1 Confidential"; 
    }
    $resourceTags
  
    # Assign tags to the resource group
    Set-AzResourceGroup -Name $resourceGroupName -Tag $resourceTags
}
else {
    Write-Host "Using existing resource group '$resourceGroupName'";
}


# Start the deployment
Write-Host "Starting deployment...";
$timestamp = ((Get-Date).ToString("MM-dd-yyyy-hh-mm-ss"))
$deploymentName = "da-dev-wus2-analytics-df" + $timestamp

if (Test-Path $templateParameterFilePath) {
    New-AzResourceGroupDeployment -ResourceGroupName $resourceGroupName -Name $deploymentName -TemplateFile $templateFilePath -TemplateParameterFile $templateParameterFilePath -dataFactoryName $uniqueName -Mode Incremental -Verbose;
}
else {
    New-AzResourceGroupDeployment -ResourceGroupName $resourceGroupName -Name $deploymentName -TemplateFile $templateFilePath  -dataFactoryName $uniqueName -Mode Incremental -Verbose;
}