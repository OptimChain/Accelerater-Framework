# -----------------------------------------------------------------------
Azure Deployment Automation
#   Azure Platform
#   Enterprise Services dev - WUS2 Development Key vault
#   Development Environment Deployment


# -----------------------------------------------------------------------

#Ensure Subscription is set to Bartell Drugs da-dev Subscription
$subscriptionId = "57b50573-d245-486d-b3b3-7a5e83dacbb8"
Set-AzContext -Subscriptionid $subscriptionId

$filePath = "C:\Users\Jason.Bian\Desktop\Solution\Powershell\deployments\Azure Automation\"
$resourceName = "da-dev-jason-analytics"
$templateFilePath = $filePath + $resourceName + ".template.json"
$templateParameterFilePath = $filePath + $resourceName + ".parameters.json"

$resourceGroupName = "da-dev-wus2-analytics-rg"
$resourceGroupLocation = [string]"westus2"

# Create Network Resource Group
#Create or check for existing resource group
$resourceGroup = Get-AzResourceGroup -Name $resourceGroupName -ErrorAction SilentlyContinue
if (!$resourceGroup) {
    Write-Host "Resource group '$resourceGroupName' does not exist.";
    Write-Host "Creating resource group '$resourceGroupName' in location '$resourceGroupLocation'";

    New-AzResourceGroup -Name $resourceGroupName -Location $resourceGroupLocation
    $resourceTags = @{ `
            "Group Name"      = ""; `
            "Budget Code"     = ""; `
            "Project ID"      = ""; `
            "Deployed By"     = "Daniel.Nayberger@bartelldrugs.onmicrosoft.com"; `
            "Internal Owner"  = ""; `
            "Support Contact" = ""; `
            "SL"              = "1 High Priority"; `
            "PL"              = "1 Confidential"; 
    }

    # Assign tags to the resource group
    Set-AzResourceGroup -Name $resourceGroupName -Tag $resourceTags
}
else {
    Write-Host "Using existing resource group '$resourceGroupName'";
}

# Start the deployment
Write-Host "Starting deployment...";
$timestamp = ((Get-Date).ToString("MM-dd-yyyy-hh-mm-ss"))
$deploymentName = $resourceName + "-" + $timestamp

if (Test-Path $templateParameterFilePath) {
    New-AzResourceGroupDeployment -ResourceGroupName $resourceGroupName -Name $deploymentName -TemplateFile $templateFilePath -TemplateParameterFile $templateParameterFilePath -Mode Incremental -Verbose -ErrorVariable ErrorMessages;
}
else {
    New-AzResourceGroupDeployment -ResourceGroupName $resourceGroupName -Name $deploymentName -TemplateFile $templateFilePath -Mode Incremental -Verbose -ErrorVariable ErrorMessages;
}
$ErrorMessages
