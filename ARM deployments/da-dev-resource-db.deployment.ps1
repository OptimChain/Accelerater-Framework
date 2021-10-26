function Get-UniqueString ([string]$id, $length = 5) {
    $hashArray = (new-object System.Security.Cryptography.SHA512Managed).ComputeHash($id.ToCharArray())
    -join ($hashArray[1..$length] | ForEach-Object { [char]($_ % 26 + [byte][char]'a') })
}
#******************************************************************************


Set-AzContext -Subscriptionid "a9736b54-71f3-4a19-bd5b-d3979b8ce04f"

$filePath = "\"
$templateFilePath = $filePath + "da-dev-wus2-analytics-dw-db.template.json"
$templateParameterFilePath = $filePath + "da-dev-wus2-analytics-dw-db.parameters.json"


$dbsname = "da-dev-wus2-analytics-dbs"
$uniqueName = $dbsname + "-" + (Get-UniqueString -id $(Get-AzResourceGroup $resourceGroupName).ResourceID)

$resourceGroupName = "da-dev-wus2-analytics-rg"
$resourceGroupLocation = [string]"westus2"

# Create Resource Group
#Create or check for existing resource group
$resourceGroup = Get-AzResourceGroup -Name $resourceGroupName -ErrorAction SilentlyContinue
if (!$resourceGroup) {
    Write-Host "Resource group '$resourceGroupName' does not exist.";
    Write-Host "Creating resource group '$resourceGroupName' in location '$resourceGroupLocation'";

    New-AzResourceGroup -Name $resourceGroupName -Location $resourceGroupLocation
    $resourceTags = @{ `
            "Group Name"      = "Data and Analytics"; `
            "Budget Code"     = ""; `
            "Project ID"      = ""; `
            "Deployed By"     = "daniel.nayberger@neudesic.com"; `
            "Internal Owner"  = ""; `
            "Support Contact" = ""; `
            "SL"              = "3 Low Priority"; `
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
$deploymentName = "da-dev-wus2-analytics-dw-db" + "-" + $timestamp

if (Test-Path $templateParameterFilePath) {
    New-AzResourceGroupDeployment -ResourceGroupName $resourceGroupName -Name $deploymentName -dbsname $uniqueName -TemplateFile $templateFilePath -TemplateParameterFile $templateParameterFilePath -Mode Incremental -Verbose;
}
else {
    New-AzResourceGroupDeployment -ResourceGroupName $resourceGroupName -Name $deploymentName -dbsname $uniqueName -TemplateFile $templateFilePath -Mode Incremental -Verbose;
}