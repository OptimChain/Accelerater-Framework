# Run the Install only once
# Install-Module azure.databricks.cicd.tools
# Import-Module azure.databricks.cicd.tools
#Install-Module Az.KeyVault

$VaultName = 'da-dev-jason-analytics'


#$Ctx = New-AzStorageContext -StorageAccountName dadevwus2analyticsdlbrbw -StorageAccountKey ''

$FrameworkBearerToken = ''
$ContainerBearerToken = ''


Import-DatabricksFolder -BearerToken $FrameworkBearerToken -Region $Region -LocalPath 'C:\Users\Jason.Bian\Desktop\Solution\Azure Databricks Notebooks\Framework\
