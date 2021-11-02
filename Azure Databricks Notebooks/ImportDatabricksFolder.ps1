# Run the Install only once
# Install-Module azure.databricks.cicd.tools
# Import-Module azure.databricks.cicd.tools
#Install-Module Az.KeyVault

$VaultName = 'da-dev-jason-analytics'

#WestUs2 is Location

#$Ctx = New-AzStorageContext -StorageAccountName dadevwus2analyticsdlbrbw -StorageAccountKey "MYopkDS5qb4KVNR2Krf7GZLy/B2VIkKQn0AbiTEiQAr0fllMW+5AdhG6BayIMD6hShBtkRK2E6o7+ZErhUItuQ=="

$FrameworkBearerToken = "dapie694e85ad5fa1294bc2e4d67f3638fef"
$ContainerBearerToken = "dapi38e33c52fb5c497e229b5aabdf6780ad"


#Import-DatabricksFolder -BearerToken $FrameworkBearerToken -Region $Region -LocalPath #'C:\Users\Jason.Bian\Desktop\Solution\Azure Databricks Notebooks\Framework\' -DatabricksPath '/Framework'

Import-DatabricksFolder -BearerToken $ContainerBearerToken -Region $Region -LocalPath 'C:\Users\Jason.Bian\Desktop\Solution\Azure Databricks Notebooks\Container\' -DatabricksPath '/Container'

