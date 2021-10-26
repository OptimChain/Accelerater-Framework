
function new-KeyVaultSecret ($secretName, $secretValue)
{

   Set-AzKeyVaultSecret -VaultName da-dev-wus2-analytics-kv  -Name $secretName -SecretValue (ConvertTo-SecureString -String $secretValue -AsPlainText -Force)
}


new-KeyVaultSecret "DatabricksFrameworkAdminToken" "dapi154909ff395c01d29097538dcb87e5df"  
new-KeyVaultSecret "DatabricksDataAdminToken" "dapi9bcf06a928521a257225c6a9404d3598"