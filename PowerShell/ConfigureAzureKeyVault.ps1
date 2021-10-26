
function new-KeyVaultSecret ($secretName, $secretValue)
{

   Set-AzKeyVaultSecret -VaultName da-dev-jason-analytics  -Name $secretName -SecretValue (ConvertTo-SecureString -String $secretValue -AsPlainText -Force)
}

$SQLDWServerPassword = "redacted"
$SQLDWServerUserName = "redacted"
$SQLFrameworkPassword = "redacted"

new-KeyVaultSecret "ADLSName" dadevresourcedlbrbw
new-KeyVaultSecret "ADLSStorageAccountKey" "9qc7pVeGRCd8ff56Ha2JcSY053SImBLfZ6iX4v9oTQWqYIdThMooacUr/LoZz7XudBREt4mhqFcVJRCd00pc8A=="
new-KeyVaultSecret "ADLSTenantId" 1e3e71be-fcca-4284-9031-688cc8f37b6b
new-KeyVaultSecret "SQLFrameworkConnectionString" 'Server=tcp:rsm-entdata-dev.database.windows.net, 1433; Initial Catalog=rsm_ed_fw_db; Persist Security Info=False; User ID=BartellAdmin; Password='REDACTED'/[659; MultipleActiveResultSets=False; Encrypt=True; TrustServerCertificate=False; Connection Timeout=30;
new-KeyVaultSecret "SQLFrameworkServerName"' "rsm-entdata-dev.database.windows.net"
new-KeyVaultSecret "SQLFrameworkDatabaseName" rsm_ed_fw_db
new-KeyVaultSecret "SQLFrameworkPassword" $SQLFrameworkPassword
new-KeyVaultSecret "SQLFrameworkUserName" BartellAdmin
new-KeyVaultSecret "SQLDWServerName" "rsmentdatadev-ondemand.sql.azuresynapse.net"
new-KeyVaultSecret "SQLDWDatabaseName" rsmentdatadev
new-KeyVaultSecret "SQLDWPassword" $SQLDWServerPassword
new-KeyVaultSecret "SQLDWUserName" $SQLDWServerUserName
new-KeyVaultSecret "DatabricksFrameworkAdminToken" "Update manually"  
new-KeyVaultSecret "DatabricksDataAdminToken" "Update manually"
new-KeyVaultSecret "AdminADGroupID" "6ade6263-38f5-4ed0-bb7a-51e17032bbd4"
new-KeyVaultSecret "ContributorADGroupID" "f7ab9864-a4a4-4c48-a49f-e1d5fbfa3af2"
new-KeyVaultSecret "PublisherADGroupID" "a31bcba2-2d73-47bb-88fd-60a78ea28b9a
"
new-KeyVaultSecret "ReaderADGroupID" "1765a305-89f3-4cfb-ac46-62520ec79ee9"