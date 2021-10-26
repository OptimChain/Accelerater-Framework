
function new-KeyVaultSecret ($secretName, $secretValue)
{

   Set-AzKeyVaultSecret -VaultName da-dev-jason-analytics  -Name $secretName -SecretValue (ConvertTo-SecureString -String $secretValue -AsPlainText -Force)
}

$SQLDWServerPassword = "redacted"
$SQLDWServerUserName = "redacted"
$SQLFrameworkPassword = "redacted"

new-KeyVaultSecret "ADLSName" dadevwus2analyticsdlbrbw
new-KeyVaultSecret "ADLSStorageAccountKey" "MYopkDS5qb4KVNR2Krf7GZLy/B2VIkKQn0AbiTEiQAr0fllMW+5AdhG6BayIMD6hShBtkRK2E6o7+ZErhUItuQ=="
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