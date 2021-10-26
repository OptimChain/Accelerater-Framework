#Container Secrets


$SubscriptionId = "49c15536-9cf0-44dd-9a1a-a15e20e6af69"
Select-AzSubscription -SubscriptionId $SubscriptionId


$KeyVaultName = "da-dev-jason-analytics"
$Region = "East US"


$ContainerToken = (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "DatabricksDataAdminToken").SecretValueText
$FrameworkToken = (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "DatabricksFrameworkAdminToken").SecretValueText


Connect-Databricks -BearerToken $ContainerToken -Region $Region

Add-DatabricksSecretScope -BearerToken $ContainerToken -Region $Region -ScopeName cgsadmin
Add-DatabricksSecretScope -BearerToken $ContainerToken -Region $Region -ScopeName cgscontribute
Add-DatabricksSecretScope -BearerToken $ContainerToken -Region $Region -ScopeName cgspublish
Add-DatabricksSecretScope -BearerToken $ContainerToken -Region $Region -ScopeName cgsread


Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName ADLSGen2StorageAccountName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ADLSName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName ADLSTenantID -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ADLSTenantID").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName SQLFrameworkServerName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkServerName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName SQLFrameworkDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName SQLFrameworkUserName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkUserName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName SQLFrameworkPassword -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkPassword").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName SQLDWDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName SQLDWServerName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWServerName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName SQLDWUserName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWUserName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName SQLDWPassword -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWPassword").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName SQLAnalyticsDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLAnalyticsDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName AdminADLSClientID -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "AdminADLSClientID").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsadmin -SecretName AdminADLSCredential -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "AdminADLSCredential").SecretValueText

Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName ADLSGen2StorageAccountName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ADLSName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName ADLSTenantID -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ADLSTenantID").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName SQLFrameworkServerName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkServerName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName SQLFrameworkDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName SQLFrameworkUserName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkUserName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName SQLFrameworkPassword -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkPassword").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName SQLDWDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName SQLDWServerName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWServerName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName SQLDWUserName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWUserName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName SQLDWPassword -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWPassword").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName SQLAnalyticsDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLAnalyticsDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName ContributeADLSClientID -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ContributeADLSClientID").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgscontribute -SecretName ContributeADLSCredential -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ContributeADLSCredential").SecretValueText

Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName ADLSGen2StorageAccountName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ADLSName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName ADLSTenantID -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ADLSTenantID").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName SQLFrameworkServerName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkServerName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName SQLFrameworkDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName SQLFrameworkUserName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkUserName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName SQLFrameworkPassword -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkPassword").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName SQLDWDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName SQLDWServerName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWServerName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName SQLDWUserName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWUserName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName SQLDWPassword -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWPassword").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName SQLAnalyticsDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLAnalyticsDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName PublishADLSClientID -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "PublishADLSClientID").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgspublish -SecretName PublishADLSCredential -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "PublishADLSCredential").SecretValueText

Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName ADLSGen2StorageAccountName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ADLSName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName ADLSTenantID -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ADLSTenantID").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName SQLFrameworkServerName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkServerName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName SQLFrameworkDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName SQLFrameworkUserName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkUserName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName SQLFrameworkPassword -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkPassword").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName SQLDWDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName SQLDWServerName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWServerName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName SQLDWUserName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWUserName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName SQLDWPassword -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWPassword").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName SQLAnalyticsDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLAnalyticsDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName ReadADLSClientID -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ReadADLSClientID").SecretValueText 
Set-DatabricksSecret -BearerToken $ContainerToken -ScopeName cgsread -SecretName ReadADLSCredential -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ReadADLSCredential").SecretValueText


#ACLs

Set-DatabricksPermission -BearerToken  $ContainerToken  -Principal AZURE_Dev-DaAnalyticsCGS-Admin -PermissionLevel Read -DatabricksObjectType secretScope - cgsadmin
Set-DatabricksPermission -BearerToken  $ContainerToken  -Principal AZURE_Dev-DaAnalyticsCGS-Contribute -PermissionLevel Read -DatabricksObjectType secretScope -DatabricksObjectId cgscontribute
Set-DatabricksPermission -BearerToken  $ContainerToken  -Principal AZURE_Dev-DaAnalyticsCGS-Publish -PermissionLevel Read -DatabricksObjectType secretScope -DatabricksObjectId cgspublish
Set-DatabricksPermission -BearerToken  $ContainerToken  -Principal AZURE_Dev-DaAnalyticsCGS-Read -PermissionLevel Read -DatabricksObjectType secretScope -DatabricksObjectId cgsread

#Framework Secrets

Connect-Databricks -BearerToken $FrameworkToken -Region $Region

Add-DatabricksSecretScope -BearerToken $FrameworkToken -Region $Region -ScopeName framework

Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName ADLSName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ADLSName").SecretValueText
Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName ADLSTenantID -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ADLSTenantID").SecretValueText
Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName SQLFrameworkServerName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkServerName").SecretValueText
Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName SQLFrameworkDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName SQLFrameworkUserName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkUserName").SecretValueText
Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName SQLFrameworkPassword -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLFrameworkPassword").SecretValueText
Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName SQLDWDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWDatabaseName").SecretValueText
Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName SQLDWServerName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWServerName").SecretValueText
Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName SQLDWUserName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWUserName").SecretValueText
Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName SQLDWPassword -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLDWPassword").SecretValueText

Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName ADLSStorageAccountKey -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "ADLSStorageAccountKey").SecretValueText
Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName SQLAnalyticsDatabaseName -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "SQLAnalyticsDatabaseName").SecretValueText

Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName ADLSClientID -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "AdminADLSClientID").SecretValueText
Set-DatabricksSecret -BearerToken $FrameworkToken -ScopeName framework -SecretName ADLSCredential -SecretValue (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name "AdminADLSCredential").SecretValueText
