﻿$container = "crm"
$containeradmin = $container+"admin"
$containercontribute = $container+"contribute"
$containerpublish = $container+"publish"
$containerread = $container+"read"
$profile = $container+"profile"

#Container Secrets

$SubscriptionId = "d1837fe2-2596-4d4b-9242-fdea4caf40ea"
Select-AzSubscription -SubscriptionId $SubscriptionId


$KeyVaultName = "rsm-entdata-dev"
$Region = "CentralUS"

#write-output "databricks secrets create-scope --scope $containeradmin --profile $profile"
#write-output "databricks secrets create-scope --scope $containercontribute --profile $profile"
#write-output "databricks secrets create-scope --scope $containerpublish --profile $profile"
#write-output "databricks secrets create-scope --scope $containerread --profile $profile"



$secrets = @('ADLSGen2StorageAccountName',
                'SQLFrameworkDatabaseName',
                'SQLFrameworkUserName',
                'SQLFrameworkPassword',
                'SQLFrameworkServerName',
                'SQLDWDatabaseName',
                'SQLDWServerName',
                'SQLDWUserName',
                'SQLDWPassword',
                'PurviewAccountName',
                'ADLSTenantId'
                )

foreach ($item in $secrets){
    #write-output $item
    if ($item -eq "ADLSGen2StorageAccountName") {
        $item1 = "ADLSName"
        }
    elseif ($item -eq "ADLSClientId") {
        $item1 = $container.ToUpper()+"AdminADLSClientId"
        }
    elseif ($item -eq "ADLSCredential"){
         $item1 = $container.ToUpper()+"AdminADLSCredential"
    }
    else{
        $item1 = $item
    }

  $secretvalue = (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name $item1 -AsPlainText)
  write-output "databricks secrets put --scope $containeradmin --key $item --string-value ""$secretvalue"""
  write-output "databricks secrets put --scope $containercontribute --key $item --string-value ""$secretvalue"""
  write-output "databricks secrets put --scope $containerpublish --key $item --string-value ""$secretvalue"""
  write-output "databricks secrets put --scope $containerread --key $item --string-value ""$secretvalue"""
   
}
## Admin
  $item = $container.ToUpper()+"AdminADLSClientId"
  $secretvalue = (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name $item -AsPlainText)
  write-output "databricks secrets put --scope $containeradmin --key ADLSClientId --string-value ""$secretvalue"""

  $item = $container.ToUpper()+"AdminADLSCredential"
  $secretvalue = (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name $item -AsPlainText)
  write-output "databricks secrets put --scope $containeradmin --key ADLSCredential --string-value ""$secretvalue"""

  ## Contribute
  $item = $container.ToUpper()+"ContributeADLSClientId"
  $secretvalue = (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name $item -AsPlainText)
  write-output "databricks secrets put --scope $containercontribute --key ADLSClientId --string-value ""$secretvalue"""
  
  $item = $container.ToUpper()+"ContributeADLSCredential"
  $secretvalue = (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name $item -AsPlainText)
  write-output "databricks secrets put --scope $containercontribute --key ADLSCredential --string-value ""$secretvalue"""  


  ## Publish
  $item = $container.ToUpper()+"PublishADLSClientId"
  $secretvalue = (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name $item -AsPlainText)
  write-output "databricks secrets put --scope $containerpublish --key ADLSClientId --string-value ""$secretvalue"""

  $item = $container.ToUpper()+"PublishADLSCredential"
  $secretvalue = (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name $item -AsPlainText)
  write-output "databricks secrets put --scope $containerpublish --key ADLSCredential --string-value ""$secretvalue"""
  
  ## Read
  $item = $container.ToUpper()+"ReadADLSClientId"
  $secretvalue = (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name $item -AsPlainText)
  write-output "databricks secrets put --scope $containerread --key ADLSClientId --string-value ""$secretvalue"""

  $item = $container.ToUpper()+"ReadADLSCredential"
  $secretvalue = (Get-AzKeyVaultSecret -VaultName $KeyVaultName  -Name $item -AsPlainText)
  write-output "databricks secrets put --scope $containerread --key ADLSCredential --string-value ""$secretvalue"""