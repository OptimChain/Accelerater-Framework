Param(
  [Parameter(Mandatory=$True,Position=3)] [string] $ContainerName
)

#Connect-AzAccount

$ApplicationPrefix = "da-tst-"
$GroupPrefix = "AzureAD-da-tst-"
$VaultName = 'da-dev-jason-analytics'
$rgName = 'da-dev-wus2-analytics-rg'
$SQLAnalyticsDatabaseName = 'da-tst-wus2-analytics-db'

$OutputFile = 'C:\Users\Jason.Bian\Desktop\Solution\Powershell\ARM deployments\DatabricksSecretsContainer_' + $containerName + '.txt'

$ADLSGen2StorageAccountName = (Get-AzKeyVaultSecret -vaultName $VaultName -name "ADLSName").SecretValueText
$ADLSTenantId = (Get-AzKeyVaultSecret -vaultName $VaultName -name "ADLSTenantID").SecretValueText
$SQLFrameworkServerName = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLFrameworkServerName").SecretValueText
$SQLFrameworkDatabaseName = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLFrameworkDatabaseName").SecretValueText
$SQLFrameworkUserName = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLFrameworkUserName").SecretValueText
$SQLFrameworkPassword = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLFrameworkPassword").SecretValueText
$SQLDWDatabaseName = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLDWDatabaseName").SecretValueText
$SQLDWUserName = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLDWUserName").SecretValueText
$SQLDWPassword = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLDWPassword").SecretValueText

$SQLDWServerName = $SQLFrameworkServerName

################ CREATE APPLICATION REGISTRATIONS

$start = Get-Date
$end = $start.AddYears(200)

# Applications (Service Principals)

$AdminDisplayName = $ApplicationPrefix + $ContainerName + '-Admin'
$AdminServicePrincipal = Get-AZADApplication -DisplayName $AdminDisplayName
If ($AdminServicePrincipal)
{Write-Output $AdminDisplayName " Service Principal Exists"}
Else
{
$NewAdminServicePrincipal = New-AzADServicePrincipal -DisplayName $AdminDisplayName
$AdminServicePrincipal = Get-AZADApplication -DisplayName $AdminDisplayName
$AdminPassword = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes((New-Guid)))
$SecureAdminPassword = convertto-securestring $AdminPassword -asplaintext -force
$AdminCredential = New-AzADAppCredential -ObjectId $AdminServicePrincipal.ObjectId -Password $SecureAdminPassword -StartDate $start -EndDate $end

Write-Output "Registered Application Display Name: $($NewAdminServicePrincipal.DisplayName) ClientId: $($AdminServicePrincipal.ApplicationId) Credential: $($AdminPassword)"
}

$ContributorDisplayName = $ApplicationPrefix + $ContainerName + '-Contribute'
$ContributorServicePrincipal = Get-AZADApplication -DisplayName $ContributorDisplayName
If ($ContributorServicePrincipal)
{Write-Output $ContributorDisplayName " Service Principal Exists"}
Else
{
$NewContributorServicePrincipal = New-AzADServicePrincipal -DisplayName $ContributorDisplayName
$ContributorServicePrincipal = Get-AZADApplication -DisplayName $ContributorDisplayName
$ContributorPassword = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes((New-Guid)))
$SecureContributorPassword = convertto-securestring $ContributorPassword -asplaintext -force
$ContributorCredential = New-AzADAppCredential -ObjectId $ContributorServicePrincipal.ObjectId -Password $SecureContributorPassword -StartDate $start -EndDate $end

Write-Output "Registered Application Display Name: $($NewContributorServicePrincipal.DisplayName) ClientId: $($ContributorServicePrincipal.ApplicationId) Credential: $($ContributorPassword)"
}

$PublisherDisplayName = $ApplicationPrefix + $ContainerName + '-Publish'
$PublisherServicePrincipal = Get-AZADApplication -DisplayName $PublisherDisplayName
If ($PublisherServicePrincipal)
{Write-Output $PublisherDisplayName " Service Principal Exists"}
Else
{
$NewPublisherServicePrincipal = New-AzADServicePrincipal -DisplayName $PublisherDisplayName
$PublisherServicePrincipal = Get-AZADApplication -DisplayName $PublisherDisplayName
$PublisherPassword = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes((New-Guid)))
$SecurePublisherPassword = convertto-securestring $PublisherPassword -asplaintext -force
$PublisherCredential = New-AzADAppCredential -ObjectId $PublisherServicePrincipal.ObjectId -Password $SecurePublisherPassword -StartDate $start -EndDate $end

Write-Output "Registered Application Display Name: $($NewPublisherServicePrincipal.DisplayName) ClientId: $($PublisherServicePrincipal.ApplicationId) Credential: $($PublisherPassword)"
}

$ReaderDisplayName = $ApplicationPrefix + $ContainerName + '-Read'
$ReaderServicePrincipal = Get-AZADApplication -DisplayName $ReaderDisplayName
If ($ReaderServicePrincipal)
{Write-Output $ReaderDisplayName " Service Principal Exists"}
Else
{
$NewReaderServicePrincipal = New-AzADServicePrincipal -DisplayName $ReaderDisplayName
$ReaderServicePrincipal = Get-AZADApplication -DisplayName $ReaderDisplayName
$ReaderPassword = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes((New-Guid)))
$SecureReaderPassword = convertto-securestring $ReaderPassword -asplaintext -force
$ReaderCredential = New-AzADAppCredential -ObjectId $ReaderServicePrincipal.ObjectId -Password $SecureReaderPassword -StartDate $start -EndDate $end

Write-Output "Registered Application Display Name: $($NewReaderServicePrincipal.DisplayName) ClientId: $($ReaderServicePrincipal.ApplicationId) Credential: $($ReaderPassword)"
}

################ CREATE DATABRICKS SECRET SCOPES SCRIPT

$DatabricksSecretsPrefix = 'databricks secrets '

$Header = 'REM Databricks Secrets for container ' + $ContainerName
$Header > $OutputFile
$Header2 = 'REM source databrickscli/bin/activate'
$Header2 | Out-file $OutputFile -Append

$Header2 = 'REM databricks configure --profile ' + $ContainerName + 'profile --token  https://northcentralus.azuredatabricks.net ' + $DatabricksToken
$Header2 | Out-file $OutputFile -Append
' ' | Out-file $OutputFile -Append

$Groups = "admin","contribute","publish","read"

#Create Group Scopes

foreach ($GroupName in $Groups) {

#Databricks secrets create-scope --scope client000000001admin --profile client000000001profile
$DatabricksCommand =  $DatabricksSecretsPrefix + 'create-scope --scope ' + $ContainerName + $GroupName + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand

$DatabricksCommand | Out-file $OutputFile -Append

}

' ' | Out-file $OutputFile -Append

#Create Common Secrets for Admin, Contribute and Publish

$Groups = "admin","contribute","publish"

foreach ($GroupName in $Groups) {

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + $GroupName + ' --key ADLSGen2StorageAccountName --string-value '  + $ADLSGen2StorageAccountName + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + $GroupName + ' --key ADLSTenantID --string-value '  + $ADLSTenantID + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + $GroupName + ' --key SQLFrameworkServerName --string-value '  + $SQLFrameworkServerName + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + $GroupName + ' --key SQLFrameworkDatabaseName --string-value '  + $SQLFrameworkDatabaseName + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + $GroupName + ' --key SQLFrameworkUserName --string-value '  + $SQLFrameworkUserName + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + $GroupName + ' --key SQLFrameworkPassword --string-value '  + $SQLFrameworkPassword + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + $GroupName + ' --key SQLAnalyticsDatabaseName --string-value '  + $SQLAnalyticsDatabaseName + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + $GroupName + ' --key SQLDWDatabaseName --string-value '  + $SQLDWDatabaseName + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + $GroupName + ' --key SQLDWServerName --string-value '  + $SQLDWServerName + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + $GroupName + ' --key SQLDWUserName --string-value '  + $SQLDWUserName + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + $GroupName + ' --key SQLDWPassword --string-value '  + $SQLDWPassword + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

' ' | Out-file $OutputFile -Append

}

#Create Client Secrets for Admin, Contribute, Publish, and Read

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + 'admin --key ADLSClientID --string-value ' + $($AdminServicePrincipal.ApplicationId) + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + 'admin --key ADLSCredential --string-value '  + $($AdminPassword) + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + 'Contribute --key ADLSClientID --string-value ' + $($ContributorServicePrincipal.ApplicationId) + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + 'contribute --key ADLSCredential --string-value '  + $($ContributorPassword) + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + 'publish --key ADLSClientID --string-value ' + $($PublisherServicePrincipal.ApplicationId) + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + 'publish --key ADLSCredential --string-value '  + $($PublisherPassword) + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + 'read --key ADLSClientID --string-value ' + $($ReaderServicePrincipal.ApplicationId) + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put --scope ' + $ContainerName + 'read --key ADLSCredential --string-value '  + $($ReaderPassword) + ' --profile ' + $ContainerName + 'profile'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

#Create ACLs for Secret Scopes 

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put-acl --scope ' + $ContainerName + 'admin --principal ' + $($AdminServicePrincipal.DisplayName) + ' --permission READ'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put-acl --scope ' + $ContainerName + 'contribute --principal ' + $($ContributorServicePrincipal.DisplayName) + ' --permission READ'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put-acl --scope ' + $ContainerName + 'publish --principal ' + $($PublisherServicePrincipal.DisplayName) + ' --permission READ'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append

$DatabricksCommand =  $DatabricksSecretsPrefix + 'put-acl --scope ' + $ContainerName + 'read --principal ' + $($ReaderServicePrincipal.DisplayName) + ' --permission READ'
Write-Output $DatabricksCommand
$DatabricksCommand | Out-file $OutputFile -Append