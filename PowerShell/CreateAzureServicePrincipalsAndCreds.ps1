Param(
  [Parameter(Mandatory=$True,Position=3)] [string] $ContainerName
)

#Connect-AzAccount

$ApplicationPrefix = "da-prd-"
$GroupPrefix = "Azure-da-prd-"
$VaultName = 'da-prd-wus2-analytics-kv'
$rgName = 'da-prd-wus2-analytics-rg'
$SQLAnalyticsDatabaseName = 'da-prd-wus2-analytics-db'
$SQLDWDatabaseName = 'da-prd-wus2-analytics-dw'

$ADLSGen2StorageAccountName = (Get-AzKeyVaultSecret -vaultName $VaultName -name "ADLSName").SecretValueText
$ADLSTenantId = (Get-AzKeyVaultSecret -vaultName $VaultName -name "ADLSTenantID").SecretValueText
$SQLFrameworkServerName = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLFrameworkServerName").SecretValueText
$SQLFrameworkDatabaseName = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLFrameworkDatabaseName").SecretValueText
$SQLFrameworkUser = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLFrameworkUserName").SecretValueText
$SQLFrameworkPassword = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLFrameworkPassword").SecretValueText


$SQLDWServerName = $SQLFrameworkServerName
$SQLDWUserName = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLDWUserName").SecretValueText
$SQLDWPassword = (Get-AzKeyVaultSecret -vaultName $VaultName -name "SQLDWPassword").SecretValueText

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

Write-Output "Display Name: $($NewAdminServicePrincipal.DisplayName) ClientId: $($AdminServicePrincipal.ApplicationId) Credential: $($AdminPassword)"
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

Write-Output "Display Name: $($NewContributorServicePrincipal.DisplayName) ClientId: $($ContributorServicePrincipal.ApplicationId) Credential: $($ContributorPassword)"
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

Write-Output "Display Name: $($NewPublisherServicePrincipal.DisplayName) ClientId: $($PublisherServicePrincipal.ApplicationId) Credential: $($PublisherPassword)"
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

Write-Output "Display Name: $($NewReaderServicePrincipal.DisplayName) ClientId: $($ReaderServicePrincipal.ApplicationId) Credential: $($ReaderPassword)"
}
