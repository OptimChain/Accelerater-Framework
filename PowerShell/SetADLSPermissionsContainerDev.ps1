$ContainerName = "cilent002"


#Connect-AzAccount
Set-AzContext -Subscriptionid "57b50573-d245-486d-b3b3-7a5e83dacbb8"
$VaultName = 'da-dev-jason-analytics'

$StorageAccountName = Get-AzKeyVaultSecret -vaultName $VaultName -name "ADLSName" -AsPlainText
$AccessKey = Get-AzKeyVaultSecret -vaultName $VaultName -name "ADLSStorageAccountKey" -AsPlainText

$ApplicationPrefix = "da-tst-"
$GroupPrefix = "AzureAD-da-tst-"
$EnvironmentSuffix1 = ""
$EnvironmentSuffix2 = ""

# Applications (Service Principals)

$AdminDisplayName = $ApplicationPrefix + $ContainerName + '-Admin' + $EnvironmentSuffix1
$AdminServicePrincipal = Get-AZADApplication -DisplayName $AdminDisplayName

$ContributorDisplayName = $ApplicationPrefix + $ContainerName + '-Contribute' + $EnvironmentSuffix1
$ContributorServicePrincipal = Get-AZADApplication -DisplayName $ContributorDisplayName

$PublisherDisplayName = $ApplicationPrefix + $ContainerName + '-Publish' + $EnvironmentSuffix1
$PublisherServicePrincipal = Get-AZADApplication -DisplayName $PublisherDisplayName

$ReaderDisplayName = $ApplicationPrefix + $ContainerName + '-Read' + $EnvironmentSuffix1
$ReaderServicePrincipal = Get-AZADApplication -DisplayName $ReaderDisplayName

# Azure AD Groups

$AdminDisplayName = $GroupPrefix + $ContainerName + '-Admin' + $EnvironmentSuffix2
$AdminADGroup = Get-AZADGroup -DisplayName $AdminDisplayName

$ContributorDisplayName = $GroupPrefix + $ContainerName + '-Contribute' + $EnvironmentSuffix2
$ContributorADGroup = Get-AZADGroup -DisplayName $ContributorDisplayName

$PublisherDisplayName = $GroupPrefix + $ContainerName + '-Publish' + $EnvironmentSuffix2
$PublisherADGroup = Get-AZADGroup -DisplayName $PublisherDisplayName

$ReaderDisplayName = $GroupPrefix + $ContainerName + '-Read' + $EnvironmentSuffix2
$ReaderADGroup = Get-AZADGroup -DisplayName $ReaderDisplayName

Write-Host $AdminDisplayName
Write-Host $ContributorDisplayName
Write-Host $PublisherDisplayName
Write-Host $ReaderDisplayName

Write-Host $AdminServicePrincipal.ApplicationId
Write-Host $ContributorServicePrincipal.ApplicationId
Write-Host $PublisherServicePrincipal.ApplicationId
Write-Host $ReaderServicePrincipal.ApplicationId


& ((Split-Path $MyInvocation.InvocationName) + "\SetADLSPermissionsContainer.ps1") -StorageAccountName $StorageAccountName -AccessKey $Accesskey -ContainerName $ContainerName -AdminServicePrincipalID $AdminServicePrincipal.ApplicationId -ContributorServicePrincipalID $ContributorServicePrincipal.ApplicationId -PublisherServicePrincipalID $PublisherServicePrincipal.ApplicationId -ReaderServicePrincipalID $ReaderServicePrincipal.ApplicationId -AdminADGroupID $AdminADGroup.Id -ContributorADGroupID $ContributorADGroup.Id -PublisherADGroupID $PublisherADGroup.Id -ReaderADGroupID $ReaderADGroup.Id

& ((Split-Path $MyInvocation.InvocationName) + "\GetADLSPermissions.ps1") -StorageAccountName $StorageAccountName -AccessKey $Accesskey -ContainerName $ContainerName



