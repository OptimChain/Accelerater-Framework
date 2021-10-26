$ContainerName = ""


#Connect-AzAccount
Set-AzContext -Subscriptionid "a7b20ab8-339a-4847-ba41-01c16c01ecb5"
$VaultName = 'rsm-entdata'

$StorageAccountName = Get-AzKeyVaultSecret -vaultName $VaultName -name "ADLSName" -AsPlainText
$AccessKey = Get-AzKeyVaultSecret -vaultName $VaultName -name "ADLSStorageAccountKey" -AsPlainText




$ApplicationPrefix = "RSMAppEntData"
$GroupPrefix = "AzureAD-entdata"
$EnvironmentSuffix1 = ""
$EnvironmentSuffix2 = ""

# Applications (Service Principals)

$AdminDisplayName = $ApplicationPrefix + $ContainerName + '_Admin' + $EnvironmentSuffix1
$AdminServicePrincipal = Get-AZADApplication -DisplayName $AdminDisplayName

$ContributorDisplayName = $ApplicationPrefix + $ContainerName + '_Contribute' + $EnvironmentSuffix1
$ContributorServicePrincipal = Get-AZADApplication -DisplayName $ContributorDisplayName

$PublisherDisplayName = $ApplicationPrefix + $ContainerName + '_Publish' + $EnvironmentSuffix1
$PublisherServicePrincipal = Get-AZADApplication -DisplayName $PublisherDisplayName

$ReaderDisplayName = $ApplicationPrefix + $ContainerName + '_Read' + $EnvironmentSuffix1
$ReaderServicePrincipal = Get-AZADApplication -DisplayName $ReaderDisplayName


$AdminServicePrincipalId = (Get-AzADServicePrincipal -DisplayName $AdminDisplayName).Id
$ContributorServicePrincipalId = (Get-AzADServicePrincipal -DisplayName $ContributorDisplayName).Id
$PublisherServicePrincipalId = (Get-AzADServicePrincipal -DisplayName $PublisherDisplayName).Id
$ReaderServicePrincipalId = (Get-AzADServicePrincipal -DisplayName $ReaderDisplayName).Id


# Azure AD Groups
$ContainerName = "rsm"

$AdminDisplayName = $GroupPrefix + '-Admin' + $EnvironmentSuffix2
$AdminADGroup = Get-AZADGroup -DisplayName $AdminDisplayName

$ContributorDisplayName = $GroupPrefix  + '-Contribute' + $EnvironmentSuffix2
$ContributorADGroup = Get-AZADGroup -DisplayName $ContributorDisplayName

$PublisherDisplayName = $GroupPrefix  + '-Publish' + $EnvironmentSuffix2
$PublisherADGroup = Get-AZADGroup -DisplayName $PublisherDisplayName

$ReaderDisplayName = $GroupPrefix  + '-Read' + $EnvironmentSuffix2
$ReaderADGroup = Get-AZADGroup -DisplayName $ReaderDisplayName




& ((Split-Path $MyInvocation.InvocationName) + "\SetADLSPermissionsContainer.ps1") -StorageAccountName $StorageAccountName -AccessKey $Accesskey -ContainerName $ContainerName -AdminServicePrincipalID $AdminServicePrincipalId -ContributorServicePrincipalID $ContributorServicePrincipalId -PublisherServicePrincipalID $PublisherServicePrincipalId -ReaderServicePrincipalID $ReaderServicePrincipalId -AdminADGroupID $AdminADGroup.Id -ContributorADGroupID $ContributorADGroup.Id -PublisherADGroupID $PublisherADGroup.Id -ReaderADGroupID $ReaderADGroup.Id

#& ((Split-Path $MyInvocation.InvocationName) + "\SetADLSPermissionsContainer.ps1") -StorageAccountName $StorageAccountName -AccessKey $Accesskey -ContainerName $ContainerName -AdminServicePrincipalID 67186104-503d-43a3-ac2a-a21b31b101cd -ContributorServicePrincipalID b826e9f6-96c3-4fb8-8af0-66526bed15e7 -PublisherServicePrincipalID 6a0dc06d-7495-40b4-82a7-11066c18e0de -ReaderServicePrincipalID e7ef10ba-0978-405f-98c9-815e90d0655d -AdminADGroupID $AdminADGroup.Id -ContributorADGroupID $ContributorADGroup.Id -PublisherADGroupID $PublisherADGroup.Id -ReaderADGroupID $ReaderADGroup.Id

& ((Split-Path $MyInvocation.InvocationName) + "\GetADLSPermissions.ps1") -StorageAccountName $StorageAccountName -AccessKey $Accesskey -ContainerName $ContainerName

