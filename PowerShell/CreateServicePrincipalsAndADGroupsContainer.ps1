Param(
  [Parameter(Mandatory=$True,Position=3)] [string] $ContainerName
)

#Connect-AzAccount
Set-AzContext -Subscriptionid "57b50573-d245-486d-b3b3-7a5e83dacbb8"

$ApplicationPrefix = "da-tst-"
$GroupPrefix = "AzureAD-da-tst-"

<#
# Applications (Service Principals)

$AdminDisplayName = $ApplicationPrefix + $ContainerName + '-Admin'
$AdminServicePrincipal = Get-AZADApplication -DisplayName $AdminDisplayName
If ($AdminServicePrincipal)
{Write-Output $AdminDisplayName " Service Principal Exists"}
Else
{New-AzADServicePrincipal -DisplayName $AdminDisplayName}

$ContributorDisplayName = $ApplicationPrefix + $ContainerName + '-Contribute'
$ContributorServicePrincipal = Get-AZADApplication -DisplayName $ContributorDisplayName
If ($ContributorServicePrincipal)
{Write-Output $ContributorDisplayName " Service Principal Exists"}
Else
{New-AzADServicePrincipal -DisplayName $ContributorDisplayName}

$PublisherDisplayName = $ApplicationPrefix + $ContainerName + '-Publish'
$PublisherServicePrincipal = Get-AZADApplication -DisplayName $PublisherDisplayName
If ($PublisherServicePrincipal)
{Write-Output $PublisherDisplayName " Service Principal Exists"}
Else
{New-AzADServicePrincipal -DisplayName $PublisherDisplayName}

$ReaderDisplayName = $ApplicationPrefix + $ContainerName + '-Read'
$ReaderServicePrincipal = Get-AZADApplication -DisplayName $ReaderDisplayName
If ($ReaderServicePrincipal)
{Write-Output $ReaderDisplayName " Service Principal Exists"}
Else
{New-AzADServicePrincipal -DisplayName $ReaderDisplayName}
#>

# Azure AD Groups

$AdminDisplayName = $GroupPrefix + $ContainerName + '-Admin'
$AdminADGroup = Get-AZADGroup -DisplayName $AdminDisplayName
If ($AdminADGroup)
{Write-Output $AdminDisplayName " AD Group Exists"}
Else
{New-AzADGroup -DisplayName $AdminDisplayName -MailNickname $AdminDisplayName}

$ContributorDisplayName = $GroupPrefix + $ContainerName + '-Contribute'
$ContributorADGroup = Get-AZADGroup -DisplayName $ContributorDisplayName
If ($ContributorADGroup)
{Write-Output $ContributorDisplayName " AD Group Exists"}
Else
{New-AzADGroup -DisplayName $ContributorDisplayName -MailNickname $ContributorDisplayName}

$PublisherDisplayName = $GroupPrefix + $ContainerName + '-Publish'
$PublisherADGroup = Get-AZADGroup -DisplayName $PublisherDisplayName
If ($PublisherADGroup)
{Write-Output $PublisherDisplayName " AD Group Exists"}
Else
{New-AzADGroup -DisplayName $PublisherDisplayName -MailNickname $PublisherDisplayName}

$ReaderDisplayName = $GroupPrefix + $ContainerName + '-Read'
$ReaderADGroup = Get-AZADGroup -DisplayName $ReaderDisplayName
If ($ReaderADGroup)
{Write-Output $ReaderDisplayName " AD Group Exists"}
Else
{New-AzADGroup -DisplayName $ReaderDisplayName -MailNickname $ReaderDisplayName}
