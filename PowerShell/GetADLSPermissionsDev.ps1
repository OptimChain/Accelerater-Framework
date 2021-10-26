[CmdletBinding()]
Param( 
  [Parameter(Mandatory=$True,Position=1)] [string] $ContainerName
)

$VaultName = 'da-dev-wus2-analytics-kv'

$StorageAccountName = (Get-AzKeyVaultSecret -vaultName $VaultName -name "ADLSName").SecretValueText
$AccessKey = (Get-AzKeyVaultSecret -vaultName $VaultName -name "ADLSStorageAccountKey").SecretValueText

& ((Split-Path $MyInvocation.InvocationName) + "\GetADLSPermissions.ps1") -StorageAccountName $StorageAccountName -AccessKey $Accesskey -ContainerName $ContainerName
