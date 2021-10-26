[CmdletBinding()]
Param(
  [Parameter(Mandatory=$true,Position=1)] [string] $StorageAccountName,
  [Parameter(Mandatory=$True,Position=2)] [string] $AccessKey,
  [Parameter(Mandatory=$True,Position=3)] [string] $ContainerName,
  [Parameter(Mandatory=$True,Position=4)] [string] $AdminServicePrincipalID,
  [Parameter(Mandatory=$True,Position=5)] [string] $ContributorServicePrincipalID,
  [Parameter(Mandatory=$True,Position=6)] [string] $PublisherServicePrincipalID,
  [Parameter(Mandatory=$True,Position=7)] [string] $ReaderServicePrincipalID,
  [Parameter(Mandatory=$True,Position=8)] [string] $AdminADGroupID,
  [Parameter(Mandatory=$True,Position=9)] [string] $ContributorADGroupID,
  [Parameter(Mandatory=$True,Position=10)] [string] $PublisherADGroupID,
  [Parameter(Mandatory=$True,Position=11)] [string] $ReaderADGroupID
)

# Rest documentation:
# https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update


$Query = "Query"
$QueryCurrentState = "Query/CurrentState"
$QueryEnriched = "Query/Enriched"
$QuerySandbox = "Query/Sandbox"
$Summary = "Summary"
$SummaryExport = "Summary/Export"
 
$date = [System.DateTime]::UtcNow.ToString("R") # ex: Sun, 10 Mar 2019 11:50:10 GMT
 
$n = "`n"
$method = "PATCH"
 
$stringToSignBase = "$method$n" #VERB
$stringToSignBase += "$n" # Content-Encoding + "\n" +  
$stringToSignBase += "$n" # Content-Language + "\n" +  
$stringToSignBase += "$n" # Content-Length + "\n" +  
$stringToSignBase += "$n" # Content-MD5 + "\n" +  
$stringToSignBase += "$n" # Content-Type + "\n" +  
$stringToSignBase += "$n" # Date + "\n" +  
$stringToSignBase += "$n" # If-Modified-Since + "\n" +  
$stringToSignBase += "$n" # If-Match + "\n" +  
$stringToSignBase += "$n" # If-None-Match + "\n" +  
$stringToSignBase += "$n" # If-Unmodified-Since + "\n" +  
$stringToSignBase += "$n" # Range + "\n" + 


##### Container Root Permissions

$PermissionString = "user:"+"$AdminServicePrincipalID"+":rwx,default:user:"+"$AdminServicePrincipalID"+":rwx,"+
                    "user:"+"$ContributorServicePrincipalID"+":r-x,"+
                    "user:"+"$PublisherServicePrincipalID"+":r-x,"+
                    "user:"+"$ReaderServicePrincipalID"+":r-x," +
                    "group:"+"$AdminADGroupID"+":rwx,default:group:"+"$AdminADGroupID"+":rwx,"+
                    "group:"+"$ContributorADGroupID"+":r-x,"+
                    "group:"+"$PublisherADGroupID"+":r-x,"+
                    "group:"+"$ReaderADGroupID"+":r-x"

$stringToSignACL =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedHeaders + "\n" #>
                    "x-ms-acl:$PermissionString" + $n +
                    "x-ms-date:$date" + $n + 
                    "x-ms-version:2018-11-09" + $n # 
                    <# SECTION: CanonicalizedHeaders + "\n" #>

$stringToSignFull =  "$stringToSignACL" +   
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName" + "/" + $n + 
                    "action:setAccessControl"
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
 
$headers = @{"x-ms-date"=$date}
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("x-ms-acl",$PermissionString)
 
$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + "/" + "?action=setAccessControl"
$URI
Try {
  Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
  $true
}
catch {
  $ErrorMessage = $_.Exception.Message
  $StatusDescription = $_.Exception.Response.StatusDescription
  $false
 
  Throw $ErrorMessage + " " + $StatusDescription
}

##### Query Zone Permissions

$PermissionString = "user:"+"$AdminServicePrincipalID"+":rwx,default:user:"+"$AdminServicePrincipalID"+":rwx,"+
                    "user:"+"$ContributorServicePrincipalID"+":r-x,default:user:"+"$ContributorServicePrincipalID"+":r-x,"+
                    "user:"+"$PublisherServicePrincipalID"+":r-x,default:user:"+"$PublisherServicePrincipalID"+":r-x,"+
                    "user:"+"$ReaderServicePrincipalID"+":r-x,default:user:"+"$ReaderServicePrincipalID"+":r-x,"+
                    "group:"+"$AdminADGroupID"+":rwx,"+
                    "group:"+"$ContributorADGroupID"+":r-x,"+
                    "group:"+"$PublisherADGroupID"+":r-x,"+
                    "group:"+"$ReaderADGroupID"+":r-x"

$stringToSignACL =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedHeaders + "\n" #>
                    "x-ms-acl:$PermissionString" + $n +
                    "x-ms-date:$date" + $n + 
                    "x-ms-version:2018-11-09" + $n # 
                    <# SECTION: CanonicalizedHeaders + "\n" #>

$stringToSignFull =  "$stringToSignACL" +   
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName/$Query" + $n + 
                    "action:setAccessControl"
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
 
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("x-ms-acl",$PermissionString)
 
$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + "/" + $Query + "?action=setAccessControl"
 
Try {
  Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
  $true
}
catch {
  $ErrorMessage = $_.Exception.Message
  $StatusDescription = $_.Exception.Response.StatusDescription
  $false
 
  Throw $ErrorMessage + " " + $StatusDescription
}

##### Query Zone CurrentState Permissions

$PermissionString = "user:"+"$AdminServicePrincipalID"+":rwx,default:user:"+"$AdminServicePrincipalID"+":rwx,"+
                    "user:"+"$ContributorServicePrincipalID"+":r-x,default:user:"+"$ContributorServicePrincipalID"+":r-x,"+
                    "user:"+"$PublisherServicePrincipalID"+":r-x,default:user:"+"$PublisherServicePrincipalID"+":r-x,"+
                    "user:"+"$ReaderServicePrincipalID"+":r-x,default:user:"+"$ReaderServicePrincipalID"+":r-x,"+
                    "group:"+"$AdminADGroupID"+":rwx,"+
                    "group:"+"$ContributorADGroupID"+":r-x,"+
                    "group:"+"$PublisherADGroupID"+":r-x,"+
                    "group:"+"$ReaderADGroupID"+":r-x"

$stringToSignACL =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedHeaders + "\n" #>
                    "x-ms-acl:$PermissionString" + $n +
                    "x-ms-date:$date" + $n + 
                    "x-ms-version:2018-11-09" + $n # 
                    <# SECTION: CanonicalizedHeaders + "\n" #>

$stringToSignFull =  "$stringToSignACL" +   
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName/$QueryCurrentState" + $n + 
                    "action:setAccessControl"
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
 
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("x-ms-acl",$PermissionString)
 
$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + "/" + $QueryCurrentState + "?action=setAccessControl"
 
Try {
  Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
  $true
}
catch {
  $ErrorMessage = $_.Exception.Message
  $StatusDescription = $_.Exception.Response.StatusDescription
  $false
 
  Throw $ErrorMessage + " " + $StatusDescription
}

##### Query Zone Enriched Permissions

$PermissionString = "user:"+"$AdminServicePrincipalID"+":rwx,default:user:"+"$AdminServicePrincipalID"+":rwx,"+
                    "user:"+"$ContributorServicePrincipalID"+":r-x,default:user:"+"$ContributorServicePrincipalID"+":r-x,"+
                    "user:"+"$PublisherServicePrincipalID"+":rwx,default:user:"+"$PublisherServicePrincipalID"+":rwx,"+
                    "user:"+"$ReaderServicePrincipalID"+":r-x,default:user:"+"$ReaderServicePrincipalID"+":r-x,"+
                    "group:"+"$AdminADGroupID"+":rwx,"+
                    "group:"+"$ContributorADGroupID"+":r-x,"+
                    "group:"+"$PublisherADGroupID"+":rwx,"+
                    "group:"+"$ReaderADGroupID"+":r-x"

$stringToSignACL =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedHeaders + "\n" #>
                    "x-ms-acl:$PermissionString" + $n +
                    "x-ms-date:$date" + $n + 
                    "x-ms-version:2018-11-09" + $n # 
                    <# SECTION: CanonicalizedHeaders + "\n" #>

$stringToSignFull =  "$stringToSignACL" +   
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName/$QueryEnriched" + $n + 
                    "action:setAccessControl"
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
 
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("x-ms-acl",$PermissionString)
 
$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + "/" + $QueryEnriched + "?action=setAccessControl"
 
Try {
  Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
  $true
}
catch {
  $ErrorMessage = $_.Exception.Message
  $StatusDescription = $_.Exception.Response.StatusDescription
  $false
 
  Throw $ErrorMessage + " " + $StatusDescription
}

##### Query Zone Sandbox Permissions

$PermissionString = "user:"+"$AdminServicePrincipalID"+":rwx,default:user:"+"$AdminServicePrincipalID"+":rwx,"+
                    "user:"+"$ContributorServicePrincipalID"+":rwx,default:user:"+"$ContributorServicePrincipalID"+":rwx,"+
                    "user:"+"$PublisherServicePrincipalID"+":rwx,default:user:"+"$PublisherServicePrincipalID"+":rwx,"+
                    "user:"+"$ReaderServicePrincipalID"+":r-x,default:user:"+"$ReaderServicePrincipalID"+":r-x,"+
                    "group:"+"$AdminADGroupID"+":rwx,"+
                    "group:"+"$ContributorADGroupID"+":rwx,"+
                    "group:"+"$PublisherADGroupID"+":rwx,"+
                    "group:"+"$ReaderADGroupID"+":r-x"

$stringToSignACL =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedHeaders + "\n" #>
                    "x-ms-acl:$PermissionString" + $n +
                    "x-ms-date:$date" + $n + 
                    "x-ms-version:2018-11-09" + $n # 
                    <# SECTION: CanonicalizedHeaders + "\n" #>

$stringToSignFull =  "$stringToSignACL" +   
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName/$QuerySandbox" + $n + 
                    "action:setAccessControl"
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
 
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("x-ms-acl",$PermissionString)
 
$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + "/" + $QuerySandbox + "?action=setAccessControl"
 
Try {
  Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
  $true
}
catch {
  $ErrorMessage = $_.Exception.Message
  $StatusDescription = $_.Exception.Response.StatusDescription
  $false
 
  Throw $ErrorMessage + " " + $StatusDescription
}

##### Summary Zone Permissions

$PermissionString = "user:"+"$AdminServicePrincipalID"+":rwx,default:user:"+"$AdminServicePrincipalID"+":rwx,"+
                    "user:"+"$ContributorServicePrincipalID"+":rwx,default:user:"+"$ContributorServicePrincipalID"+":rwx,"+
                    "user:"+"$PublisherServicePrincipalID"+":rwx,default:user:"+"$PublisherServicePrincipalID"+":rwx,"+
                    "user:"+"$ReaderServicePrincipalID"+":r-x,default:user:"+"$ReaderServicePrincipalID"+":r-x," +
                    "group:"+"$AdminADGroupID"+":rwx,default:group:"+"$AdminADGroupID"+":rwx,"+
                    "group:"+"$ContributorADGroupID"+":rwx,default:group:"+"$ContributorADGroupID"+":rwx,"+
                    "group:"+"$PublisherADGroupID"+":rwx,default:group:"+"$PublisherADGroupID"+":rwx,"+
                    "group:"+"$ReaderADGroupID"+":r-x,default:group:"+"$ReaderADGroupID"+":r-x"


$stringToSignACL =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedHeaders + "\n" #>
                    "x-ms-acl:$PermissionString" + $n +
                    "x-ms-date:$date" + $n + 
                    "x-ms-version:2018-11-09" + $n # 
                    <# SECTION: CanonicalizedHeaders + "\n" #>

$stringToSignFull =  "$stringToSignACL" +   
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName/$Summary" + $n + 
                    "action:setAccessControl"
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
 
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("x-ms-acl",$PermissionString)
 
$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + "/" + $Summary + "?action=setAccessControl"
 
Try {
  Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
  $true
}
catch {
  $ErrorMessage = $_.Exception.Message
  $StatusDescription = $_.Exception.Response.StatusDescription
  $false
 
  Throw $ErrorMessage + " " + $StatusDescription
}


##### Summary Zone Export Permissions

$PermissionString = "user:"+"$AdminServicePrincipalID"+":rwx,default:user:"+"$AdminServicePrincipalID"+":rwx,"+
                    "user:"+"$ContributorServicePrincipalID"+":rwx,default:user:"+"$ContributorServicePrincipalID"+":rwx,"+
                    "user:"+"$PublisherServicePrincipalID"+":rwx,default:user:"+"$PublisherServicePrincipalID"+":rwx,"+
                    "user:"+"$ReaderServicePrincipalID"+":r-x,default:user:"+"$ReaderServicePrincipalID"+":r-x," +
                    "group:"+"$AdminADGroupID"+":rwx,default:group:"+"$AdminADGroupID"+":rwx,"+
                    "group:"+"$ContributorADGroupID"+":rwx,default:group:"+"$ContributorADGroupID"+":rwx,"+
                    "group:"+"$PublisherADGroupID"+":rwx,default:group:"+"$PublisherADGroupID"+":rwx,"+
                    "group:"+"$ReaderADGroupID"+":r-x,default:group:"+"$ReaderADGroupID"+":r-x"

$stringToSignACL =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedHeaders + "\n" #>
                    "x-ms-acl:$PermissionString" + $n +
                    "x-ms-date:$date" + $n + 
                    "x-ms-version:2018-11-09" + $n # 
                    <# SECTION: CanonicalizedHeaders + "\n" #>

$stringToSignFull =  "$stringToSignACL" +   
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName/$SummaryExport" + $n + 
                    "action:setAccessControl"
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
 
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("x-ms-acl",$PermissionString)
 
$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + "/" + $SummaryExport + "?action=setAccessControl"
 
Try {
  Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
  $true
}
catch {
  $ErrorMessage = $_.Exception.Message
  $StatusDescription = $_.Exception.Response.StatusDescription
  $false
 
  Throw $ErrorMessage + " " + $StatusDescription
}


