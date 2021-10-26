

Set-AzContext -Subscriptionid "57b50573-d245-486d-b3b3-7a5e83dacbb8"

$VaultName = 'da-dev-jason-analytics'
$ContainerName  = 'cilent002'

$StorageAccountName = Get-AzKeyVaultSecret -vaultName $VaultName -name "ADLSName" -AsPlainText
$AccessKey = Get-AzKeyVaultSecret -vaultName $VaultName -name "ADLSStorageAccountKey" -AsPlainText

# Rest documentation:
# https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create
 
$ContainerName = $ContainerName.ToLower() 
$BadRecords = "/BadRecords"
$Landing = "/Landing"
$Raw = "/Raw"
$Query = "/Query"
$QueryCurrentState = "/Query/CurrentState"
$QueryEnriched = "/Query/Enriched"
$QuerySandbox = "/Query/Sandbox"
$Schemas = "/Schemas"
$Summary = "/Summary"
$SummaryExport = "/Summary/Export"
 
$date = [System.DateTime]::UtcNow.ToString("R") # ex: Sun, 10 Mar 2019 11:50:10 GMT
 
$n = "`n"
$method = "PUT"

############### Create Blob Container

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
$stringToSignBase +=    
                    <# SECTION: CanonicalizedHeaders + "\n" #>
                    "x-ms-date:$date" + $n + 
                    "x-ms-version:2018-11-09" + $n # 
                    <# SECTION: CanonicalizedHeaders + "\n" #>
 
$stringToSignFull =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName" + $n + 
                    "resource:filesystem"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
 
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)

 
$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + "?resource=filesystem"
 
Try {
    Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
}
catch {
    $ErrorMessage = $_.Exception.Message
    $StatusDescription = $_.Exception.Response.StatusDescription
    $false
 
    Throw $ErrorMessage + " " + $StatusDescription
}

############### Create Container Folders

$stringToSignBase = "$method$n" #VERB
$stringToSignBase += "$n" # Content-Encoding + "\n" +  
$stringToSignBase += "$n" # Content-Language + "\n" +  
$stringToSignBase += "$n" # Content-Length + "\n" +  
$stringToSignBase += "$n" # Content-MD5 + "\n" +  
$stringToSignBase += "$n" # Content-Type + "\n" +  
$stringToSignBase += "$n" # Date + "\n" +  
$stringToSignBase += "$n" # If-Modified-Since + "\n" +  
$stringToSignBase += "$n" # If-Match + "\n" +  
$stringToSignBase += "*" + "$n" # If-None-Match + "\n" +  
$stringToSignBase += "$n" # If-Unmodified-Since + "\n" +  
$stringToSignBase += "$n" # Range + "\n" + 
$stringToSignBase +=    
                    <# SECTION: CanonicalizedHeaders + "\n" #>
                    "x-ms-date:$date" + $n + 
                    "x-ms-version:2018-11-09" + $n # 
                    <# SECTION: CanonicalizedHeaders + "\n" #>
                     
$stringToSignFull =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName" + $BadRecords + $n + 
                    "resource:directory"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
 
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("If-None-Match","*") # To fail if the destination already exists, use a conditional request with If-None-Match: "*"
 
$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + $BadRecords + "?resource=directory"
 
try {
    Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
    $true
}
catch {
    $ErrorMessage = $_.Exception.Message
    $StatusDescription = $_.Exception.Response.StatusDescription
    $false
 
    Throw $ErrorMessage + " " + $StatusDescription
}

                    
$stringToSignFull =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName" + $Landing + $n + 
                    "resource:directory"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("If-None-Match","*") # To fail if the destination already exists, use a conditional request with If-None-Match: "*"

$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + $Landing + "?resource=directory"
 
try {
    Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
    $true
}
catch {
    $ErrorMessage = $_.Exception.Message
    $StatusDescription = $_.Exception.Response.StatusDescription
    $false
 
    Throw $ErrorMessage + " " + $StatusDescription
}


$stringToSignFull =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName" + $Raw + $n + 
                    "resource:directory"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("If-None-Match","*") # To fail if the destination already exists, use a conditional request with If-None-Match: "*"

$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + $Raw + "?resource=directory"
 
try {
    Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
    $true
}
catch {
    $ErrorMessage = $_.Exception.Message
    $StatusDescription = $_.Exception.Response.StatusDescription
    $false
 
    Throw $ErrorMessage + " " + $StatusDescription
}

$stringToSignFull =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName" + $Query + $n + 
                    "resource:directory"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("If-None-Match","*") # To fail if the destination already exists, use a conditional request with If-None-Match: "*"

$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + $Query + "?resource=directory"
 
try {
    Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
    $true
}
catch {
    $ErrorMessage = $_.Exception.Message
    $StatusDescription = $_.Exception.Response.StatusDescription
    $false
 
    Throw $ErrorMessage + " " + $StatusDescription
}

$stringToSignFull =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName" + $QueryCurrentState + $n + 
                    "resource:directory"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("If-None-Match","*") # To fail if the destination already exists, use a conditional request with If-None-Match: "*"

$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + $QueryCurrentState + "?resource=directory"
 
try {
    Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
    $true
}
catch {
    $ErrorMessage = $_.Exception.Message
    $StatusDescription = $_.Exception.Response.StatusDescription
    $false
 
    Throw $ErrorMessage + " " + $StatusDescription
}

$stringToSignFull =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName" + $QueryEnriched + $n + 
                    "resource:directory"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("If-None-Match","*") # To fail if the destination already exists, use a conditional request with If-None-Match: "*"

$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + $QueryEnriched + "?resource=directory"
 
try {
    Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
    $true
}
catch {
    $ErrorMessage = $_.Exception.Message
    $StatusDescription = $_.Exception.Response.StatusDescription
    $false
 
    Throw $ErrorMessage + " " + $StatusDescription
}

$stringToSignFull =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName" + $QuerySandbox + $n + 
                    "resource:directory"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("If-None-Match","*") # To fail if the destination already exists, use a conditional request with If-None-Match: "*"

$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + $QuerySandbox + "?resource=directory"
 
try {
    Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
    $true
}
catch {
    $ErrorMessage = $_.Exception.Message
    $StatusDescription = $_.Exception.Response.StatusDescription
    $false
 
    Throw $ErrorMessage + " " + $StatusDescription
}

$stringToSignFull =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName" + $Schemas + $n + 
                    "resource:directory"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("If-None-Match","*") # To fail if the destination already exists, use a conditional request with If-None-Match: "*"

$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + $Schemas + "?resource=directory"
 
try {
    Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
    $true
}
catch {
    $ErrorMessage = $_.Exception.Message
    $StatusDescription = $_.Exception.Response.StatusDescription
    $false
 
    Throw $ErrorMessage + " " + $StatusDescription
}

 
$stringToSignFull =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName" + $Summary + $n + 
                    "resource:directory"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("If-None-Match","*") # To fail if the destination already exists, use a conditional request with If-None-Match: "*"

$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + $Summary + "?resource=directory"
 
try {
    Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
    $true
}
catch {
    $ErrorMessage = $_.Exception.Message
    $StatusDescription = $_.Exception.Response.StatusDescription
    $false
 
    Throw $ErrorMessage + " " + $StatusDescription
}

$stringToSignFull =  "$stringToSignBase" + 
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$ContainerName" + $SummaryExport + $n + 
                    "resource:directory"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSignFull)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
$headers.Add("If-None-Match","*") # To fail if the destination already exists, use a conditional request with If-None-Match: "*"

$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $ContainerName + $SummaryExport + "?resource=directory"
 
try {
    Invoke-RestMethod -method $method -Uri $URI -Headers $headers # returns empty response
    $true
}
catch {
    $ErrorMessage = $_.Exception.Message
    $StatusDescription = $_.Exception.Response.StatusDescription
    $false
 
    Throw $ErrorMessage + " " + $StatusDescription
}