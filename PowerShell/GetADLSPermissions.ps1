[CmdletBinding()]
Param( 
  [Parameter(Mandatory=$True,Position=1)] [string] $ContainerName,
  [Parameter(Mandatory=$True,Position=2)] [string] $StorageAccountName,
  [Parameter(Mandatory=$True,Position=3)] [string] $AccessKey
)


$FilesystemName = $ContainerName

$Folders = "$ContainerName","Query","Query/CurrentState","Query/Enriched","Query/Sandbox","Summary","Summary/Export"


$tabName = "ACLTable"

#Create Table object
$table = New-Object system.Data.DataTable “$tabName”

#Define Columns
$Folder = New-Object system.Data.DataColumn Folder,([string])
$User = New-Object system.Data.DataColumn User,([string])
$AppId = New-Object system.Data.DataColumn AppId,([string])
$Type = New-Object system.Data.DataColumn Type,([string])
$ACL = New-Object system.Data.DataColumn ACL,([string])

#Add the Columns
$table.columns.add($Folder)
$table.columns.add($User)
$table.columns.add($AppId)
$table.columns.add($Type)
$table.columns.add($ACL)

foreach ($FolderName in $Folders) {
$Path = $FolderName

# Rest documentation:
# https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/getproperties
 
$date = [System.DateTime]::UtcNow.ToString("R") # ex: Sun, 10 Mar 2019 11:50:10 GMT
 
$n = "`n"
$method = "HEAD"
 
$stringToSign = "$method$n" #VERB
$stringToSign += "$n" # Content-Encoding + "\n" +  
$stringToSign += "$n" # Content-Language + "\n" +  
$stringToSign += "$n" # Content-Length + "\n" +  
$stringToSign += "$n" # Content-MD5 + "\n" +  
$stringToSign += "$n" # Content-Type + "\n" +  
$stringToSign += "$n" # Date + "\n" +  
$stringToSign += "$n" # If-Modified-Since + "\n" +  
$stringToSign += "$n" # If-Match + "\n" +  
$stringToSign += "$n" # If-None-Match + "\n" +  
$stringToSign += "$n" # If-Unmodified-Since + "\n" +  
$stringToSign += "$n" # Range + "\n" + 
$stringToSign +=    
                    <# SECTION: CanonicalizedHeaders + "\n" #>
                    "x-ms-date:$date" + $n + 
                    "x-ms-version:2018-11-09" + $n # 
                    <# SECTION: CanonicalizedHeaders + "\n" #>
 
If ($Path -eq $ContainerName) {
$stringToSign +=    
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$FilesystemName" + "/" + $n + 
                    "action:getAccessControl" + $n +
                    "upn:true"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
}
Else {
$stringToSign +=    
                    <# SECTION: CanonicalizedResource + "\n" #>
                    "/$StorageAccountName/$FilesystemName/$Path" + $n + 
                    "action:getAccessControl" + $n +
                    "upn:true"# 
                    <# SECTION: CanonicalizedResource + "\n" #>
}
 
$sharedKey = [System.Convert]::FromBase64String($AccessKey)
$hasher = New-Object System.Security.Cryptography.HMACSHA256
$hasher.Key = $sharedKey
 
$signedSignature = [System.Convert]::ToBase64String($hasher.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToSign)))
 
 
$authHeader = "SharedKey ${StorageAccountName}:$signedSignature"
 
$headers = @{"x-ms-date"=$date} 
$headers.Add("x-ms-version","2018-11-09")
$headers.Add("Authorization",$authHeader)
 
If ($Path -eq $ContainerName) { 
$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $FilesystemName + "/" + "?action=getAccessControl&upn=true"
}
Else {
$URI = "https://$StorageAccountName.dfs.core.windows.net/" + $FilesystemName + "/" + $Path + "?action=getAccessControl&upn=true"
}

#$URI
#$headers
#$method
 
$result = Invoke-WebRequest -method $method -Uri $URI -Headers $headers
 
#$result.Headers.'x-ms-acl'


$Permissions = $result.Headers.'x-ms-acl'.Split(",")

foreach ($ACL in $Permissions) 
{
#$ACL
#Create a row

    $ApplicationId = $ACL.Split(":")[1]
    $Type = $ACL.Split(":")[0]

    If ($ApplicationId -like '*-*')
    {

       If ($Type -eq 'user') 
       {
         #$Type = "application"
         $Application = Get-AZADApplication -ApplicationId $ApplicationId
       } 
       ElseIf ($Type -eq "group") 
       {
         $Application = Get-AZADGroup -ObjectId $ApplicationId
       }

        #$Type + " " + $ApplicationId + " " + $Application.DisplayName

      If ($Application.DisplayName -like '*-*') 
      {
        $row = $table.NewRow()

        #Enter data in the row
        $row.Folder = $Path
        $row.User = $Application.DisplayName
        $row.AppId = $ApplicationId
        $row.Type = $Type
        $row.ACL = $ACL.Split(":")[2]
    
        #Add the row to the table
        $table.Rows.Add($row)
      }
    }
}
}

#Display the table
$table | format-table -AutoSize