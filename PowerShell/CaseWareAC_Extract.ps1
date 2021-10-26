#Get parent path of files..
 
$parentDir = "C:\CaseWareACFiles\"
 
$parentFiles = Get-ChildItem $parentDir -Recurse -Include ('*.ac_')
 

#Grab a reference of caseware working papers com object
$app = New-Object -ComObject Caseware.Application
 
 
#user app.clients to open a file via com 
$clients = $app.Clients
 
foreach($parentFile in $parentFiles)
{
    Copy-Item -path $parentFile.FullName -Destination "C:\CaseWare\Test1\"

    $fileName = Get-ChildItem "C:\CaseWare\Test1\" -Recurse -Include ('*.ac_')

    write-host "Opening File: " $fileName.FullName
    #if RSM customization is fully installed use "RSMEM!Adm2007" .. .other wise, use sup as password.
 
    $client = $clients.Open2($fileName.FullName, "sup", "RSMEM!Adm2007")
    $client.close()
    $extractedFiles = Get-ChildItem "C:\CaseWare\Test1\" -Recurse -Include ('*.*')
    foreach($file in $extractedFiles)
    {
        Move-Item -Path $file.FullName -Destination "C:\CaseWare\" -Force
    }
   
    
  
}
 


 

 
#to list all the documents within doc manager and Broken Links... 
 
