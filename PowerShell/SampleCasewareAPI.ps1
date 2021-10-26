#Get parent path of files..
 
$parentDir = "C:\CaseWare"
 
$parentFiles = Get-ChildItem $parentDir -Recurse -Include ('*.ac_', '*.ac')
 
$resultFile = ".\result.txt"
 
#Array of stores
$stores = "store01", "store02", "store03", "store04", "store05", "store06", "store07", "store08", "store09", "store10", "store11", "store12"

 
#Array of prod servers
$servers = "fwdmapssvc01", "fwdmapssvc02", "fwdmapssvc03", "fwdmapssvc04", "fwdmapssvc05", "fwdmapssvc06", "fwdmapssvc07", "fwdmapssvc08", "fwdmapssvc09", "fwdmapssvc10", "fwdmapssvc11", "fwdmapssvc12"
 
 

#Grab a reference of caseware working papers com object
$app = New-Object -ComObject Caseware.Application
 
 
 
#user app.clients to open a file via com 
$clients = $app.Clients
 
foreach($parentFile in $parentFiles)
{
    write-host "Opening File: " $parentFile.FullName
    #if RSM customization is fully installed use "RSMEM!Adm2007" .. .other wise, use sup as password.
 
    $client = $clients.Open2($parentFile.FullName, "sup", "RSMEM!Adm2007")
 
 
 
    #always call close or CloseCompressed().. otherwise, it will leave a .locks file... and .cwclock
 
 
    #if you leave this file, no com api can open the file again. you can get rid of the lock file by opening the client file via the GUI 
    #or navigating the windows directory where the client file is.. then try to delte it.
 
 
   # $client.Close()
   # $client.CloseCompressed()
 
}
 
 
#if working papers GUI is open with a client file.
#you can get a hold of the active client by using app.ActiveClient
 
#Grab a reference of caseware working papers com object
$app = New-Object -ComObject Caseware.Application
$client  = $app.ActiveClient
 
$client.Close()
 
 
#to list all the documents within doc manager and Broken Links... 
 
$docs = $client.Documents
 
$clientPath = $client.FilePath
 
foreach($doc in $docs)
{
    $fullFilePath = Join-Path $clientPath $doc.FileName     
 
    if((Test-Path $fullFilePath) -eq $false)
    {
        Write-Host "Broken Link" $doc.FileName
    }
 
    Write-Host $fullFilePath.Length
 
    #you can filter by doc type.. id, ad such... 
    #Write-Host $doc.Name
 
    $doc.Type
 
#Constant Name   Description      Value
#dtFolder        Folder  0
#dtAutomatic     Automatic Document       1
#dtCaseView      CaseView Document        2
#dtManual        Manual Document Reference        3
#dtExternalLink  External Document Link   4
#dtLink Document Link    5
#dtURL  URL     6
#dtIdea IDEA    7
#dtWord Word (CW 2006.00.084)    8
#dtExcel Excel (CW 2006.00.084)   9
    
 
}
 
 
#to get a hold of the CV database and get and set variables..
 
$cwData = $client.CaseViewData
 
 
# set values of 1 to a variable name "EngComplete"
 
$cwData.Data("EngComplete") = 1
 
# set values of 0 to a variable name "EngCompleteError"
$cwData.Data("EngCompleteError") = 0
 
 
# gets values from variable "EngComplete" if it exist
$cwData.Data("EngComplete")
 
 
#get client template version (if this is a client file based off a template) this would have value..
 
$client.ClientVersionInfo
 
#get template version (if this is a template file installed on factory server... or if it's a template installed on desktop)
 
$client.TemplateVersionInfo
 
#get client's name from client file..
 
$client.ClientProfile
 
#do a "Year End Close".... aka a roll forward..
$app = New-Object -ComObject Caseware.Application
$client = $app.Clients.Open2("C:\FullLocationOfAcFile\includeNameofAc.ac", "sup", "sup")
 
$cwYec = $client.YearEndClose
$cwYec.IncludeBAKFiles = $false;
 
 
$cwYec.CompressPriorYearFile = $false;
 
$cwYec.UpdatePriorYearBalanceData = $true;
$cwYec.UpdateNextYearOpeningBalanceData = $false;
$cwYec.RollForwardForecasts = $false;
$cwYec.RollForwardBudgets = $false;
$cwYec.UpdateCaseViewRollForwardCells = $true;  
 
$cwYec.RollForwardAllCustomBalances = $true;
$cwYec.CopySpreadsheetAnalysis = $false;
$cwYec.CopyForeignExchange = $true; 
$cwYec.CopyProgramAssertionInfo = $false;
$cwYec.CopyProgramChecklistInfo = $false;
$cwYec.CopyAnnotationText = $true;
$cwYec.CopyOutstandingTransactions = $false;
$cwYec.CopyDocumentReferences = $true;
$cwYec.CopyTickmarks = $true;
$cwYec.CopyAnnotationReferences = $true;
$cwYec.CopyCVDocumentReferences = $true;
$cwYec.CopyCVTickmarks = $true;
$cwYec.CopyCVNotes = $true;
 
$cwYec.CompleteDestinationFileName = "C:\Ranga\Caseware\Data\Output\SomeFileName"
 
$cwYec.DestinationPassword = "sup"
$cwYec.DestinationUserId = "RSMEM!Adm2007"
$cwYec.DoYearEndClose()
 
 
#create engagement... aka copy componenet..
$app = New-Object -ComObject Caseware.Application
 
$cwCopy = $app.CopyTemplate;
$cwCopy.CopyAll = true;
$cwCopy.CompleteSourceFileName = "C:\Program Files (x86)\CaseWare\Document Library\RSM Audit\RSM AUDIT.ac"
$cwCopy.SourcePassword = "RSMEM!Adm2007"
$cwCopy.SourceUserId = "sup"
$cwCopy.CompleteDestinationFileName = "C:\LocationOfTheClientFileToBeCreated\"
$cwCopy.DestinationPassword = "RSMEM!Adm2007"
$cwCopy.DestinationUserId = "sup"
$cwCopy.DoCopyLite();
 
 
function Log-ToFile ($message)
{
    $now = (get-date).DateTime
    $now + ', ' + $message | Out-File -Append -FilePath $resultFile
}
 
 
function Convert-ToCurrentVersion($clients, $pathOfAc)
{
    #set a testing path to test
    #$path = "E:\ D and S Auto Parts Inc - COL665 - 201(888558)_\ D and S Auto Parts Inc - COL665 - 201(888558).ac_"
 
   try
   {
        Write-Host "Checking version for path: " $pathOfAc
        $metaData = $clients.GetMetaData($pathOfAc)
 
        $fileVersion = $metaData["CWFileVersion"].value
        Write-Host "File Version: "  $fileVersion
 
        if($fileVersion -ne "8.87")
        {
            Write-Host "Converting file to Current version of working papers "  $pathOfAc
 
            $clients.Convert($path)
 
            Write-Host "Conversion complete for file "  $pathOfAc
 
        }
   }
   finally
   {
        Log-ToFile("Error while Converting " + $pathOfAc)
       
   }
}
 
 
foreach($parentFile in $parentFiles)
{
    
    Log-ToFile($parentFile.FullName)
    write-host (get-date) "Opening File: " $parentFile.FullName
    $fullPathOfAcFile = $parentFile.FullName
    #if RSM customization is fully installed use "RSMEM!Adm2007" .. .other wise, use sup as password.
 
    Convert-ToCurrentVersion $clients $fullPathOfAcFile
 
   $client = $app.Clients.Open2($parentFile.FullName, $cwUser, $cwPassword)
 
 
    $security = $client.Security
 
    Write-Host "Current Protection Status"   $security.Protection
 
 
 
    if($security.Protection -eq $true)
    {
        Write-Host $security.CurrentUser
        Write-Host "Attempting to turn off protection status"
        $security.SetProtection($false, $cwUser, $cwPassword)
        Write-Host "New Protection Status"   $security.Protection
    }
 
    Write-Host "Compressing File"
 
    $client.CloseCompressed()
}
