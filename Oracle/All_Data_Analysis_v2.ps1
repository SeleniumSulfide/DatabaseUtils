[CmdletBinding()]
Param(
[Parameter(Mandatory=$true)][string]$srcDB, #Database to be used as the exemplar, utilizing TNSNames.ORA
[Parameter(Mandatory=$false)][string]$srcUser, #User name for use with Source Database
[Parameter(Mandatory=$false)]$srcPass, #Password for use with Source User
[Parameter(Mandatory=$true)][string]$trgDB, #Database under test, utilizing TNSNAmes.ora file
[Parameter(Mandatory=$false)][string]$trgUser, #User name for use with Target Database
[Parameter(Mandatory=$false)]$trgPass, #Password for use with Target User
[Parameter(Mandatory=$false)][boolean]$StoredPass = $false, #Flag for utilizing encrypted passwords stored on disk
[Parameter(Mandatory=$false)][string]$logPath, #Path to log files, default is user's desktop
[Parameter(Mandatory=$false)][int]$threads, #determines the number of concurrent threads
[Parameter(Mandatory=$true)]$schemas = '', #Commadelimited List of schemas to validate
[Parameter(Mandatory=$false)][int]$minRecords = 2000 #Minimum number of records for determining Sample Size
)

#Establish the path the script is being executed from and build module paths
$ScriptPath = Split-Path -parent $MyInvocation.MyCommand.Definition
$AnalysisLogging = "$ScriptPath\Analysis_Logging.ps1"
$EncryptPassword = "$ScriptPath\Encrypt-Password.psm1"
$Threading = "$ScriptPath\Threading.psm1"
$GetData = "$ScriptPath\Get-Data.psm1"
$Utils = "$ScriptPath\Data_Analysis_Utils.ps1"

#import necessary modules
try
{
Import-Module $AnalysisLogging | out-null
Import-Module $EncryptPassword | out-null
Import-Module $Threading | out-null
Import-Module $GetData | out-null
Import-Module $Utils | out-null
}
catch
{
	Write-Host "Error importing module:"
	Write-Host $_
	exit
}

#Begin the script block. What is containined within will be what each Thread is executing
[scriptblock]$sb =
{
	[CmdletBinding()]
	Param(
		[string]$srcConnStr, #Connection String of the Exemplar Database
		[string]$trgConnStr, #Connection String of the Target Database
		[string]$srcDB, #Name of the Exemplar Database
		[string]$trgDB, #Name of the Target Database
		[string]$rootPath, #Base Path for logging purposes
		[string]$ScriptPath, #Where the Script is being executed from
		[string]$table, #Table undergoing test
		[int]$minRecords, #Minimum number of records for determining Sample Size
		[int]$progressID, #INT used in write-progress calls
		[string]$commPath
		)
	try
	{
		$script = "$ScriptPath\Data_Analysis_Code.ps1"
		powershell $script "'$srcConnStr'" "'$trgConnStr'" "'$srcDB'" "'$trgDB'" "'$rootPath'" "'$ScriptPath'" "'$table'" $minRecords $progressID "'$commPath'"
	}
	catch
	{
		add-content "$rootPath\!Error.Log" $_
	}
}#End $SB

#Set Root Path
if ($logPath -eq "")
{
    $logPath = [Environment]::GetFolderPath("Desktop") 
}
else
{
    if ($logPath.EndsWith("\") -or $logPath.EndsWith("/")) { $logPath = $logPath -replace ".{1}$" } #remove a trailing \ or / if one exists
}
$rootPath = "$logPath\All_Data_Analysis_v2-$(get-date -f 'yyyyMMddHHmmss')"

New-Path "$logPath"
New-Path "$rootPath"
New-Path "$rootPath\CSV-LOGS"

$commPath = New-TemporaryDirectory

#Check if user wishes to use a stored password for the database
if ($storedPass -eq $false)
{
    #Prompt user for Source DB Credentials as necessary
    if ($srcUser -eq "") { $srcUser = read-host "Enter $($srcDB.ToUpper()) User Name" }
    if ($srcPass -eq $null) { $srcPass = read-host "Enter $($srcDB.ToUpper()) Password" -AsSecureString; $srcUnenc = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($srcPass)); }
        else { $srcUnenc = $srcPass }
   

    #Prompt user for Target DB credentials as necessary
    if ($trgUser -eq "") { $trgUser = read-host "Enter $($trgDB.ToUpper()) User Name" }
    if ($trgPass -eq $null) { $trgPass = read-host "Enter $($trgDB.ToUpper()) Password" -AsSecureString; $trgUnenc = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($trgPass)); }
        else { $trgUnenc = $trgPass }
}
else
{
    #Decrypt the password, function in Encrypt-Password.psm1
	$srcUnenc = Unprotect-Password $srcDB "$ScriptPath\Encrypted-Pass.txt"
    $trgUnenc = Unprotect-Password $trgDB "$ScriptPath\Encrypted-Pass.txt"
        
    if (!($srcUnenc)) #Check if the password was found
    {
        Write-Host "Stored Password not found for $($srcDB.ToUpper())"
        exit
    }

    if (!($trgUnenc)) #Check if the password was found
    {
        Write-Host "Stored Password not found for $($trgDB.ToUpper())"
        exit
    }
}

#Instantiate the connection objects
$srcConn = New-Object System.Data.OleDb.OleDbConnection
$trgConn = New-Object System.Data.OleDb.OleDbConnection

#Set the Connection String
$srcConnStr = "User ID=$srcUser;password=$srcUnenc;Data Source=$srcDB;Provider=OraOLEDB.Oracle"
$trgConnSTr = "User ID=$trgUser;password=$trgUnenc;Data Source=$trgDB;Provider=OraOLEDB.Oracle"

#Set the ConnectionString property
$srcConn.ConnectionString = $srcConnStr
$trgConn.ConnectionString = $trgConnStr

#Replace any spaces, add ' around commas, then add ' at either end
$schemas = $schemas -replace " ", ""
$schemas = $schemas -replace ",", "','"
$schemas = "'$schemas'"

#Build SQL to get list of tables
$sql = "" #For Readability of the below lines
$sql = "$($sql)select distinct t.owner || '.' || t.table_name TABLES`r`n"
$sql = "$($sql)from dba_tables t`r`n"
$sql = "$($sql)inner join dba_constraints c`r`n"
$sql = "$($sql)on t.owner = c.owner and t.table_name = c.table_name`r`n"
$sql = "$($sql)where c.constraint_type = 'P'`r`n"
$sql = "$($sql)and t.owner in ($schemas)`r`n"
$sql = "$($sql)and c.index_name is not null`r`n" #Excludes tables with PK constraints not backed by an Index
$sql = "$($sql)and t.table_name not in (select table_name from dba_tab_columns where DATA_TYPE = 'XMLTYPE')`r`n" #XML Datatype cannot be handled by OleDbConneciton at this time
$sql = "$($sql)order by tables"
$tbls = Get-DataTable $srcConn $sql

#Instatiate the Tables array and then fill it with the recordset
$tables = @()
foreach ($tbl in $tbls)
{
    $tables += "$($tbl.TABLES)"
}

#Assign the variables to the session state
$sessionState = [system.management.automation.runspaces.initialsessionstate]::CreateDefault()

#If threads was not specified, calculate the number of threads with a Max of 8 based on the processing cores available
if ($threads -eq 0)
{
	#Determine the number of Cores on the executing machine
	$processor = get-wmiobject win32_processor;
	[int]$procs = $processor.NumberofLogicalProcessors
	$threads = $procs
}

#Create a Threading Pool that will allow $threads number of threads to execute at once
$Pool = Get-RunspacePool $threads -SessionState $sessionState

#Create the pipeline (eg the Powershell Console is a pipeline)
$pipeline  = [System.Management.Automation.PowerShell]::create()
$pipeline.RunspacePool = $pool

$pipes = @() #instatiate the PIPES array
$x = 1 #Set x to 1 for use as the ProgressID
$start = get-date #Get the Start Time

#Create the Progress bar so that if threads start prior to the completion of queueing, they have a parent to be under
Write-Progress -Activity "Performing PK Data Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Queuing Threads: 0 of $($tables.length)" -id 0

Write-Host "`r`n`tBEGINNING QUEUEING THREADS"

foreach($table in $tables)
{
	#Write the Progress of queueing threads, add a thread to the pool, increment
	Write-Progress -Activity "Performing Data Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Queuing Threads: $x of $($tables.length)" -id 0
	$pipes += Invoke-Async -RunspacePool $pool -ScriptBlock $sb -Parameters $srcConnStr, $trgConnStr, $srcDB, $trgDB, $rootPath, $ScriptPath, $table, $minRecords, $x, $commPath
	$x++
}

#Inform user all threads have completed
Write-Host "`r`n`tALL THREADS QUEUED, PLEASE BE PATIENT WHILE THEY EXECUTE"

#Begin watching for completion messages from threads
Watch-Files $pipes $srcDB $trgDB $commPath

Write-Summary $rootPath $commPath

$file = "$rootpath\AllDataAnalysis-Log.csv"
write-log-headers $file $srcDB $trgDB

Join-CSVlogs "$rootPath\CSV-LOGS" $file

$end = get-date
$x = $end - $start

Write-Host "`r`nExecution Time: $x`r`n"

Remove-Item $commPath -force -recurse

#Begin cleaning up threads and connections
$srcConn.close() 
$trgConn.close()

foreach ($pipe in $pipes)
{
	$pipe.pipeline.dispose()
}
$pool.close()
$pool.dispose()

[System.GC]::Collect()