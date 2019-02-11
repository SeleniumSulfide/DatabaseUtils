[CmdletBinding()]
Param(
[Parameter(Mandatory=$false)][string]$srcServer = "wdv-bizappsql3",
[Parameter(Mandatory=$false)][string]$srcPort = 1583,
[Parameter(Mandatory=$false)][string]$srcDB = "CapitalStockInt",
[Parameter(Mandatory=$false)][string]$trgServer = "wdv-bizappsql3",
[Parameter(Mandatory=$false)][string]$trgPort = 1583,
[Parameter(Mandatory=$false)][string]$trgDB = "CapitalStockInt",
[Parameter(Mandatory=$false)][string]$logPath,
[Parameter(Mandatory=$false)][int]$threads,
[Parameter(Mandatory=$false)][int]$minRecords = 2000 #Minimum number of records for determining Sample Size
)

#Establish the path the script is being executed from and build module paths
$ScriptPath = Split-Path -parent $MyInvocation.MyCommand.Definition
$AnalysisLogging = "$ScriptPath\Analysis_Logging.ps1"
$Threading = "$ScriptPath\Threading.psm1"
$GetData = "$ScriptPath\Get-Data.psm1"
$Utils = "$ScriptPath\Data_Analysis_Utils.ps1"
Write-Host $utils

#import necessary modules
try
{
Import-Module $AnalysisLogging #| out-null
Import-Module $Threading #| out-null
Import-Module $GetData #| out-null
Import-Module $Utils #| out-null
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
		[string]$srcFQ, #Name of the Exemplar Database
		[string]$trgFQ, #Name of the Target Database
		[string]$rootPath, #Base Path for logging purposes
		[string]$ScriptPath, #Where the Script is being executed from
		[string]$table, #Table undergoing test
		[int]$minRecords, #Minimum number of records for determining Sample Size
		[int]$progressID, #INT used in write-progress calls
		[string]$commPath
		)
	try
	{
		if ($ScriptPath -eq "")
		{
			Add-Content "c:\users\itgend\dekstop\ScriptPathBlank-SB.txt" "blank"
			exit
		}
		$script = "$ScriptPath\Data_Analysis_Code.ps1"
		powershell $script "'$srcConnStr'" "'$trgConnStr'" "'$srcFQ'" "'$trgFQ'" "'$rootPath'" "'$ScriptPath'" "'$table'" $minRecords $progressID "'$commPath'"
	}
	catch
	{
		add-content "$rootPath\!Error.Log" $_
	}
}#End $SB

if ($srcServer -eq "" -or $srcPort -eq "" -or $srcDB -eq "" -or $trgServer -eq "" -or $trgPort -eq "" -or $trgDB -eq "")
{
	write-host "Connection information incomplete, exitng"
	break
}

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

#Instantiate the connection objects
$srcConn = New-Object System.Data.SqlClient.SqlConnection
$trgConn = New-Object System.Data.SqlClient.SqlConnection

#Set the Connection String
$srcConnStr = "Server=$srcServer,$srcPort;Database=$srcDB;Trusted_Connection=True;"
$trgConnSTr = "Server=$trgServer,$trgPort;Database=$trgDB;Trusted_Connection=True;"

#Set the ConnectionString property
$srcConn.ConnectionString = $srcConnStr
$trgConn.ConnectionString = $trgConnStr

#Build SQL to get list of tables
$sql = "" #For Readability of the below lines
$sql = "$($sql)select distinct top 1 concat(t.TABLE_SCHEMA, '.', t.TABLE_NAME) [TABLES]`r`n"
$sql = "$($sql)from INFORMATION_SCHEMA.TABLES t`r`n"
$sql = "$($sql)inner join INFORMATION_SCHEMA.TABLE_CONSTRAINTS c`r`n"
$sql = "$($sql)on t.TABLE_SCHEMA = c.TABLE_SCHEMA and t.TABLE_NAME = c.TABLE_NAME`r`n"
$sql = "$($sql)where TABLE_TYPE = 'BASE TABLE'`r`n"
$sql = "$($sql)and c.CONSTRAINT_TYPE = 'PRIMARY KEY'`r`n"
$sql = "$($sql)order by [TABLES]"
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

$srcFQ = "$($srcServer.ToUpper()).$($srcDB.ToUpper())"
$trgFQ = "$($trgServer.ToUpper()).$($trgDB.ToUpper())"

#Create the Progress bar so that if threads start prior to the completion of queueing, they have a parent to be under
Write-Progress -Activity "Performing PK Data Analysis of $($srcFQ) and $($trgFQ)" -Status "Queuing Threads: 0 of $($tables.length)" -id 0

Write-Host "`r`n`tBEGINNING QUEUEING THREADS"

foreach($table in $tables)
{
	#Write the Progress of queueing threads, add a thread to the pool, increment
	Write-Progress -Activity "Performing Data Analysis of $($srcFQ) and $($trgFQ)" -Status "Queuing Threads: $x of $($tables.length)" -id 0
	$pipes += Invoke-Async -RunspacePool $pool -ScriptBlock $sb -Parameters $srcConnStr, $trgConnStr, $srcFQ, $trgFQ, $rootPath, $ScriptPath, $table, $minRecords, $x, $commPath
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