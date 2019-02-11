[CmdletBinding()]
Param(
	[string]$srcConnStr, #Connection String of the Exemplar Database
	[string]$trgConnStr, #Connection String of the Target DAtabase
	[string]$srcDB, #Name of the Exemplar Database
	[string]$trgDB, #Name of hte Target Database
	[string]$rootPath, #Base Path for logging purposes
	[string]$ScriptPath, #Where the Script is being executed from
	[string]$table, #Table undergoing test
	[int]$minRecords, #Minimum number of records for determining Sample Size
	[int]$progressID, #INT used in write-progress calls
	[string]$commPath
	)

#Attempt to import the necessary modules and exit the thread if there is an exception
try
{
	Import-Module "$ScriptPath\Analysis_Logging.ps1"
	Import-Module "$ScriptPath\Get-Data.psm1"
	Import-Module "$ScriptPath\Data_Analysis_Utils.ps1"
}
catch
{
	Add-Content "$rootpath\Exceptions.log" "Error Loading Module"
	Add-Content "$rootpath\Exceptions.log" $_
	exit
}

$commFile = "$commPath\$pid.txt"

#Instantiate the connection variables
$srcConn = New-Object System.Data.OleDb.OleDbConnection
$trgConn = New-Object System.Data.OleDb.OleDbConnection

#Split the table into Schema & TableName
$tabInfo = $table -split ".",0,"simplematch"

#Set the Connection Strings on the connection variables
$srcConn.ConnectionString = $srcConnStr
$trgConn.ConnectionString = $trgConnStr

$i = 0
$valErr = 0

#Set logging file
$file = "$rootPath\Data_Analysis_$table.log"
$global:LogFile = "$rootPath\CSV-LOGS\$table.csv"

Add-Content $file "Beginning Data Analysis for $table`r`n"

#Get Source and Target record counts
$srcCount = Get-DataTable $srcConn "Select count(*) RECS from $table"
$trgCount = Get-DataTable $trgConn "select count(*) RECS from $table"

if ([int]$srcCount.RECS -eq 0 -and [int]$trgcount.RECS -ne 0) #Checks if the Source has no records so that the Analysis can be skipped
{
	Add-Content $file "`tError: NO RECORDS FOUND IN $srcDB"
	write-log-entry $global:LogFile $tabInfo[0] $tabInfo[1] "" "RECORD COUNT" $false "ERROR: NO RECORDS FOUND IN SOURCE" $srcCount $trgCount
	
	$failures.Add("------ NO RECORDS FOUND IN $($srcDB.ToUpper()) TABLE: $table") | out-null
	$noRecs = $true
	$valErr += 1
}
elseif ([int]$srcCount[0] -ne 0 -and [int]$trgCount[0] -eq 0) #Check if the Target has no records so the Analysis can be skipped
{
	Add-Content $file "`tError: NO RECORDS FOUND IN $trgDB"
	write-log-entry $global:LogFile $tabInfo[0] $tabInfo[1] "" "RECORD COUNT" $false "ERROR: NO RECORDS FOUND IN TARGET" $srcCount $trgCount
	
	$failures.Add("------ NO RECORDS FOUND IN $($trgDB.ToUpper()) TABLE: $table") | out-null
	$noRecs = $true
	$valErr += 1
}
elseif ([int]$srcCount[0] -ne [int]$trgCount[0]) #Compares the record counts and logs the error if necessary
{
	Add-Content $file "`tError: Record Count Mismatch"
	Add-Content $file "`t`t$($srcDB.ToUpper()) Record Count: $($srcCount[0])"
	Add-Content $file "`t`t$($trgDB.ToUpper()) Record Count: $($trgCount[0])"
	
	write-log-entry $global:LogFile $tabInfo[0] $tabInfo[1] "" "RECORD COUNT" $false "ERROR: RECORD COUNT MISMATCH" $srcCount $trgCount
	
	$valErr += 1
}
else #The records match, continue
{
	Add-Content $file "`tRecord Count Match"
	Add-Content $file "`t`t$($srcDB.ToUpper()) Record Count: $($srcCount[0])"
	Add-Content $file "`t`t$($trgDB.ToUpper()) Record Count: $($trgCount[0])"
	
	if ([int]$srcCount[0] -eq 0) { $noRecs = $true }
}

if ($noRecs -eq $true) #Checks if Either DB was missing rows and skips running through the records as it isn't necessary
{
	$noRecs = $false
	Add-Content $file "`r`n------NO RECORDS FOUND IN $trgDB, SKIPPING------"
}
else
{
	#Calculate Sample Percentage
	$sample = (($minRecords/[int]$srcCount[0])*100)
	$sample = [System.Math]::Round($sample,6)

	Add-Content $file "`r`n`t------Getting Exemplar Recordset for Analysis------`r`n"
	
	Set-Progress $commFile "Analyzing: $table" "Determing sample size" $progressID 0
	
	#If sample is 90% of records, get all records
	if ($sample -ge 90)
	{ 
		Add-Content $file "`t`tSample Size is ≥90% of table records, Selecting All Rows"
		$sql = "Select * from $table"
	}
	#if the sample size is less than .000001 use $minRecords value as using such a small sample size would cause a SQL error
	elseif ($sample -lt .000001) 
	{ 
		Add-Content $file "`t`tSample percentage too small, Selecting where rownum <= $minRecords"
		$sql = "Select * from $table where rownum <= $minRecords"#where rownum <10"; 
	}
	#Otherwise, get a random sample of a percentage of records as calculated
	else 
	{ 
		Add-Content $file "`t`tUsing Sample Size: $sample%"
		$sql = "Select * from $table sample($sample)"
	}
	
	Set-Progress $commFile "Analyzing: $table" "Getting $($srcdb.ToUpper()) record set" $progressID 0
	
	#Get Source Dataset
	$srcRows = Get-DataTable $srcConn $sql
	
	$records = 0
	$srcRows | foreach-object { $records++; } #Count the number of records returned for use with Write-Progress
	
	Add-Content $file "`t`tAnalyzing $records records"		
	Add-Content $file "`r`n`t------Finished Getting Exemplar Recordset------`r`n"
	
	#Get Primary Keys
	$PKs = Get-PKs $srcConn $tabInfo
	
	#Check if PKs is null which means no Primary Key has been defined on the table
	if ($PKs -eq $null)
	{
		#Get all Columns
		$PKs = Get-All_Cols $srcConn $tabInfo
		$distinct = " distinct"
		Add-Content $file "`r`n`t$table HAS NO PRIMARY KEY`r`n`tMATCHING ON ALL COLUMNS`r`n"
	}
	else
	{
		$distinct = ""
		Add-Content $file "`r`n`t$table PRIMARY KEY FOUND`r`n`tMATCHING ON PRIMARY KEY`r`n"
	}
	
	#Build SQL and get the number of Columns in this table
	$sql = "Select count(*) from dba_tab_columns where owner = '$($tabInfo[0])' and table_name = '$($tabInfo[1])'"
	$colCount = Get-DataTable $srcConn $sql
	
	$record = 0
	
	#Begin looping through dataset
	foreach ($srcRow in $srcRows)
	{
		
		#Build SQL for getting the Target Row
		$sql = Get-SQL $srcRow $PKs $table $distinct
		
		$trgRow = Get-DataTable $trgConn $sql
		
		#Write to log and command line every 100 records
		$i += 1
		if ($i % 100 -eq 0) { Add-Content $file "`tAnalyzed rows: $i"; }
		
		$status = $true
		
		#Check if row exists in target
		if ($trgRow -eq $null)
		{
			write-log $srcRow $trgRow $colCount[0] $file $sql
			write-log-entry $global:LogFile $tabInfo[0] $tabInfo[1] "" "TABLE DATA" $false "ERROR: RECORD NOT FOUND" $srcRow $trgRow
			$status = $false
			$valErr += 1
		}
		else
		{
			#Check the value of each column
			for($x = 0;$x -lt $colCount[0];$x++)
			{
				#Build SQL statement to get the DATA_TYPE of the column
				$dtSql = "select DATA_TYPE from dba_tab_columns where owner = '$($tabInfo[0])' and table_name = '$($tabInfo[1])' and column_id = $($x+1)"
				$dt = Get-DataTable $srcConn $dtSQL
				
				#Switch on Data_Type, if RAW, convert to Strings
				switch ([string]$dt.DATA_TYPE)
				{
					"RAW" { $srcVal = [string]$srcRow[$x]; $trgVal = [string]$trgRow[$x]; }
					DEFAULT { $srcVal = $srcRow[$x]; $trgVal = $trgRow[$x]; }
				}
				
				#If the values do not match, log the information
				if ($srcVal -ne $trgVal)
				{
					write-log $srcRow $trgRow $colCount[0] $file $sql
					write-log-entry $global:LogFile $tabInfo[0] $tabInfo[1] "" "TABLE DATA" $false "ERROR: DATA MISMATCH IN COLUMN $x" $srcRow $trgRow
					$valErr += 1
					$status = $false
					break
				}
			}
		}
		if ($status)
		{
			write-log-entry $global:LogFile $tabInfo[0] $tabInfo[1] "" "TABLE DATA" $true "NO ERROR FOUND" $srcRow $trgRow
		}
		#Increment the records analyzed, calculate Percent complete and display to user
		$record++
		$pct = [math]::floor(($record/$records)*100)
		Set-Progress $commFile "Analyzing: $table"  "Completed: $record of $records records - $pct%" $progressID 0
	}
}

Add-Content $file "`r`n`tData Analysis Errors: $valErr"
Add-Content $file "`tTotal Analyzed Rows: $i"

#Add the results to either Successes or Failures
$msg = "$table Data Analysis Errors: $valErr"
if ($valErr -eq 0)
{
	$resultFile = "$commPath\Successes.log" 
}
else
{
	$resultFile = "$commPath\Failures.log"
	rename-item $file "!Data_Analysis_$table.log";
}

add-content $resultFile $msg

$pct = [math]::floor(($record/$records)*100)
Set-Progress $commFile "Analyzing: $table"  "Completed: $record of $records records - $pct%" $progressID 0 $true

$srcConn.Close()
$trgConn.Close()

$srcConn.Dispose()
$trgConn.Dispose()
[System.GC]::Collect()