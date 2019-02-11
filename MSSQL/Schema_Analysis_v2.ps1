[CmdletBinding()]
Param(
[Parameter(Position=0,Mandatory=$true)][string]$srcDB,
[Parameter(Position=1,Mandatory=$false)][string]$srcUser,
[Parameter(Position=2,Mandatory=$false)]$srcPass,
[Parameter(Position=3,Mandatory=$true)][string]$trgDB,
[Parameter(Position=4,Mandatory=$false)][string]$trgUser,
[Parameter(Position=5,Mandatory=$false)]$trgPass,
[Parameter(Position=9,Mandatory=$false)][boolean]$StoredPass = $false,
[Parameter(Position=10,Mandatory=$false)][string]$logPath,
[Parameter(Mandatory=$false)][int]$threads, #determines the number of concurrent threads
[Parameter(Position=6,Mandatory=$true)]$Schemas = '', #comma delimited list
[Parameter(Position=7,Mandatory=$false)][string]$ObjectTypes, #comma delimited list
[Parameter(Position=8,Mandatory=$false)]$excludeTypes = $false,
[Parameter(Position=11,Mandatory=$false)][int]$Page = 500

)

#Establish the path the script is being executed from and build module paths
$ScriptPath = Split-Path -parent $MyInvocation.MyCommand.Definition
$AnalysisLogging = "$ScriptPath\Analysis_Logging.ps1"
$EncryptPassword = "$ScriptPath\Encrypt-Password.psm1"
$Threading = "$ScriptPath\Threading.psm1"
$GetData = "$ScriptPath\Get-Data.psm1"

#import necessary modules
try
{
Import-Module $AnalysisLogging | out-null
Import-Module $EncryptPassword | out-null
Import-Module $Threading | out-null
Import-Module $GetData | out-null
}
catch
{
	Write-Host "Error importing module:"
	write-host $_
	exit
}

#This function endlessly loops while the threads are running and
#writes the current progress
Function Watch-Messages
{
	[Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)]$Pipes,
		[Parameter(Position=1,Mandatory=$True)][string]$srcDB,
		[Parameter(Position=2,Mandatory=$True)][string]$trgDB
    )
	
	#Sets the status to false for the while statement
	[boolean]$status = $false
	
	#begin looping while $status is $false
	while(!($status))
	{
		#check for Thread status, if a thread is running, returns $true
		#Get-ThreadComplete is in Threading.psm1
		$status = Get-ThreadsComplete $pipes
		
		#Clones the current version of the session variable $threadMsgs, necessary for avoiding thread contention
		$Messages = $threadMsgs.Clone()
		$i = 0
		#count the number of completed threads
		foreach ($msg in $Messages) 
		{ 
			if ($msg -match "Completed")
			{
				$i++
			}
		}
		#Determine % of threads complete and display progress, then sleep for 1 second	
		$pct =[math]::floor(($i/$Pipes.Length)*100)
		Write-Progress -Activity "Performing Schema Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Completed: $i of $($Pipes.Length) - $pct%" -id 0
		Start-Sleep -Seconds 1
	}

	#This causes the progress bar and any children to disappear from the screen
	Write-Progress -Activity "Performing Schema Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Completed: $i of $($Pipes.Length) - $pct%" -id 0 -Completed #-PercentComplete $pct
}

#This function checks for a folder and if not found creates it
Function New-Path
{
	[Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)]$path
    )
	
	if(!(Test-Path -path $path)){ new-item -itemtype directory -path $path | out-null }
}

#Begin the script block. What is containined within will be what each Thread is executing
[scriptblock]$sb =
{
    Param
    (
    [Parameter(Position=0,Mandatory=$true)][string]$srcConnStr,#Connection String of the Exemplar Database
    [Parameter(Position=1,Mandatory=$true)][string]$trgConnStr,#Connection String of the Target Database
    [Parameter(Position=2,Mandatory=$true)][string]$schema, #Schema that will be tested
    [Parameter(Position=3,Mandatory=$true)][string]$objectTypes, #The single or multiple object types that will be analyzed by this thread
    [Parameter(Position=4,Mandatory=$true)][string]$rootPath, #Base Path for logging purposes
    [Parameter(Position=5,Mandatory=$true)][string]$srcDB, #Name of the Exemplar Database
    [Parameter(Position=6,Mandatory=$true)][string]$trgDB, #Name of hte Target Database
    [Parameter(Position=7,Mandatory=$true)][string]$ScriptPath, #Where the Script is being executed from
	[Parameter(Position=8,Mandatory=$true)][int]$Page, #The Page size or Record Set size that will be retrieved for each loop
	[Parameter(Position=9,Mandatory=$false)][int]$progressID #INT used in write-progress calls
    )
    
	#Attempt to import the necessary modules and exit the thread if there is an exception
	
	try
	{
		Import-Module "$ScriptPath\Analysis_Logging.ps1"
		Import-Module "$ScriptPath\Get-Data.psm1"
	}
	catch
	{
		add-content "$rootpath\Exceptions.log" "Error Loading Module"
		add-content "$rootpath\Exceptions.log" $_
		exit
	}
	
	#This function checks for a folder and if not found creates it
	Function New-Path
	{
		[Cmdletbinding()]
		Param
		(
			[Parameter(Position=0,Mandatory=$True)]$path
		)
		
		if(!(Test-Path -path $path)){ new-item -itemtype directory -path $path | out-null }
	}
	
	#This function buildes the 
    function Build-SQL ($from, $cols)
    {
        
    	$sql = "select * from $from where" #Begin building the SQL statement
        $and = " " #set to blank until the end of the first loop
    	
		#Construct the Where clause based on the Cols collection and the column's datatype
        foreach ($col in $cols)
    	{
    		switch ($global:src.GetDataTypeName($col))
    		{
    			"DBTYPE_DATE" 
                {
                    $where = "coalesce($($global:src.GetName($col)), to_date('01/01/1970 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))"
                    $val = "coalesce(to_date('$($global:src.item($col))', 'MM/DD/YYYY HH24:MI:SS'), to_date('01/01/1970 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))"
                }
    			"DBTYPE_TIMESTAMP"
                {
                    $where = "coalesce($($global:src.GetName($col)), to_timestamp('01/01/1970 00:00:000000000', 'MM/DD/YYYY HH24:MI:SS.FF'))"
                    $val = "coalesce(to_timestamp('$($global:src.item($col))', 'MM/DD/YYYY HH24:MI:SS.FF'), to_timestamp('01/01/1970 00:00:000000000', 'MM/DD/YYYY HH24:MI:SS.FF'))"
                }
    			"DBTYPE_VARNUMERIC"
                {
                    $where = "coalesce($($global:src.GetName($col)), 0)"
                    $val = "coalesce($($global:src.item($col)), 0)"
                }
                default 
                {
                    $where = "coalesce($($global:src.GetName($col)), 'NULL')"
                    $val = "coalesce('$($global:src.item($col))', 'NULL')"
                }
    		}
            
            $sql = "$sql$and$where = $val" #Concatenate the clause
            $and = " and " #change so that more than one column can be handled
			$where = "" #Clear the current value
    	}
    	
    	return $sql
    }

	#This function analyzes the differences between the two objects bsed on the COLS array
    function Analyze-Records ($cols, $file, $objType, $schema, $objName)
    {
    	$status = $true
    	    
    	foreach ($col in $cols) #Begin looping through COLS
    	{
            try 
            {
        		#Compare the source & target values
				#The code to retrieve the target value is written so that the column name is used, this accounts for the event that the column order has shifted between versions
				if ($global:src.Item($col) -ne $global:trg.Item($global:src.GetName($col).ToString()))
        		{
					$tabs = "`t`t" #Sets the number of tabs, this can be modified if the object type is a TABLE COLUMN
        			$global:failure = Add-Result $objType $global:failure $schema #Adds a failure result
					if ($objType -eq "TABLE COLUMN") { add-content $file "$($tabs)Column: $($global:subObjName)"; $tabs = "`t`t`t"; } #Modify the number of TABs in $tabs and write into log file
        			add-content $file "$($tabs)Error: OBJECT DEFINITION - $($global:src.GetName($col))"
        			add-content $file "$($tabs)`t$($global:srcDB): $($global:src.Item($col))"
        			add-content $file "$($tabs)`t$($global:trgDB): $($global:trg.Item($global:src.GetName($col).ToString()).ToString())"
                    write-log-entry $global:LogFile $schema $global:ObjectName $global:subObjName $objType $false "Error: OBJECT DEFINITION - $($global:src.GetName($col))`r`n$($global:srcDB): $($global:src.item($col))`r`n$($global:trgDB): $($global:trg.Item($global:src.GetName($col).ToString()).ToString())" "src" "trg"
        			$status = $false
        		}
            } 
            catch 
            {
                add-content"$rootpath\Exceptions.log" $_
            }
    	}
    	
        if ($status) #If no errors were thrown, log a success
        {
            write-log-entry $global:LogFile $schema $global:ObjectName $global:subObjName $objType $true "No Definition Errors Found" "src" "trg"
            $global:success = Add-Result $objType $global:success $schema
        }
        
    	return $status
    }
	
	#This adds a result for a given Schema & Object type
    function Add-Result ($object, $Result, $schema)
    {
		#Unpack the $Result that was passed in
    	$schemas = $Result[0]
        $objs = $Result[1]
    	$count = $Result[2]
    	$found = $false
    	
        
    	for ($i = 0; $i -lt $objs.count; $i++) #Begin looping through items in the collections
    	{
    		if ([string]$schemas[$i] -eq $schema -and [string]$objs[$i] -eq [string]$object) #IF the Schema and Type are found, break
    		{
    			$found = $true
    			break
    		}
    	}
    	
    	if ($found) #If found, increment
    	{
    		$count[$i] += 1
    	}
    	else #Otherwise, add a new entry
    	{
            $schemas += $schema
    		$objs += $object
    		$count += 1
    	}
    	
		#Pack the $Result back up for returning
    	$Result[0] = $schemas
        $Result[1] = $objs
    	$Result[2] = $count
        
    	return $Result
    }

	#After the script has finished, write the results to a log file
    Function Write-Results($file, $result, $status, $schema)
    {
		#Unpack the result
    	$schemas = $result[0]
        $objs = $result[1]
    	$count = $result[2]
    	$msgs = @()
    	
    	for ($i = 0; $i -lt $objs.count; $i++) #Begin looking through the results for the entries that match the $Schema
    	{
            if ($schemas[$i] -eq $schema -or $schema -eq "")
            {
				$obj = ($objs[$i]).PadRight(18," ") #Pad the obj variable
				$stat = ($status).PadRight(9," ") #Pad the number
                $msgs += "`t$($schemas[$i]) - $($obj) - $($stat): $($count[$i])" #Add the result message to the MSGS array
            }
    	}
    	
    	$msgs.sort #Sort the array
        
    	foreach ($msg in $msgs) #add the messages to a file
    	{
    		add-content $file $msg
    	}
    }
	
	$start = get-date
	#Instantiate the connection objects
	$srcConn = New-Object System.Data.OleDb.OleDbConnection
	$trgConn = New-Object System.Data.OleDb.OleDbConnection
    
	#Set the Connection String
    $srcConn.ConnectionString = $srcConnStr
    $trgConn.ConnectionString = $trgConnStr
    
	#Place the Source & Target DB Names in global variables
    $global:srcDB = $srcDB
	$global:trgDB = $trgDB
    
	#Instantiate the necessary objects
    $global:failure = New-Object 'object[]' (3)
    $global:failure[0] = @()
    $global:failure[1] = @()
    $global:failure[2] = @()

    $global:success = New-Object 'object[]' (3)
    $global:success[0] = @()
    $global:success[1] = @()
    $global:success[2] = @()
    
    #Create a folder for the Schema logs
	$schemaPath = "$rootpath\$schema"
	New-Path $schemaPath
    
	#If the type is a TABLE COLUMN, a different SQL statuement is utilized
	if ($ObjectType -eq "TABLE COLUMN")
	{
		$sql = "select count(*) from dba_tab_columns"
		$sql = "$sql`r`nwhere owner = '$schema'"
	}
	else #Get the total number of objects of this type for this schema
	{
		$sql = "select count(*) RECS from (`r`n"
		$sql = "$sql select OWNER, OBJECT_NAME, SUBOBJECT_NAME, OBJECT_ID, DATA_OBJECT_ID, OBJECT_TYPE, CREATED, LAST_DDL_TIME, cast(TIMESTAMP as VARCHAR2(30)) TIMESTAMP, STATUS, TEMPORARY, GENERATED, SECONDARY from dba_objects where owner = '$schema'`r`n"
		$sql = "$sql union all`r`n"
		$sql = "$sql select owner, object_name, subobject_name, object_id, data_object_id, 'OBJECT DEFINITION', created, last_ddl_time, timestamp, status, temporary, generated, secondary from dba_objects where owner = '$schema'`r`n"
		$sql = "$sql union all`r`n"
		$sql = "$sql select owner, table_name, column_name,0 ,0 ,'TABLE COLUMN',sysdate ,sysdate ,cast(sysdate as VARCHAR2(30)),'VALID' ,'N' ,'N' ,'N' From dba_tab_columns where owner = '$schema'`r`n"
		$sql = "$sql union all`r`n"
		$sql = "$sql select index_owner,index_name,column_name,0 ,0 ,'INDEX COLUMN',sysdate ,sysdate ,cast(sysdate as VARCHAR2(30)),'VALID' ,'N' ,'N' ,'N' from dba_ind_columns where index_owner = '$schema'`r`n"
		$sql = "$sql union all`r`n"
		$sql = "$sql select owner, constraint_name, table_name, 0, 0, 'CONSTRAINT',sysdate ,sysdate ,cast(sysdate as VARCHAR2(30)),'VALID' ,'N' ,'N' ,'N' from dba_constraints where owner = '$schema')`r`n"
		$sql = "$sql where object_type in ('$ObjectTypes')"# and rownum < 6"
		$sql = "$sql order by owner, object_type, object_name, subobject_name"
	}
	$objCount = Get-DataReader $srcConn $sql
	
	if ($objCount.HasRows) #If records were found, read them into objRecords
	{
		$objCount.read() | out-null
		$objRecords = [int]$objCount[0]
	}
	else
	{
		$objRecords = 0
	}
	
	#Clean up objects
	$objRecords.Close()
	$objRecords.Dispose()
	
	#Initialize paging variables
	#Paging is necessary to not exceed PowerShells memory limitations
	$objMin = 1
	$objMax = $page
	$objAnalyzed = 0
	
	while ($objMin -le $objRecords) #Begin looping through the number of objects
	{
		if ($ObjectTypes -eq "TABLE COLUMN") #Synthetically builds the "TABLE COLUMN" objects from the source
		{
			$sql = "select b.*, '[BLANK]', '[BLANK]', 'TABLE COLUMN' from (select a.*, rownum rnum from (select distinct owner, table_name from dba_tab_columns where owner = '$schema' order by owner, table_name) a) b where rnum between $objMin and $objMax"
		}
		else #Retrieve a page sized recordset of the objects for this schema
		{
			$sql = "select * from (select a.*, rownum rnum from (`r`n"
			$sql = "$sql select OWNER, OBJECT_NAME, SUBOBJECT_NAME, OBJECT_ID, DATA_OBJECT_ID, OBJECT_TYPE, CREATED, LAST_DDL_TIME, cast(TIMESTAMP as VARCHAR2(30)) TIMESTAMP, STATUS, TEMPORARY, GENERATED, SECONDARY from dba_objects where owner = '$schema'`r`n"
			$sql = "$sql union all`r`n"
			$sql = "$sql select owner, object_name, subobject_name, object_id, data_object_id, 'OBJECT DEFINITION', created, last_ddl_time, timestamp, status, temporary, generated, secondary from dba_objects where owner = '$schema'`r`n"
			$sql = "$sql union all`r`n"
			$sql = "$sql select owner ,table_name,column_name,0 ,0 ,'	TABLE COLUMN',sysdate ,sysdate ,cast(sysdate as VARCHAR2(30)),'VALID' ,'N' ,'N' ,'N' From dba_tab_columns where owner = '$schema'`r`n"
			$sql = "$sql union all`r`n"
			$sql = "$sql select index_owner,index_name,column_name,0 ,0 ,'INDEX COLUMN',sysdate ,sysdate ,cast(sysdate as VARCHAR2(30)),'VALID' ,'N' ,'N' ,'N' from dba_ind_columns where index_owner = '$schema'`r`n"
			$sql = "$sql union all`r`n"
			$sql = "$sql select owner, constraint_name, table_name, 0, 0, 'CONSTRAINT',sysdate ,sysdate ,cast(sysdate as VARCHAR2(30)),'VALID' ,'N' ,'N' ,'N' from dba_constraints where owner = '$schema') a`r`n"
			$sql = "$sql where object_type in ('$ObjectTypes'))"
			$sql = "$sql where rnum between $objMin and $objMax"
			$sql = "$sql order by owner, object_type, object_name, subobject_name"
		}
		
		$objs = Get-DataReader $srcConn $sql
		$subObj = $false
		
		$skip = $false
		
		while ($objs.Read()) #Begin reading through the objects
		{	
    
			$objType = [string]$objs[5].ToString() #Record the Object Type
			
			#Set the logging files
			$file = "$schemaPath\$schema - $objType.log"
			$global:LogFile = "$rootpath\CSV-LOGS\$schema-$objType-log.csv"
			
			$subObj = $false
			
			#Switch on the Object Type to get which columns to use in finding the object in the source & target databases and which columns of those recordsets to compare
			switch ($objType)
			{
				"CLUSTER" 
				{ 
					$from = "dba_CLUSTERs"
					$where = "owner = '$schema' and CLUSTER_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(2,16, 21, 22, 23, 24)
					$colsWhere = @(0,1)
				}
				"CONSTRAINT"
				{
					$from = "dba_CONSTRAINTs"
					$where = "owner = '$schema' and constraint_name = '$($objs[1].ToString())'"
					$colsAnalyze = @(2,3,4,5,6,7,8,9,10,11,12,13,15,16,17,18)
					$colsWhere = @(0,1)
				}
				"CONSUMER GROUP"
				{ 
					$from = "dba_rsrc_CONSUMER_GROUPs"
					$where = "CONSUMER_GROUP = '$($objs[1].ToString())'" 
					$colsAnalyze = @(1,2,3,4)
					$colsWhere = @(0)
				}
				"CONTEXT" 
				{
					$from = "dba_CONTEXT"
					$where = "owner = '$schema' and NAMESPACE = '$($objs[1].ToString())'" 
					$colsAnalyze = @(1,2,3,4)
					$colsWhere = @(0)
				}
				"DATABASE LINK" 
				{ 
					$from = "dba_db_LINKs"
					$where = "owner = '$schema' and DB_LINK = '$($objs[1].ToString())'"
					$colsAnalyze = @(2,3)
					$colsWhere = @(0,1)
				}
				"DIRECTORY" 
				{ 
					$from = "dba_DIRECTORies"
					$where = "owner = '$schema' and DIRECTORY_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(2)
					$colsWhere = @(0,1)
				}
				"EVALUATION CONTEXT" 
				{ 
					$from = "dba_EVALUATION_CONTEXTs"
					$where = "owner = '$schema' and EVALUATION_CONTEXT_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(2,3)
					$colsWhere = @(0,1)
				}
				"FUNCTION" 
				{ 
					$from = "dba_procedures"
					$where = "owner = '$schema' and OBJECT_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(7,8,9,10,11,12,13,14)
					$colsWhere = @(0,1,6)
				}
				"INDEX" 
				{ 
					$from = "dba_INDEXes"
					$where = "owner = '$schema' and INDEX_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(2,3,4,5,6,7,8,22,29,35,36,37,38,39,45,49,50,51)
					$colsWhere = @(0,1)
					
				}
				"INDEX COLUMN"
				{
					$from = "dba_IND_COLUMNs"
					$where = "index_owner = '$schema' and INDEX_NAME = '$($objs[1].ToString())' and COLUMN_NAME = '$($objs[2].ToString())'"
					$colsAnalyze = @(2,3,5,6,7,8)
					$colsWhere = @(0,1,4)
					$subObj = $true
				}
				"INDEX PARTITION" 
				{ 
					$from = "dba_IND_PARTITIONs" 
					$where = "owner = '$schema' and index_name = '$($objs[1].ToString())' and partition_name = '$($objs[2].ToString())'" 
					$colsAnalyze = @(2,8,9,20,21,31,32,34)
					$colsWhere = @(0,1,3)
					#$subObj
				}
				"INDEXTYPE" 
				{ 
					$from = "dba_INDEXTYPEs"
					$where = "owner = '$schema' and INDEXTYPE_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(2,3,4,5,6,7,8)
					$colsWhere = @(0,1)
				}
				"JAVA CLASS" 
				{ 
					$from = "dba_JAVA_CLASSes"
					$where = "owner = '$schema' and NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(2,3,4,5,6,7,8,9,10,11,12)
					$colsWhere = @(0,1)
				}
				"JOB" 
				{ 
					$from = "dba_scheduler_JOBs"
					$where = "owner = '$schema' and JOB_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(2,3,4,5,6,7,8,9,10,11,12,13,15,16,17,18,19,21,22,23,24,25,26,37,38,39,40,41,42,43,44,45,46)
					$colsWhere = @(0,1)
				}
				"JOB CLASS" 
				{ 
					$from = "dba_scheduler_job_classes"
					$where = "owner = '$schema' and JOB_CLASS_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(1,2,3,4,5)
					$colsWhere = @(0)
				}
				"LIBRARY" 
				{ 
					$from = "dba_LIBRARies"
					$where = "owner = '$schema' and LIBRARY_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(2,3,4)
					$colsWhere = @(0,1)
				}
				"LOB" 
				{ 
					$from = "dba_LOBs"
					$where = "owner = '$schema' and SEGMENT_NAME = '$($objs[1].ToString())'" 
					$colsAnalyze = @(4,5,10,11,12,13,14)
					$colsWhere = @(0,1,2,3)
				}
				"LOB PARTITION" 
				{ 
					$from = "dba_LOB_PARTITIONs" 
					$where = "owner = '$schema' and lob_name = '$($objs[1].ToString())' and partition_name = '$($objs[2].ToString())'"
					$colsAnalyze = @(3,5,6,7,8,9,12,13,14,22,23)
					$colsWhere = @(0,1,4)
					$subObj = $true
				}
				"OBJECT DEFINITION"
				{
					$from = "dba_OBJECTs"
					$where = "owner = '$schema' and object_name = '$($objs[1].ToString())' and coalesce(subobject_name, 'NULL') = coalesce('$($objs[2].ToString())', 'NULL') and object_type = '$($objs[5].ToString())'"
					$colsAnalyze = @(9,10,11,12)
					$colsWhere = @(0,1,2,5)
				}
				"OPERATOR" 
				{ 
					$from = "dba_OPERATORs"
					$where = "owner = '$schema' and OPERATOR_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(2)
					$colsWhere = @(0,1)
				}
				"PACKAGE"
				{ 
					$from = "dba_SOURCE"
					$where = "owner = '$schema' and name = '$($objs[1].ToString())' and type = '$($objTYPE)'"
					$colsAnalyze = @(4)
					$colsWhere = @(0,1,2,3)
					$subObj = $true
				}
				"PACKAGE BODY" 
				{ 
					$from = "dba_SOURCE"
					$where = "owner = '$schema' and name = '$($objs[1].ToString())' and type = '$($objTYPE)'" 
					$colsAnalyze = @(4)
					$colsWhere = @(0,1,2,3)
					$subObj = $false
				}
				"PROCEDURE" 
				{ 
					$from = "dba_PROCEDUREs"
					$where = "owner = '$schema' and OBJECT_NAME = '$($objs[1].ToString())' and OBJECT_TYPE = '$($objTYPE)'"
					$colsAnalyze = @(2,7,8,9,10,11,12,13)
					$colsWhere = @(0,1,2,4,5,6)
				}
				"PROGRAM" 
				{ 
					$from = "dba_scheduler_PROGRAMs"
					$where = "owner = '$schema' and PROGRAM_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(2,3,4,5,6,7)
					$colsWhere = @(0,1)
				}
				"QUEUE" 
				{ 
					$from = "dba_QUEUEs" 
					$where = "owner = '$schema' and NAME = '$($objs[1].ToString())'" 
					$colsAnalyze = @(2,4,7,8,9,10)
					$colsWhere = @(0,1)
				}
				"RESOURCE PLAN" 
				{ 
					$from = "dba_rsrc_plans" 
					$where = "owner = '$schema' and PLAN = '$($objs[1].ToString())'" 
					$colsAnalyze = @(1,2,3,4,5,6,7,8)
					$colsWhere = @(0)
				}
				"RULE" 
				{ 
					$from = "dba_RULEs" 
					$where = "owner = '$schema' and RULE_NAME = '$($objs[1].ToString())'" 
					$colsAnalyze = @(2,3,4,5,6)
					$colsWhere = @(0,1)
				}
				"RULE SET" 
				{ 
					$from = "dba_RULE_SETs" 
					$where = "owner = '$schema' and RULE_SET_NAME = '$($objs[1].ToString())'" 
					$colsAnalyze = @(2,3,4)
					$colsWhere = @(0,1)
				}
				"SCHEDULE" 
				{ 
					$from = "dba_scheduler_SCHEDULEs" 
					$where = "owner = '$schema' and SCHEDULE_NAME = '$($objs[1].ToString())'" 
					$colsAnalyze = @(2,4,5,6,7,8,10)
					$colsWhere = @(0,1)
				}
				"SEQUENCE" 
				{ 
					$from = "dba_SEQUENCEs" 
					$where = "sequence_owner = '$schema' and SEQUENCE_NAME = '$($objs[1].ToString())'" 
					$colsAnalyze = @(2,3,4,5,6)
					$colsWhere = @(0,1)
				}
				"SYNONYM" 
				{ 
					$from = "dba_SYNONYMs" 
					$where = "owner = '$schema' and SYNONYM_NAME = '$($objs[1].ToString())'" 
					$colsAnalyze = @(2,3,4)
					$colsWhere = @(0,1)
				}
				"TABLE" 
				{ 
					$from = "dba_TABLEs" 
					$where = "owner = '$schema' and TABLE_NAME = '$($objs[1].ToString())'" 
					$colsAnalyze = @(2,3,4,5,17,18,29,30,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48)
					$colsWhere = @(0,1)
					
				}
				"TABLE COLUMN"
				{
					$from = "dba_TAB_COLUMNs" 
					$where = "owner = '$schema' and TABLE_NAME = '$($objs[1].ToString())'" 
					$orderby = " order by owner, table_name, column_id"
					$colsAnalyze = @(3,4,5,6,7,8,9,10,11,12,21,22,23,24,26,27,28,29,30)
					$colsWhere = @(0,1,2)
					$subObj = $false
				}
				"TABLE PARTITION" 
				{ 
					$from = "dba_TAB_PARTITIONs" 
					$where = "owner = '$schema' and table_name = '$($objs[1].ToString())' and partition_name = '$($objs[2].ToString())'" 
					$colsAnalyze = @(2,4,8,20,21,30,31,32)
					$colsWhere = @(0,1,3)
					$subObj = $true
				}
				"TRIGGER" 
				{ 
					$from = "dba_TRIGGERs"
					$where = "owner = '$schema' and TRIGGER_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(2,3,4,5,6,7,8,9,10,11,12,13)
					$colsWhere = @(0,1)
				}
				"TYPE" 
				{ 
					$from = "dba_SOURCE" 
					$where = "owner = '$schema' and name = '$($objs[1].ToString())' and type = '$($objTYPE)'" 
					$colsAnalyze = @(4)
					$colsWhere = @(0,1,2,3)
				}
				"TYPE BODY" 
				{ 
					$from = "dba_SOURCE" 
					$where = "owner = '$schema' and name = '$($objs[1].ToString())' and type = '$($objTYPE)'" 
					$colsAnalyze = @(4)
					$colsWhere = @(0,1,2,3)
					$subObj = $true
				}
				"VIEW" 
				{ 
					$from = "dba_VIEWs"
					$where = "owner = '$schema' and VIEW_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(2,3,4,5,6,7,8,9,10)
					$colsWhere = @(0,1)
				}
				"WINDOW" 
				{ 
					$from = "dba_scheduler_WINDOWs"
					$where = "owner = '$schema' and WINDOW_NAME = '$($objs[1].ToString())'"
					$colsAnalyze = @(1,2,3,4,6,8,9,12,13,14,15,16)
					$colsWhere = @(0)
				}
				"WINDOW GROUP" 
				{ 
					$from = "dba_scheduler_WINDOW_GROUPs" 
					$where = "owner = '$schema' and WINDOW_GROUP_NAME = '$($objs[1].ToString())'" 
					$colsAnalyze = @(1,2,4)
					$colsWhere = @(0)
				}
				Default
				{
					$skip = $true
				}
			}
			
			#Populate variables with needed information
			$objName = $objs[1].ToString()
			$global:ObjectName = $objName
			$global:subObjName = " "
			
			#If the object has a subObj then add that to the ObjName
			if ($subObj)
			{
				$objName = "$objName - $($objs[2].ToString())"	
				$global:subObjName = $objs[2].ToString()
			}
			
			add-content $file "`tAnalyzing $objType - $schema.$objName"
			
			#Determine number of records this object has. This is mainly useful for records in DBA_Source
			$sql = "select count(*) RECS from $from where $where"
			$srcCount = Get-DataReader $srcConn $sql
			if ($srcCount.HasRows)
			{
				$srcCount.read() | out-null
				$records = [int]$srcCount[0]
			}
			else
			{
				$records = 0
			}
			
			#Clean up variables
			$srcCount.Close()
			$srcCount.Dispose()
			
			#Initialize paging variables
			#Paging is necessary to not exceed PowerShells memory limitations 
			$min = 1
			$max = $page
			$analyzed = 0
			
			$srcObjs = 0
			
			if ($from -eq "dba_SOURCE") #This code dramatically increases the speed of comparing these objects
			{
				if ($objName.EndsWith(" - ")) { $objName = $objName -replace ".{3}$" }
				
				$pct = [math]::floor(($objAnalyzed/$objRecords)*100)
				write-progress -Activity "Analyzing - $schema - $ObjectTypes" -Status "Analyzed $objAnalyzed of $objRecords - $pct%" -id $progressID -parentID 0 #-PercentComplete $pct
				
				$linesAnalyzed = 0
				while ($min -le $records) #begin looping through Page sized chunks of lines
				{
					$sql = "select * from $from where $where and line between $min and $max order by owner, name, line" #SQL will return the same recordset in both database
					$global:src = Get-DataReader $srcConn $sql #populate recordset
					$global:trg = Get-DataReader $trgConn $sql #populate recordset
					
					if (!($global:trg.HasRows)) #Check if records were returned from Target
					{
						add-content $file "`t`tError: Object not found in $($global:trgDB) - $($global:src.Item(1)) - $($global:src.Item(3))"
						$global:failure = Add-Result $objType $global:failure $schema
						write-log-entry $global:LogFile $schema $global:ObjectName $global:subObjName $objType $false "Record not found in target" "src" $null
						break
					}
					else
					{
						$srcRead = $global:src.Read(); $trgRead = $global:trg.Read(); #Read in the next row from both recordsets
						
						while ($srcRead -and $trgRead) #While these variables are true it means data was successfully read
						{
							$global:SubObjName = "$($global:src[3].ToString())"
							$status = Analyze-Records $colsAnalyze $file $objType $schema $objName #Analyze the records for differences
							
							$srcObjs += 1 #increment
							
							if ($srcObjs % 100 -eq 0)
							{
								add-content $file "`t`tAnalyzed: $srcObjs"
							}
							$linesAnalyzed++
							$pct = [math]::floor(($linesAnalyzed/$records)*100)
							write-progress -Activity "Analyzing - $schema - $ObjectTypes - $objName" -Status "Analyzed $linesAnalyzed of $records - $pct%" -id ($progressID+200) -parentID $progressID
							
							$srcRead = $global:src.Read(); $trgRead = $global:trg.Read(); #read the next line from both recordsets
						}
						
						if ($srcRead -ne $trgRead) #check if one recordset ran out of data before the other and report if so
						{
							add-content $file "`t`tError: Number of Lines different between DBs"
							$global:failure = add-result $objType $global:failure $schema
							write-log-entry $global:LogFile $schema $global:ObjectName $global:SubObjName $objType $false "Number of lines different between databases" $null $null
							break
						}
					}
					#Increment the paging variables
					$min += $page
					$max += $page
					
					#Cleanup objects
					$global:src.close()
					$global:src.dispose()
					
					$global:trg.close()
					$global:trg.dispose()
				}
				write-progress -Activity "Analyzing - $schema - $ObjectTypes - $objName" -Status "Analyzed $linesAnalyzed of $records - $pct%" -id ($progressID+200) -parentID $progressID -completed
				$objAnalyzed++
			}
			elseif ($objType -eq "TABLE COLUMN") #This code dramatically increases the speed of compare Tables with a large number of columns
			{
				while ($min -le $records) #Begin looping through record set
				{
					#Build the sql to select the page sized record set of columns from a given table
					$sql = "select * from (select a.*, rownum rnum from (select * from $from where $where$orderby) a) where rnum between $min and $max"
					$global:src = Get-DataReader $srcConn $sql
					$global:trg = Get-DataReader $trgConn $sql
					
					if (!($global:trg.HasRows)) #Ensure records were returned from the target
					{
						add-content $file "`t`tError: Columns not found in $($global:trgDB)`r`n$sql"
						$global:failure = Add-Result $objType $global:failure $schema
						write-log-entry $global:LogFile $schema $global:ObjectName $global:subObjName $objType $false "Record not found in target" "src" $null
						break
					}
					else
					{
						$srcRead = $global:src.Read(); $trgRead = $global:trg.Read();
						
						while ($srcRead -and $trgRead) #If either return Read() = False, leave the loop as this means no more data
						{
							#$objName = $objs[1].ToString()
							#$global:ObjectName = $objName
							#$objName = "$objName - $($objs[2].ToString())"
							$global:SubObjName = $global:src[2].ToString() #Set the SubObj name = to the Column Name
							$status = Analyze-Records $colsAnalyze $file $objType $schema $objName #Analyze the Columns
							
							$srcObjs += 1
							
							if ($srcObjs % 100 -eq 0)
							{
								add-content $file "`t`tAnalyzed: $srcObjs"
							}
							$objAnalyzed++
							$pct = [math]::floor(($objAnalyzed/$objRecords)*100)
							write-progress -Activity "Analyzing - $schema - $ObjectTypes" -Status "Analyzed $objAnalyzed of $objRecords - $pct%" -id $progressID -parentID 0
							$srcRead = $global:src.Read(); $trgRead = $global:trg.Read();
						}
						
						if ($srcRead -ne $trgRead) #If one recordset ran out of records before the other the values won't match
						{
							add-content $file "`t`tError: Number of Lines different between DBs"
							$global:failure = add-result $objType $global:failure $schema
							write-log-entry $global:LogFile $schema $global:ObjectName $global:SubObjName $objType $false "Number of lines different between databases" $null $null
							break
						}
					}
					
					#Increment the paging variables
					$min += $page
					$max += $page
					
					#Clean up objects
					$global:src.close()
					$global:src.dispose()
					
					$global:trg.close()
					$global:trg.dispose()
				}
			}
			else #All other object types have to be compared in the following much slower action fashion
			{
				while ($min -le $records) #Begin looping through page sized sets of records
				{
					#Select the page sized record from the Object Types table (eg dba_tab_columns, dba_views)
					$sql = "select * from (select a.*, rownum rnum from $from a where $where) where rnum between $min and $max"# and rownum < 6"
					$global:src = Get-DataReader $srcConn $sql
					
					if ($global:src.HasRows) #If records were returned from source, begin analyzing
					{
						while ($global:src.read()) #while records exist
						{
										
							$sql = Build-SQL $from $colsWhere #Build SQL necessary to get records from target
							$global:trg = Get-DataReader $trgConn $sql #get records from target
							$x = 0
							if ($global:trg.HasRows) #Check that records where returned
							{
								while ($global:trg.read()) #while records exist in set, analyze them
								{
									$status = Analyze-Records $colsAnalyze $file $objType $schema $objName
								}
							}
							else #Log that no records were found
							{
								add-content $file "`t`tError: Object not found in $($global:trgDB)"
								$global:failure = Add-Result $objType $global:failure $schema
								write-log-entry $global:LogFile $schema $global:ObjectName $global:subObjName $objType $false "Record not found in target" "src" $null
							}
							
							$srcObjs++
						
							if ($srcObjs % 100 -eq 0)
							{
								add-content $file "`t`tAnalyzed: $srcObjs"
							}
							
							#Clean up objects
							$global:trg.close()
							$global:trg.dispose()
							
							$objAnalyzed++
							$pct = [math]::floor(($objAnalyzed/$objRecords)*100)
							write-progress -Activity "Analyzing - $schema - $ObjectTypes" -Status "Analyzed $objAnalyzed of $objRecords - $pct%" -id $progressID -parentID 0 #-PercentComplete $pct
						}
					}
					
					#Increment paging variables
					$min += $page
					$max += $page
					
					#Clean up objects
					$global:src.close()
					$global:src.dispose()
				}	
			}
			
			if ($srcObjs -gt 1 -and $srcobjs % 100 -ne 0)
			{
				add-content $file "`t`tAnalyzed: $srcObjs"
			}
		}
		
		#Increment paging variables
		$objMin += $page
		$objMax += $page
		
		#Clean up objects
        $objs.Close()
        $objs.Dispose()
	}
	
	#Clean up objects
	$global:src.close()
	$global:src.dispose()
	
	$global:trg.close()
	$global:trg.dispose()
	
	$trgConn.close()
	$trgConn.dispose()
	$trgConn.ConnectionString = $trgConnStr #Disposing a connection variable deletes the connection string
	
    $file = "$rootPath\Summary_$schema.log"
	
	#Write the results of the analysis
	Write-Results $file $global:failure "Errors" $schema
    Write-Results $file $global:success "Successes" $schema
	
	$end = get-date
	$x = $end - $start
    #add-content $file "`r`n`tExecution Time: $x"
	
	#Clean up remain objects
    $objs.dispose()
    $objs.close()
    
    $global:src.dispose()
    $global:src.close()
    
    $srcConn.dispose()
    $srcConn.close()
    
    $global:trg.dispose()
    $global:trg.close()
    
    $trgConn.dispose()
    $trgConn.close()
    
    $threadMsgs.Add("Completed: $schema - $objectTypes") | out-null
	write-progress -activity "Anaylyzing $schema - $objType" -status "Analyzed $objAnalyzed of $objRecords - $pct%" -id $progressID -parentID 0 -completed
	
	[System.GC]::Collect()
} #end SB

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

#Create the Connection Objects
$srcConn = New-Object System.Data.OleDb.OleDbConnection
$trgConn = New-Object System.Data.OleDb.OleDbConnection

#Set the Connection String
$srcConn.ConnectionString = "User ID=$srcUser;password=$srcUnenc;Data Source=$srcDB;Provider=OraOLEDB.Oracle"
$trgConn.ConnectionString = "User ID=$trgUser;password=$trgUnenc;Data Source=$trgDB;Provider=OraOLEDB.Oracle"

$srcConnStr = $srcConn.ConnectionString
$trgConnStr = $trgConn.ConnectionString

#Set the base path for writing log files
if ($logPath -eq "")
{
    $logPath = [Environment]::GetFolderPath("Desktop") 
}
else 
{
    if ($logPath.EndsWith("\") -or $logPath.EndsWith("/")) { $logPath = $logPath -replace ".{1}$" }
}
$rootPath = "$logPath\Schema_Analysis_v2-$(get-date -f 'yyyyMMddHHmmss')"

#create the path if it does not exist
New-Path $logPath
New-Path $rootPath
New-Path "$rootPath\CSV-LOGS"

#Get the number of Processors to determine if less than 4 threads should be started
#It is very easy to run into the Maximum Number of Cursors for oracle with this script, thus why 4 is the maximum
if ($threads -eq 0)
{
	#Determine the number of Cores on the executing machine
	$processor = get-wmiobject win32_processor;
	[int]$procs = $processor.NumberofLogicalProcessors
	if ($procs -gt 4) { $procs = 4 }
	$threads = $procs
}

#Instantiate the variable necessary for cross-thread communication
$threadMsgs = New-Object System.Collections.ArrayList

#Add the variable to the SessionState
$sessionState = [system.management.automation.runspaces.initialsessionstate]::CreateDefault()
$sessionState.Variables.Add((New-Object System.Management.Automation.Runspaces.SessionStateVariableEntry('threadMsgs', $threadMsgs, $null)))

#Create the pool of thread
$Pool = Get-RunspacePool $threads -SessionState $sessionState

#Create the pipline the pool will run in
$pipeline  = [System.Management.Automation.PowerShell]::create()
$pipeline.RunspacePool = $pool

$pipes = @()

$start = get-date
[string]$querySchema = ""

#Create an Array of Schemas for loops
$schemas = $schemas -replace " ", "" 
$schemas = $schemas.ToUpper() -split ","

foreach ($schema in $schemas) #Begin building the where clause of the schemas that need tested
{   
    $querySchema = "$querySchema'$schema'"    
}

$querySchema = $querySchema -replace "''", "','" #Insert commas between ''s

if ($excludeTypes) #if the objects specified in ObjectTypes are meant to be excluded
{
    $excludeTypes = "not "
}
else
{
    $excludeTypes = ""
}

if ($ObjectTypes -ne "") #Modify value of ObjectTypes if comma delimited list be adding 's and building where clause
{
	$ObjectTypes = $ObjectTypes -replace ", ",","
	$ObjectTypes = $ObjectTypes -replace " ,",","
	$ObjectTypes = $ObjectTypes.ToUpper() -replace ",","','"
	$ObjectTypes = " where OBJECT_TYPE $($excludeTypes)in ('$ObjectTypes')`r`n"
}
else
{
	$ObjectTypes = " "
}

#Build SQL to return objects of the schemas
$sql = "select distinct owner, object_type, case object_type when 'PACKAGE BODY' then 3 when 'TABLE COLUMN' then 1 else 2 end Priority, case owner ||'.'|| object_type when 'IFS.PACKAGE BODY' then 0 else 1 end OVRD_Priority`r`n"
$sql = "$sql from ("
$sql = "$sql select distinct owner, object_type from dba_objects where owner in ($querySchema)`r`n"
$sql = "$sql union all`r`n"
$sql = "$sql select distinct owner, 'OBJECT DEFINITION' OBJECT_TYPE from dba_objects where owner in ($querySchema)`r`n"
$sql = "$sql union all`r`n"
$sql = "$sql select distinct owner, 'TABLE COLUMN' OBJECT_TYPE from dba_tab_columns where owner in ($querySchema)`r`n"
$sql = "$sql union all`r`n"
$sql = "$sql select distinct index_owner, 'INDEX COLUMN' OBJECT_TYPE from dba_ind_columns where index_owner in ($querySchema)`r`n"
$sql = "$sql union all`r`n"
$sql = "$sql select distinct owner, 'CONSTRAINT' from dba_constraints where owner in ($querySchema))`r`n"
$sql = "$sql$objectTypes" #if no Types are specifed then all are utilized and $objectTypes = ""
$sql = "$sql order by OVRD_Priority, priority, owner, object_type`r`n"

write-progress -Activity "Performing Schema Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Retrieving List of Schema & Objects from $($srcDB.ToUpper())" -id 0

$objs = Get-DataReader $srcConn $sql



$x = 1
if ($objs.HasRows) #if there records are returned
{
	$records = 0
	while ($objs.read()) { $records++; }
	$objs.Close()
	$objs.Dispose()
	$objs = Get-DataReader $srcConn $sql
	
    while ($objs.Read()) #create folders and threads for all schemas returned by the query
    {        
        $schema = [string]$objs[0].ToString()
        $objType = [string]$objs[1].ToString()
 
    	$schemaPath = "$rootpath\$schema"
    	New-Path $schemaPath
        
        write-progress -Activity "Performing Schema Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Queuing Threads: $x of $records" -id 0
        $pipes += Invoke-Async -RunspacePool $pool -ScriptBlock $sb -Parameters $srcConnStr, $trgConnStr, $schema, $objType, $rootPath, $srcDB, $trgDB, $ScriptPath, $page, $x
		$x++
    }
}

Write-Progress -Activity "Performing Schema Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Completed: $i of $($Pipes.Length) - $pct%" -id 0

write-host "`r`n`tALL THREADS QUEUED, PLEASE BE PATIENT WHILE THEY EXECUTE"

#Clean up objects
$objs.close()
$objs.dispose()

$srcConn.close()
$srcConn.dispose()

$trgConn.close()
$trgConn.dispose()

#Begin watching for all threads to complete
Watch-Messages $pipes $srcDB $trgDB

#Create & Write the headers to the consolidated log file
$file = "$rootpath\SchemaAnalysis-Log.csv"
write-log-headers $file $srcDB $trgDB

Join-CSVlogs "$rootPath\CSV-LOGS" $file

Write-Host "`r`n------ Analysis Complete ------"
Write-Host "`r`nAccess Log Files At:`r`n$rootPath`r`n"

$end = get-date
$x = $end - $start
Write-host "`r`n`tExecution Time: $x"

#Clean up objects
foreach ($pipe in $pipes)
{
	$pipe.pipeline.dispose()
}
$pool.close()
$pool.dispose()
[System.GC]::Collect()