#In the event of a mismatch between the Target & Source, this writes the requisite log messages
function Write-Log($src, $trg, $cols, $file, $sql)
{
	#Replace Carriage Returns & Line Feeds with spaces
	$sql = $sql  -replace "`r`n", " "
	
	#$trg is null then the data was not found in the
	if($trg -eq $null)
	{
		$srcData = ""
		$delim = ""
		for ($x = 0;$x -lt $cols;$x++)
		{
			$srcData = "$srcData$delim$($src[$x])"
			$delim = "|"
		}
		Add-Content $file "`tError: Record Not Found"
		Add-Content $file "`t`t$($srcDB.ToUpper()): $srcData"
		Add-Content $file "`t`tData not found in $trgDB - $sql"
	}
	else #$trg was found, this means that the data didn't match
	{
		$srcData = ""
		$trgData = ""
		$delim = ""
		for ($x = 0;$x -lt $cols;$x++)
		{
			$srcData = "$srcData$delim$($src[$x])"
			$trgData = "$trgData$delim$($trg[$x])"
			$delim = "|"
		}
		Add-Content $file "`tData Mismatch Detected"
		Add-Content $file "`t`t$($srcDB.ToUpper()): $srcData"
		Add-Content $file "`t`t$($trgDB.ToUpper()): $trgData"
		Add-Content $file "`t`tSQL Statement: $sql"
	}
}

#This function gets the list of Primary Keys for the table under test
function Get-PKs($Conn, $tabInfo)
{
	$sql = ""
	$sql = "$($sql)select c.COLUMN_NAME, c.ORDINAL_POSITION-1, c.DATA_TYPE`r`n"#ORDINAL_POSITION-1 handles the DataRow.Item collection indexing at 0
	$sql = "$($sql)from INFORMATION_SCHEMA.TABLES t`r`n"
	$sql = "$($sql)inner join INFORMATION_SCHEMA.COLUMNS c`r`n"
	$sql = "$($sql)on t.TABLE_SCHEMA = c.TABLE_SCHEMA`r`n"
	$sql = "$($sql)and t.TABLE_NAME = c.TABLE_NAME`r`n"
	$sql = "$($sql)inner join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc`r`n"
	$sql = "$($sql)on t.TABLE_SCHEMA = tc.TABLE_SCHEMA`r`n"
	$sql = "$($sql)and t.TABLE_NAME = tc.TABLE_NAME`r`n"
	$sql = "$($sql)inner join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu`r`n"
	$sql = "$($sql)on t.TABLE_SCHEMA = ccu.TABLE_SCHEMA`r`n"
	$sql = "$($sql)and t.TABLE_NAME = ccu.TABLE_NAME`r`n"
	$sql = "$($sql)and tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME`r`n"
	$sql = "$($sql)and c.COLUMN_NAME = ccu.COLUMN_NAME`r`n"
	$sql = "$($sql)where tc.CONSTRAINT_TYPE = 'PRIMARY KEY'`r`n"
	$sql = "$($sql)and t.TABLE_SCHEMA = '$($tabInfo[0])'`r`n"
	$sql = "$($sql)and t.TABLE_NAME = '$($tabInfo[1])'`r`n"
	$sql = "$($sql)order by c.ORDINAL_POSITION"
		
	$PKs = Get-DataTable $Conn $sql
	
	return $PKs
}

#In the case that the table under test does not have a primary key, get all the columns
function Get-All_Cols($Conn, $tabInfo)
{
	$sql = ""
	$sql = "$($sql)Select column_name, ORDINAL_POSITION-1, data_type`r`n" #ORDINAL_POSITION-1 handles the DataRow.Item collection indexing at 0
	$sql = "$($sql)from INFORMATION_SCHEMA.COLUMNS`r`n"
	$sql = "$($sql)where TABLE_SCHEMA = '$($tabInfo[0])'`r`n"
	$sql = "$($sql)and TABLE_NAME = '$($tabInfo[1])'`r`n"
	$sql = "$($sql)order by ORDINAL_POSITION"
	
	$allCols = Get-DataTable $Conn $sql
	
	return $allCols
}

#This function builds the SQL Statement that will select the row under test from the target table based on the columns passed in the $PKs variable
function Get-SQL ($src, $PKs, $table)
{
	$sql = "select * from $table `r`nwhere" #begin building the SQL statement
	
	$and = " " #set to blank and then set to " and " later for multiple criteria
	
	foreach ($PK in $PKs)
	{
		
		$col = [string]$PK[0] #This is the name of the Column
		$position = [int]$PK[1] #This is the position of the column in the table and therefore in the DataTable object
		$type = [string]$PK[2] #This is the datatype of the colummn
		$op = "=" #this is the operation to be performed on the column
		
		$srcVal = [string]$src[$position] -replace "'", "''" #This replaces all ' with doubles so that they are escaped in the SQL Statement that is built
		
		#This switch changes the behavior between NULL and not NULL values
		#The only way to have nulls in this section of code is if all the columns are being utilized for a match in the target DB
		switch ($srcVal)
		{
			#NULL path
			""
			{
				#This switch sets the default value based on type for a null
				switch ($type)
				{
					"datetime"
					{
						$val = "cast('' as datetime)"
					}
					"datetime2"
					{
						$val = "cast('' as datetime2)"
					}
					"bit"
					{
						$val = "0"
					}
					"int"
					{
						$val = "0"
					}
					"smallint"
					{
						$val = "0"
					}
					default
					{
						$val = "''"
					}
				}
				
				$col = "isnull($col, $val)" #This sets the Coalesce so that the SQL Statement ends up with "null" = "null"
			}
			
			#Not NULL path
			default
			{
				#This sets the value based on type, dates are the only special case
				switch ($type)
				{
					"datetime" 
					{ 
						$val = "cast('$srcVal' as datetime)" 
					}
					"datetime2" #Timestamps are very difficult to handle due to database going to a precision of 9 and OLEDB only handling a precision of 3
					{ 
						$op = "between" #Sets the operation to a BETWEEN operator
						
						$date = $src.item($position) #Gets the TimeStamp value
						$month = "$($date.month)".PadLeft(2,'0') #Gets the Month and pads so it's 2 digits
						$day = "$($date.day)".PadLeft(2,'0') #Gets the Day and pads so it's 2 digits
						$year = "$($date.year)" #Gets the Year
						$hour = "$($date.hour)".PadLeft(2,'0') #Gets the Hour and pads so it's 2 digits
						$minute = "$($date.minute)".PadLeft(2,'0') #Gets the Minute and pads so it's 2 digits
						$second = "$($date.second)".PadLeft(2,'0') #Gets the Second and pads so it's 2 digits
						$millisecond = "$($date.millisecond)".PadLeft(3,'0') #Gets the Milliseconds and pads so it's 3 digits
						
						#Build the first date for use in the BETWEEN
						$begin = "$($month)/$($day)/$($year) $($hour):$($minute):$($second).$($millisecond)"
						
						if ($millisecond -eq "999") #Detect if the Millisecond number is 1 millisecond away from the next whole second
						{
							$second = "$($date.second+1)".PadLeft(2,'0') #Increase to the next second and pad
							$millisecond = "000" #Set to 000
						}
						else
						{
							$millisecond = "$($date.millisecond+1)".PadLeft(3,'0') #increase by one millisecond
						}
						
						#Build the second date for use in the BETWEEN
						$end = "$($month)/$($day)/$($year) $($hour):$($minute):$($second).$($millisecond)"
						
						#Build the statement to go to the right of the operator
						$val = "cast('$begin' as datetime2) and cast('$end' as datetime2)"
					}
					default 
					{ 
						$val = "'$srcVal'" 
					}
				}
			}
		}        
		#Add the criteria to the SQL statement
		$sql = "$sql$and$col $op $val"
		#Modify the $and value so that the SQL statement is properly formatted for more than one criteria
		$and = "`r`nand "
	}
	
	return $sql
}

#This function creates a new folder in the %temp% directory with a random name, then returns it
function New-TemporaryDirectory 
{
    $parent = [System.IO.Path]::GetTempPath()
    $name = [System.IO.Path]::GetRandomFileName()
    New-Item -ItemType Directory -Path (Join-Path $parent $name)
}

#This checks if the provided Path exists and, if not, creates it
Function New-Path
{
	[Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)]$path
    )
	
	if(!(Test-Path -path $path)){ new-item -itemtype directory -path $path | out-null }
}

#Writes the provided values to the given file (Utilized with Get-Progress and Watch-Files)
#Function retries recursively 10 times and then silently gives up attempting to write the contents of the file
function Set-Progress 
{
	[CmdletBinding()]Param
	(
		[string]$file,
		[string]$activity,
		[string]$status,
		[int]$id,
		[int]$parent,
		[boolean]$completed = $false,
		[int]$count = 1
	)
	try
	{
		set-content $file "$activity`t$status`t$id`t$parent`t$completed"
	}
	catch
	{
		start-sleep -milliseconds 50
		if ($count -le 10)
		{
			Set-Progress $file $activity $status $id $parent $completed $count++
		}
	}
}

#Reads the contents of the provided file (Utilized with Set-Progress and Watch-Files)
#Function retries recursively 10 times and then returns null
Function Get-Progress
{
	[CmdletBinding()]Param
	(
		[System.Io.FileSystemInfo]$file,
		[int]$count = 1
	)
	$old = $ErrorActionPreference
	$ErrorActionPreference = 'SilentlyContinue'
	try { $content = get-content $file}
	catch 
	{ 
		if ($count -le 10)
		{
			start-sleep -milliseconds 50
			$content = get-progress $file $count++
		}
		else
		{
			$content = $null
		}
	}
	$ErrorActionPreference = $old
	return $content
}

#writes the current progress
Function Watch-Files
{
	[Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)]$Pipes,
		[Parameter(Position=1,Mandatory=$True)][string]$srcDB,
		[Parameter(Position=2,Mandatory=$True)][string]$trgDB,
		[Parameter(Position=3,Mandatory=$True)][string]$commPath
    )
	
	#Sets the status to false for the while statement
	[boolean]$status = $false
	$i = 0
	
	#begin looping while $status is $false
	while(!($status))
	{
		#check for Thread status, if a thread is running, returns $true
		#Get-ThreadComplete is in Threading.psm1
		$status = Get-ThreadsComplete $pipes
		$files = get-childitem "$commPath\*.txt"
		
		#count the number of completed threads
		if ($files -ne $null)
		{
			foreach ($file in $files) 
			{ 
				$msg = Get-Progress $file
				if ($msg -ne $null)
				{
					$msg = $msg -split "`t"
					if ($msg[4] -eq "True")
					{
						$i++
						remove-item $file -force -recurse
						write-progress -Activity $msg[0] -Status $msg[1] -id $msg[2] -parent $msg[3] -Completed
					}
					else
					{
						write-progress -Activity $msg[0] -Status $msg[1] -id $msg[2] -parent $msg[3]
					}
				}
			}
		}
		#Determine % of threads complete and display progress, then sleep for 1 second	
		$pct =[math]::floor(($i/$Pipes.Length)*100)
		Write-Progress -Activity "Performing Data Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Completed: $i of $($Pipes.Length) - $pct%" -id 0 
		Start-Sleep -Seconds 1
	}

	#This causes the progress bar and any children to disappear from the screen
	Write-Progress -Activity "Performing Data Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Completed: $i of $($Pipes.Length) - $pct%" -id 0 -Completed
}

#writes the summary file of Successes and Failures
Function Write-Summary
{
	[Cmdletbinding()]
    Param
    (
        [string]$rootPath,
		[string]$commPath
    )
	
	if (test-path "$commPath\Successes.log") { $successes = get-content "$commPath\Successes.log" }
	if (test-path "$commPath\Failures.log") { $failures = get-content "$commPath\Failures.log" }
	
	#Sort the Successes and Failures
	$successes = $successes | sort-object
	$failures = $failures | sort-object

	#Set the logging file
	$file = "$rootPath\Data_Summary.log"

	Write-Host "`r`n------ Analysis Complete ------"
	Write-Host "`r`nAccess Log Files At:`r`n$rootPath`r`n"

	Add-Content $file "`r`n------TABLES WITH DATA ANALYSIS ERRORS------`r`n"
	#Write-Host "`r`n------TABLES WITH DATA ANALYSIS ERRORS------`r`n"

	#Write the results to both the file and command line
	foreach ($result in $failures)
	{   
		Add-Content $file $result
		#Write-Host $result
	}

	Add-Content $file "`r`n------TABLES WITH NO DATA ANALYSIS ERRORS------`r`n"
	#Write-Host "`r`n------TABLES WITH NO DATA ANALYSIS ERRORS------`r`n"

	#Write the results to both the file and command line
	foreach ($result in $successes)
	{
		Add-Content $file $result
		#Write-Host $result
	}
	
	remove-item "$commPath\Successes.log" -force -recurse
	remove-item "$commPath\Failures.log" -force -recurse
	
}