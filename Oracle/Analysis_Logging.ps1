#Writes Headers to the Specified File, checking if the file exists first and if so, writes the headers in such a way that they are always first
function Write-Log-Headers ($file, $srcDB, $trgDB)
{	
    $header = "`"DateTime`",`"Schema`",`"Object`",`"Sub-Object`",`"Object Type`",`"Result`",`"Message`",`"$srcDB`",`"$trgDB`"" #Sets the CSV Headers
    
    $exists = test-path $file #Checks if the file Exists
    
    if ($exists -eq $true) #If the file does exist, get the file's content, set the file's content to the headers, then write the content back in
    { 
        $line = Get-Content $file -totalcount 1; #Gets the first line of the file
        
        if ($header -ne $line) #If the first line of the file isn't the headers
        {
            $content = get-content $file #get the file's content
            set-content $file $header #overwrite the file's content with the headers, wiping out it's current contents
            foreach ($line in $content) #writes the content of the file back, line by line
            {
                add-content $file $line
            }
        }
    }
    else #If the file doesn't exist write the headers
    {
        add-content $file $header
    }
}

#Writes an entry into the file specified
function Write-Log-Entry ($file, $schema, $objName, $subObj, $objType, $result, $msg, $src, $trg)
{
	#Expands the source and target rows into a write-able format
    if ($src -ne $null) { $srcData = Expand-Data $src }
    if ($trg -ne $null) { $trgData = Expand-Data $trg }
    
	#Replaces "s with 's so that the CSV formatting isn't compromised
    $srcData = $srcData -replace "`"", "'"
    $trgData = $trgData -replace "`"", "'"
    $msg = $msg -replace "`"","'"
    
	#Collects what's going to be written into a variable
    $write = "`"$(get-date)`",`"$schema`",`"$objName`",`"$subObj`",`"$objType`",`"$result`",`"$msg`",`"$srcData`",`"$trgData`""
    
	#Write the data to the file
    add-content $file $write
}

#Expands the passed in data row
Function Expand-Data
{
	[CmdletBinding()]
	Param(
		[Parameter(Mandatory=$true)]$dr
	)
	
	$data = ""
	
	#Check for the Datatype
    if ($dr.GetType().Name -eq "DataRow")
    {
		$delim = ""
		#ItemArray is the collection of columns in the provided DataRow
        foreach ($val in $dr.ItemArray)
        {
            #Consolidate the column values with a | delimiter
			$data = "$data$delim$val"
			$delim = "|"
        }
    }
    elseif ($dr.GetType().Name -eq "String") #When using DataReader, the individual rows cannot be passed to functions and are stored in Global variables
    {
		if ($dr -eq "src")
		{
			$dr = $global:src
		}
		elseif ($dr -eq "trg")
		{
			$dr = $global:trg
		}
		
		#Consolidate the records from the DataReader
        for ($i = 0; $i -lt $dr.FieldCount; $i++)
        {
            if ($i -eq 0)
            {
				$data = "$($dr.Item($i))"
            }
            else
            {
				$val = "$($dr.Item($i))"
                $data = "$data|$val"
            }
        }
    }
	#Replace Carriage-Returns and Line-Feeds with "" to protect CSV Formatting
    $data = $data -replace "`r", ""
	$data = $data -replace "`n", ""
	
    return $data
}

Function Join-CSVlogs
{
	Param
    (
        [string]$logPath,
		[string]$file
    )
	
	#Get the list of CSV-Logs that were created
	$files = get-childitem "$logPath\*"
	$i = 0
	$pct = [math]::floor(($i/$files.length)*100)
	Write-Progress -Activity "Consolidating CSV Logs" -Status "Completed: $i of $($files.Length) - $pct%" -id 0
	if ($files -ne $null)
	{
		foreach ($f in $files) #begin looping through the CSV Files
		{
			Get-Content $f | Add-Content $file #Write the file contents to the central log file
			$i++
			$pct = [math]::floor(($i/$files.length)*100)
			Write-Progress -Activity "Consolidating CSV Logs" -Status "Completed: $i of $($files.Length) - $pct%" -id 0
		}
		Write-Progress -Activity "Consolidating CSV Logs" -Status "Completed: $i of $($files.Length) - $pct%" -id 0 -completed
	}
	else
	{
		write-host "No files found in $logPath"
	}
}