#In the event of a mismatch between the Target & Source, this writes the requisite log messages
function Write-Log
{
    [Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)]$file,
        [Parameter(Position=1,Mandatory=$True)]$msg
    )

    add-content $file $msg
}

#This function creates a new folder in the %temp% directory with a random name, then returns it
function New-TemporaryDirectory 
{
    $parent = [System.IO.Path]::GetTempPath()
    $name = [System.IO.Path]::GetRandomFileName()
    New-Item -ItemType Directory -Path (Join-Path $parent $name)
}


Function New-Path
{
	[Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)]$path
    )
	
	if(!(Test-Path -path $path)){ new-item -itemtype directory -path $path | out-null }
}

#Reads the contents of the provided file (Utilized with Set-Progress and Watch-Files)
#Function retries recursively 10 times and then returns null
Function Get-Iteration
{
	[CmdletBinding()]Param
	(
		[String]$file
	)
    
    if (Test-Path $file -PathType Leaf)
    {
        $Iteration = get-content $file
    }
    else
    {
        $Iteration = 1
        set-content $file 1
    }

    Return [int]$Iteration
}

#Reads the contents of the provided file (Utilized with Set-Progress and Watch-Files)
#Function retries recursively 10 times and then returns null
Function Set-Iteration
{
	[CmdletBinding()]Param
	(
		[String]$file
	)
    
    if (Test-Path $file -PathType Leaf)
    {
        $Iteration = get-content $file
    }

    Set-Content $file [int]$Iteration+1
}

Function Watch-Files
{
	[Cmdletbinding()]
    Param
    (
		[Parameter(Position=3,Mandatory=$True)][string]$commPath,
		[Parameter(Position=3,Mandatory=$True)][string[]]$IPs
    )

}

#writes the summary file of Successes and Failures
Function Get-BITStats
{
	[Cmdletbinding()]
    Param
    (
        [string]$file
    )
	
	$content = Get-Content $file
    $results = [System.Collections.ArrayList]@()

	foreach ($line in $content)
    {
        if ($line -like "Test Start time:*")
        {
            $StartDate = $line.Replace("Test Start time: ","")
        }

        if ($line -like "                      CPU   *" -or
            $line -like "             Memory (RAM)   *" -or
            $line -like "                Disk (C:)   *")
        {
            $results.add($line) | out-null
        }
    }

    $stats = $results | select -property @{name='TestName'; expression={$_.substring(0,28).trim()}},
                                        @{name='Cycles'; expression={$_.substring(28,9).trim()}},
                                        @{name='Operations'; expression={$_.substring(37,16).trim()}},
                                        @{name='Result'; expression={$_.substring(53,7).trim()}},
                                        @{name='Errors'; expression={$_.substring(60,9).trim()}},
                                        @{name='LastError'; expression={$_.substring(69).trim()}}
    
    $date = get-date -date "$($startDate.Substring(4,3)) $($StartDate.Substring(8,2).trim()) $($StartDate.Substring(20)) $($StartDate.Substring(11,8))" -format "MM/dd/yyyy HH:mm:ss"
    $results.Clear()
    $results.add($date)
    $results.add($stats)

    return $results
}