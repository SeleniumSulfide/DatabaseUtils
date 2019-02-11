
$ScriptPath = Split-Path -parent $MyInvocation.MyCommand.Definition;

$DBPath = "$ScriptPath\DB_Utils.psm1";
$FilePath = "$ScriptPath\FileUtils.psm1";
$PerfPath = "$ScriptPath\Perf_Utils.psm1";

$MyDocuments = [environment]::getfolderpath("mydocuments");
#$PassMarkPath = "$MyDocuments\PassMark\BurnInTest";
$IterationFile = "C:\Iteration.txt";

Import-Module $DBPath;
Import-Module $FilePath;
Import-Modules $PerfPath;

$Machine = "APV-Machine-001" #$env:computername;
$MachineNumber = [int]$Machine.Split("-")[2];


###### THIS NEEDS CHANGED TO THE ACTUAL TARGET ######
$StatConn = Get-SQLConnectionObject "52650-L\SQLEXPRESS", 1433, "BITStats"

#Get iteration
$Iteration = Get-Iteration $IterationFile;
Set-Iteration $IterationFile;

#Check which test to run
switch ($Iteration)
{
    1 { $BITCFG = "C:\25pct.bitcfg"; $BITStats = "c:\25pct.log"; } 

    2 { $BITCFG = "C:\50pct.bitcfg"; $BITStats = "c:\50pct.log"; }

    3 { $BITCFG = "C:\75pct.bitcfg"; $BITStats = "c:\75pct.log"; }

    4 { $BITCFG = "C:\100pct.bitcfg"; $BITStats = "c:\100pct.log"; }
}

Start-DataCollector "VDI_Perf_Test"

Start-BITSync $BITCFG

Stop-DataCollector "VDI_Perf_Test"

$Stats = Get-BITStats $BITStats

Insert-BITStats $StatConn, $Machine, $Iteration, $Stats