Function Start-DataCollector
{
	[Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)]$DataCollectorName
    )

    $datacollectorset = New-Object -COM Pla.DataCollectorSet
    $datacollectorset.Query("$DataCollectorName",$null)
    $datacollectorset.start($false)

    Start-Sleep 10
}

Function Stop-DataCollector
{
	[Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)]$DataCollectorName
    )

    $datacollectorset = New-Object -COM Pla.DataCollectorSet
    $datacollectorset.Query("$DataCollectorName",$null)
    $datacollectorset.stop($false)

    Start-Sleep 10
}

Function Start-BITSync
{
	[Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)]$BITCFG
    )

    # Self-elevate the script if required
    $BITPath = "c:\program files\burnintest\bit.exe"
    $BITargs = "/x /r /c \`"$BITCFG\`""

    if (-Not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] 'Administrator')) 
    {
        if ([int](Get-CimInstance -Class Win32_OperatingSystem | Select-Object -ExpandProperty BuildNumber) -ge 6000) 
        {
            Start-Process -FilePath $BITPath -ArgumentList $BITargs -Verb Runas -Wait
        }
    }
}

Function Start-BITAsync
{
	[Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)]$BITCFG
    )
    # Self-elevate the script if required
    $BITPath = "c:\program files\burnintest\bit.exe"
    $BITargs = "/x /r /c \`"$BITCFG\`""

    if (-Not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] 'Administrator')) 
    {
        if ([int](Get-CimInstance -Class Win32_OperatingSystem | Select-Object -ExpandProperty BuildNumber) -ge 6000) 
        {
            Start-Process -FilePath $BITPath -ArgumentList $BITargs -Verb Runas
        }
    }
}