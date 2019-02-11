Add-Type @'
public class AsyncPipeline
{
    public System.Management.Automation.PowerShell Pipeline ;
    public System.IAsyncResult AsyncResult ;
}
'@

Function Invoke-Async
{
    [Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)]$RunspacePool,
        [Parameter(Position=1,Mandatory=$True)][ScriptBlock]$ScriptBlock,
        [Parameter(Position=2,Mandatory=$False)][Object[]]$Parameters
    )
    
    $Pipeline = [System.Management.Automation.PowerShell]::Create() 

	$Pipeline.RunspacePool = $RunspacePool
	    
    $Pipeline.AddScript($ScriptBlock) | Out-Null
    
    Foreach($Arg in $Parameters) { $Pipeline.AddArgument($Arg) | Out-Null }
    
	$AsyncResult = $Pipeline.BeginInvoke() 
	
	$Output = New-Object AsyncPipeline 
	
	$Output.Pipeline = $Pipeline
	$Output.AsyncResult = $AsyncResult
	
	$Output
}

Function Get-RunspacePool
{
    [Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$true)][int]$PoolSize,
        [Parameter(Position=1,Mandatory=$False)][Switch]$MTA,
        [Parameter(Position=2,Mandatory=$false)]$SessionState
    )
    
    if ($SessionState -ne $null)
    {
        $pool = [RunspaceFactory]::CreateRunspacePool(1, $PoolSize, $SessionState, $Host)
    }
    else
    {
        $pool = [RunspaceFactory]::CreateRunspacePool(1, $PoolSize)
    }
    
    If(!$MTA) { $pool.ApartmentState = "STA" }
    
    $pool.Open()
    
    return $pool
}

Function Receive-AsyncResults
{
    [Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)][AsyncPipeline[]]$Pipelines,
		[Parameter(Position=1,Mandatory=$false)][Switch]$ShowProgress
    )
	
    $i = 1 # incrementing for Write-Progress
	
    foreach($Pipeline in $Pipelines)
    {
		
		try { $Pipeline.Pipeline.EndInvoke($Pipeline.AsyncResult) }
		catch { $_ }
        
		$Pipeline.Pipeline.Dispose()
		
		If($ShowProgress) { Write-Progress -Activity 'Receiving Results' -Status "Percent Complete" -PercentComplete $(($i/$Pipelines.Length) * 100) }
		$i++
    }
}

Function Receive-AsyncStatus
{
    [Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)][AsyncPipeline[]]$Pipelines
    )
    
    foreach($Pipeline in $Pipelines)
    {

	   New-Object PSObject -Property @{
	   		InstanceID = $Pipeline.Pipeline.Instance_Id
	   		Status = $Pipeline.Pipeline.InvocationStateInfo.State
			Reason = $Pipeline.Pipeline.InvocationStateInfo.Reason
			Completed = $Pipeline.AsyncResult.IsCompleted
			AsyncState = $Pipeline.AsyncResult.AsyncState			
			Error = $Pipeline.Pipeline.Streams.Error
       }
	} 
}

Function Get-ThreadsComplete
{
    [Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)][AsyncPipeline[]]$Pipelines
    )
    
    foreach($Pipeline in $Pipelines)
    {
        if ($Pipeline.Pipeline.InvocationStateInfo.State -eq "Running")
        {
            return $false
        }
    }
    
    return $true
}