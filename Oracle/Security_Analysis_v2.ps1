[CmdletBinding()]
Param(
[Parameter(Mandatory=$true)][string]$srcDB,
[Parameter(Mandatory=$false)][string]$srcUser,
[Parameter(Mandatory=$false)]$srcPass,
[Parameter(Mandatory=$true)][string]$trgDB,
[Parameter(Mandatory=$false)][string]$trgUser,
[Parameter(Mandatory=$false)]$trgPass,
[Parameter(Mandatory=$false)][boolean]$StoredPass = $false,
[Parameter(Mandatory=$false)][string]$LogPath,
[Parameter(Mandatory=$false)][int]$threads, #determines the number of concurrent threads
[Parameter(Mandatory=$false)][string]$Principals
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
		Write-Progress -Activity "Performing Security Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Completed: $i of $($Pipes.Length) - $pct%" -id 0  
		Start-Sleep -Seconds 1
	}
	
	#This causes the progress bar and any children to disappear from the screen
	Write-Progress -Activity "Performing Security Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Completed: $i of $($Pipes.Length) - $pct%" -id 0 -Completed 
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
	[CmdletBinding()]
	Param(
		[System.Data.OleDb.OleDbConnection]$srcConn, #Source OleDbConnection Object
		[System.Data.OleDb.OleDbConnection]$trgConn, #Target OleDbConnection Object
		[string]$srcDB, #Source Database Name
		[string]$trgDB, #Target Database NAme
		[string]$rootPath, #Base logging path
		[string]$ScriptPath, #Path to where the script is being executed
		[string]$grantee, #Grantee that is being analyzed
		[string]$type, #Type of Grantee (User/Role)
		[int]$progressID #INT used in write-progress calls
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
	
	#Analyze definition values for User
	function Analyze-User ($src, $trg, $file)
	{
		$status = $true
		$msgs = @()
		
		if ($src.USER_ID -ne $trg.USER_ID) { $msgs += "`t`tError: User Definition Mismatch - USER_ID`r`n`t`t`t$($srcDB): $($src.USER_ID)`r`n`t`t`t$($trgDB): $($trg.USER_ID)`r`n"; $status = $false; }
		#if ($src.PASSWORD -ne $trg.PASSWORD) { $msgs += "`t`tError: User Definition Mismatch - PASSWORD`r`n`t`t`t$($srcDB): $($src.PASSWORD)`r`n`t`t`t$($trgDB): $($trg.PASSWORD)`r`n"; $status = $false; }
		if ($src.ACCOUNT_STATUS -ne $trg.ACCOUNT_STATUS) { $msgs += "`t`tError: User Definition Mismatch - ACCOUNT_STATUS`r`n`t`t`t$($srcDB): $($src.ACCOUNT_STATUS)`r`n`t`t`t$($trgDB): $($trg.ACCOUNT_STATUS)`r`n"; $status = $false; }
		if ($src.LOCK_DATE -ne $trg.LOCK_DATE) { $msgs += "`t`tError: User Definition Mismatch - LOCK_DATE`r`n`t`t`t$($srcDB): $($src.LOCK_DATE)`r`n`t`t`t$($trgDB): $($trg.LOCK_DATE)`r`n"; $status = $false; }
		if ($src.EXPIRY_DATE -ne $trg.EXPIRY_DATE) { $msgs += "`t`tError: User Definition Mismatch - EXPIRY_DATE`r`n`t`t`t$($srcDB): $($src.EXPIRY_DATE)`r`n`t`t`t$($trgDB): $($trg.EXPIRY_DATE)`r`n"; $status = $false; }
		if ($src.DEFAULT_TABLESPACE -ne $trg.DEFAULT_TABLESPACE) { $msgs += "`t`tError: User Definition Mismatch - DEFAULT_TABLESPACE`r`n`t`t`t$($srcDB): $($src.DEFAULT_TABLESPACE)`r`n`t`t`t$($trgDB): $($trg.DEFAULT_TABLESPACE)`r`n"; $status = $false; }
		if ($src.TEMPORARY_TABLESPACE -ne $trg.TEMPORARY_TABLESPACE) { $msgs += "`t`tError: User Definition Mismatch - TEMPORARY_TABLESPACE`r`n`t`t`t$($srcDB): $($src.TEMPORARY_TABLESPACE)`r`n`t`t`t$($trgDB): $($trg.TEMPORARY_TABLESPACE)`r`n"; $status = $false; }
		if ($src.CREATED -ne $trg.CREATED) { $msgs += "`t`tError: User Definition Mismatch - CREATED`r`n`t`t`t$($srcDB): $($src.CREATED)`r`n`t`t`t$($trgDB): $($trg.CREATED)`r`n"; $status = $false; }
		if ($src.PROFILE -ne $trg.PROFILE) { $msgs += "`t`tError: User Definition Mismatch - PROFILE`r`n`t`t`t$($srcDB): $($src.PROFILE)`r`n`t`t`t$($trgDB): $($trg.PROFILE)`r`n"; $status = $false; }
		if ($src.INITIAL_RSRC_CONSUMER_GROUP -ne $trg.INITIAL_RSRC_CONSUMER_GROUP) { $msgs += "`t`tError: User Definition Mismatch - INITIAL_RSRC_CONSUMER_GROUP`r`n`t`t`t$($srcDB): $($src.INITIAL_RSRC_CONSUMER_GROUP)`r`n`t`t`t$($trgDB): $($trg.INITIAL_RSRC_CONSUMER_GROUP)`r`n"; $status = $false; }
		if ($src.EXTERNAL_NAME -ne $trg.EXTERNAL_NAME) { $msgs += "`t`tError: User Definition Mismatch - EXTERNAL_NAME`r`n`t`t`t$($srcDB): $($src.EXTERNAL_NAME)`r`n`t`t`t$($trgDB): $($trg.EXTERNAL_NAME)`r`n"; $status = $false; }
		
		#These are new columns in version after 10g
		#if ($src.PASSWORD_VERSION -ne $trg.PASSWORD_VERSION) { $msgs += "`t`tError: User Definition Mismatch - PASSWORD_VERSION`r`n`t`t`t$($srcDB): $($src.PASSWORD_VERSION)`r`n`t`t`t$($trgDB): $($trg.PASSWORD_VERSION)`r`n"; $status = $false; }
		#if ($src.EDITIONS_ENABLED -ne $trg.EDITIONS_ENABLED) { $msgs += "`t`tError: User Definition Mismatch - EDITIONS_ENABLED`r`n`t`t`t$($srcDB): $($src.EDITIONS_ENABLED)`r`n`t`t`t$($trgDB): $($trg.EDITIONS_ENABLED)`r`n"; $status = $false; }
		#if ($src.AUTHENTICATION_TAPE -ne $trg.AUTHENTICATION_TAPE) { $msgs += "`t`tError: User Definition Mismatch - AUTHENTICATION_TAPE`r`n`t`t`t$($srcDB): $($src.AUTHENTICATION_TAPE)`r`n`t`t`t$($trgDB): $($trg.AUTHENTICATION_TAPE)`r`n"; $status = $false; }
		#if ($src.PROXY_ONLY_CONNECT -ne $trg.PROXY_ONLY_CONNECT) { $msgs += "`t`tError: User Definition Mismatch - PROXY_ONLY_CONNECT`r`n`t`t`t$($srcDB): $($src.PROXY_ONLY_CONNECT)`r`n`t`t`t$($trgDB): $($trg.PROXY_ONLY_CONNECT)`r`n"; $status = $false; }
		#if ($src.COMMON -ne $trg.COMMON) { $msgs += "`t`tError: User Definition Mismatch - COMMON`r`n`t`t`t$($srcDB): $($src.COMMON)`r`n`t`t`t$($trgDB): $($trg.COMMON)`r`n"; $status = $false; }
		#if ($src.LAST_LOGIN -ne $trg.LAST_LOGIN) { $msgs += "`t`tError: User Definition Mismatch - LAST_LOGIN`r`n`t`t`t$($srcDB): $($src.LAST_LOGIN)`r`n`t`t`t$($trgDB): $($trg.LAST_LOGIN)`r`n"; $status = $false; }
		#if ($src.ORACLE_MAINTAINED -ne $trg.ORACLE_MAINTAINED) { $msgs += "`t`tError: User Definition Mismatch - ORACLE_MAINTAINED`r`n`t`t`t$($srcDB): $($src.ORACLE_MAINTAINED)`r`n`t`t`t$($trgDB): $($trg.ORACLE_MAINTAINED)`r`n"; $status = $false; }

		#If there were errors, write log entries
		if (!($status))
		{
			foreach ($msg in $msgs)
			{
				add-content $file $msg
				write-log-entry $global:LogFile "$($src.USERNAME)" "" "" "USER DEFINITION" $false $msg $src $trg
			}
		}
		else #write log entry of success
		{
			write-log-entry $global:LogFile "$($src.USERNAME)" "" "" "USER DEFINITION" $true "NO ERRORS FOUND" $src $trg
		}
		
		return $status
	}

	#Analyze the Role definition
	function Analyze-Role ($src, $trg, $file)
	{
		$status = $true
		$msgs = @()
		
		if ($src.PASSWORD_REQUIRED -ne $trg.PASSWORD_REQUIRED) { $msgs += "`t`tError: Role Definition Mismatch - PASSWORD_REQUIRED`r`n`t`t`t$($srcDB): $($src.PASSWORD_REQUIRED)`r`n`t`t`t$($trgDB): $($trg.PASSWORD_REQUIRED)`r`n"; $status = $false; }
		
		#These are new columns in version after 10g
		#if ($src.AUTHENTICATION_TYPE -ne $trg.AUTHENTICATION_TYPE) { $msgs += "`t`tError: Role Definition Mismatch - AUTHENTICATION_TYPE`r`n`t`t`t$($srcDB): $($src.AUTHENTICATION_TYPE)`r`n`t`t`t$($trgDB): $($trg.AUTHENTICATION_TYPE)`r`n"; $status = $false; }
		#if ($src.COMMON -ne $trg.COMMON) { $msgs += "`t`tError: Role Definition Mismatch - COMMON`r`n`t`t`t$($srcDB): $($src.COMMON)`r`n`t`t`t$($trgDB): $($trg.COMMON)`r`n"; $status = $false; }
		#if ($src.ORACLE_MAINTAINED -ne $trg.ORACLE_MAINTAINED) { $msgs += "`t`tError: Role Definition Mismatch - ORACLE_MAINTAINED`r`n`t`t`t$($srcDB): $($src.ORACLE_MAINTAINED)`r`n`t`t`t$($trgDB): $($trg.ORACLE_MAINTAINED)`r`n"; $status = $false; }
		
		#If there were errors, write log entries
		if (!($status))
		{
			foreach ($msg in $msgs)
			{
				add-content $file $msg
				write-log-entry $global:LogFile "$($src.ROLE)" "" "" "ROLE DEFINITION" $false $msg $src $trg
			}
		}
		else #write log entry of success
		{
			write-log-entry $global:LogFile "$($src.ROLE)" "" "" "ROLE DEFINITION" $true "NO ERRORS FOUND" $src $trg
		}
		
		return $status
	}

	#Analyze the Priveleges granted on Roles
	function Analyze-Role_Privs ($src, $trg, $file)
	{
		$status = $true
		$msgs = @()
			
		if ($src.GRANTED_ROLE -ne $trg.GRANTED_ROLE) { $msgs += "`t`t`tError: Role Privilege Mismatch - GRANTED_ROLE`r`n`t`t`t`t$($srcDB): $($src.GRANTED_ROLE)`r`n`t`t`t`t$($trgDB): $($trg.GRANTED_ROLE)`r`n"; $status = $false; }
		if ($src.ADMIN_OPTION -ne $trg.ADMIN_OPTION) { $msgs += "`t`t`tError: Role Privilege Mismatch - ADMIN_OPTION`r`n`t`t`t`t$($srcDB): $($src.ADMIN_OPTION)`r`n`t`t`t`t$($trgDB): $($trg.ADMIN_OPTION)`r`n"; $status = $false; }
		if ($src.DEFAULT_ROLE -ne $trg.DEFAULT_ROLE) { $msgs += "`t`t`tError: Role Privilege Mismatch - DEFAULT_ROLE`r`n`t`t`t`t$($srcDB): $($src.DEFAULT_ROLE)`r`n`t`t`t`t$($trgDB): $($trg.DEFAULT_ROLE)`r`n"; $status = $false; }
		
		#These are new columns in version after 10g
		#if ($src.DELEGATE_OPTION -ne $trg.DELEGATE_OPTION) { $msgs += "`t`t`tError: Role Privilege Mismatch - DELEGATE_OPTION`r`n`t`t`t`t$($srcDB): $($src.DELEGATE_OPTION)`r`n`t`t`t`t$($trgDB): $($trg.DELEGATE_OPTION)`r`n"; $status = $false; }
		#if ($src.COMMON -ne $trg.COMMON) { $msgs += "`t`t`tError: Role Privilege Mismatch - COMMON`r`n`t`t`t`t$($srcDB): $($src.COMMON)`r`n`t`t`t`t$($trgDB): $($trg.COMMON)`r`n"; $status = $false; }
		
		#If there were errors, write log entries
		if (!($status))
		{
			add-content $file "`t`tRole Privilege: $($src.GRANTED_ROLE)"
			foreach ($msg in $msgs)
			{
				add-content $file $msg
				write-log-entry $global:LogFile "$($src.GRANTEE)" "$($src.GRANTED_ROLE)" "" "ROLE PRIVILEGE" $false $msg $src $trg
			}
		}
		else #write log entry of success
		{
			write-log-entry $global:LogFile "$($src.GRANTEE)" "$($src.GRANTED_ROLE)" "" "ROLE PRIVILEGE" $true "NO ERRORS FOUND" $src $trg
		}
		
		return $status
	}

	#Analyze the Priveleges granted on Tables
	function Analyze-Tab_Privs ($src, $trg, $file)
	{
		$status = $true
		$msgs = @()
		
		if ($src.GRANTOR -ne $trg.GRANTOR) { $msgs += "`t`t`tError: Table Privilege Mismatch - GRANTOR`r`n`t`t`t`t$($srcDB): $($src.GRANTOR)`r`n`t`t`t`t$($trgDB): $($trg.GRANTOR)`r`n"; $status = $false; }
		if ($src.PRIVILEGE -ne $trg.PRIVILEGE) { $msgs += "`t`t`tError: Table Privilege Mismatch - PRIVILEGE`r`n`t`t`t`t$($srcDB): $($src.PRIVILEGE)`r`n`t`t`t`t$($trgDB): $($trg.PRIVILEGE)`r`n"; $status = $false; }
		if ($src.GRANTABLE -ne $trg.GRANTABLE) { $msgs += "`t`t`tError: Table Privilege Mismatch - GRANTABLE`r`n`t`t`t`t$($srcDB): $($src.GRANTABLE)`r`n`t`t`t`t$($trgDB): $($trg.GRANTABLE)`r`n"; $status = $false; }
		if ($src.HIERARCHY -ne $trg.HIERARCHY) { $msgs += "`t`t`tError: Table Privilege Mismatch - HIERARCHY`r`n`t`t`t`t$($srcDB): $($src.HIERARCHY)`r`n`t`t`t`t$($trgDB): $($trg.HIERARCHY)`r`n"; $status = $false; }
		
		#These are new columns in version after 10g
		#if ($src.COMMON -ne $trg.COMMON) { $msgs += "`t`t`tError: Table Privilege Mismatch - COMMON`r`n`t`t`t`t$($srcDB): $($src.COMMON)`r`n`t`t`t`t$($trgDB): $($trg.COMMON)`r`n"; $status = $false; }
		#if ($src.TYPE -ne $trg.TYPE) { $msgs += "`t`t`tError: Table Privilege Mismatch - TYPE`r`n`t`t`t`t$($srcDB): $($src.TYPE)`r`n`t`t`t`t$($trgDB): $($trg.TYPE)`r`n"; $status = $false; }
		
		#If there were errors, write log entries
		if (!($status))
		{
			add-content $file "`t`tTable Privilege: $($src.OWNER).$($src.TABLE_NAME) : $($src.PRIVILEGE)"
			foreach ($msg in $msgs)
			{
				add-content $file $msg
				write-log-entry $global:LogFile "$($src.GRANTEE)" "$($src.OWNER).$($src.TABLE_NAME)" "$($src.PRIVILEGE)" "TABLE PRIVILEGE" $false $msg $src $trg
			}
		}
		else #write log entry of success
		{
			write-log-entry $global:LogFile "$($src.GRANTEE)" "$($src.OWNER).$($src.TABLE_NAME)" "$($src.PRIVILEGE)" "TABLE PRIVILEGE" $true "NO ERRORS FOUND" $src $trg
		}
		
		return $status
	}

	#Analyze the Priveleges granted on Columns
	function Analyze-Col_Privs($src, $trg, $file)
	{
		$status = $true
		$msgs = @()
		
		if ($src.GRANTOR -ne $trg.GRANTOR) { $msgs += "`t`t`tError: Column Privilege Mismatch - GRANTOR`r`n`t`t`t`t$($srcDB): $($src.GRANTOR)`r`n`t`t`t`t$($trgDB): $($trg.GRANTOR)`r`n"; $status = $false; }
		if ($src.GRANTABLE -ne $trg.GRANTABLE) { $msgs += "`t`t`tError: Column Privilege Mismatch - GRANTABLE`r`n`t`t`t`t$($srcDB): $($src.GRANTABLE)`r`n`t`t`t`t$($trgDB): $($trg.GRANTABLE)`r`n"; $status = $false; }
		
		#If there were errors, write log entries
		if (!($status))
		{
			add-content $file "`t`tTable Privilege: $($src.OWNER).$($src.TABLE_NAME).$($src.COLUMN_NAME) : $($src.PRIVILEGE)"
			foreach ($msg in $msgs)
			{
				add-content $file $msg
				write-log-entry $global:LogFile "$($src.GRANTEE)" "$($src.OWNER).$($src.TABLE_NAME).$($src.COLUMN_NAME)" "$($src.PRIVILEGE)" "COLUMN PRIVILEGE" $false $msg $src $trg
			}
		}
		else #write log entry of success
		{
			write-log-entry $global:LogFile "$($src.GRANTEE)" "$($src.OWNER).$($src.TABLE_NAME).$($src.COLUMN_NAME)" "$($src.PRIVILEGE)" "COLUMN PRIVILEGE" $true "NO ERRORS FOUND" $src $trg
		}
		
		return $status
	}

	#Analyze the Privelege granted on System
	function Analyze-Sys_Privs ($src, $trg, $file)
	{
		$status = $true
		$msgs = @()
		
		if ($src.ADMIN_OPTION -ne $trg.ADMIN_OPTION) { $msgs += "`t`t`tError: Sys Privilege Mismatch - ADMIN_OPTION`r`n`t`t`t`t$($srcDB): $($src.ADMIN_OPTION)`r`n`t`t`t`t$($trgDB): $($trg.ADMIN_OPTION)`r`n"; $status = $false; }
		
		#These are new columns in version after 10g
		#if ($src.COMMON -ne $trg.COMMON) { $msgs += "`t`t`tError: Sys Privilege Mismatch - COMMON`r`n`t`t`t`t$($srcDB): $($src.COMMON)`r`n`t`t`t`t$($trgDB): $($trg.COMMON)`r`n"; $status = $false; }
		if (!($status))#If there were errors, write log entries
		{
			add-content $file "`t`tSystem Privilege: $($src.PRIVILEGE)"
			foreach ($msg in $msgs)
			{
				add-content $file $msg
				write-log-entry $global:LogFile "$($src.GRANTEE)" "$($src.PRIVILEGE)" "" "SYSTEM PRIVILEGE" $false $msg $src $trg
			}
		}
		else #write log entry of success
		{
			write-log-entry $global:LogFile "$($src.GRANTEE)" "$($src.PRIVILEGE)" "" "SYSTEM PRIVILEGE" $true "NO ERRORS FOUND" $src $trg
		}
		
		return $status
	}

	#Handles retrieving and Analyzing Priveleges of a specified Type for a given Grantee
	#This is necessary as both Users and Roles can be granted privleges on Roles
	function Privs ($srcConn, $trgConn, $file, $grantee, $type)
	{
		$PrivErr = 0
		switch ($type)
		{
			"Role" { $from = "sys.dba_role_privs"; $order = "granted_role"; }
			"Table" { $from = "sys.dba_tab_privs"; $order = "owner, table_name, privilege"; }
			"Column" { $from = "sys.dba_col_privs"; $order = "owner, table_name, column_name, privilege"; }
			"System" { $from = "sys.dba_sys_privs"; $order = "privilege"; }
		}
		$where = "grantee = '$($grantee)'"
		
		$sql = "select * from $from where $where order by $order"
		$srcPrivs = Get-DataTable $srcConn $sql
		
		if ($srcPrivs -ne $null)
		{
			add-content $file "`tAnalyzing $type Privileges"
			$errFound = $false
			$i = 0
			
			foreach ($srcPriv in $srcPrivs)
			{
				$i += 1
				
				#Build SQL and retrieve privilege from Target
				switch ($type)
				{
					"Role" { $and = "granted_role = '$($srcPriv.GRANTED_ROLE)'"; }
					"Table" { $and = "owner = '$($srcPriv.OWNER)' and table_name = '$($srcPriv.TABLE_NAME)' and privilege = '$($srcPriv.PRIVILEGE)'"; }
					"Column" { $and = "owner = '$($srcPriv.OWNER)' and table_name = '$($srcPriv.TABLE_NAME)' and column_name = '$($srcPriv.COLUMN_NAME)' and privilege = '$($srcPriv.PRIVILEGE)'"; }
					"System" { $and = "privilege = '$($srcPriv.PRIVILEGE)'"; }
				}
				
				$sql = "select * from $from where $where and $and order by $order"
				$trgPrivs = Get-DataTable $trgConn $sql
				
				if ($trgPrivs -eq $null)
				{
					switch ($type)
					{
						"Role" { $obj = "$($srcPriv.GRANTED_ROLE)"; $subObj = ""; $msg = "`t`tRole Privilege: $($srcPriv.GRANTED_ROLE)"; }
						"Table" { $obj = "$($srcPriv.TABLE_NAME)"; $subObj = "$($srcPriv.PRIVILEGE)"; $msg = "`t`tTable Privilege: $($srcPriv.OWNER).$($srcPriv.TABLE_NAME) : $($srcPriv.PRIVILEGE)"; }
						"Column" { $obj = "$($srcPriv.TABLE_NAME).$($srcPriv.COLUMN_NAME)"; $subObj = "$($srcPriv.PRIVILEGE)"; $msg = "`t`tColumn Privilege: $($srcPriv.OWNER).$($srcPriv.TABLE_NAME).$($srcPriv.COLUMN_NAME) : $($srcPriv.PRIVILEGE)"; }
						"System" { $obj = "$($srcPriv.PRIVILEGE)"; $subObj = ""; $msg = "`t`tSystem Privilege: $($srcPriv.PRIVILEGE)"; }
					}
					$msg = "$msg`r`n`t`t`tError: $type Privilege not found in $trgDB`r`n"
					add-content $file $msg
					
					write-log-entry $global:LogFile "$grantee" "$obj" "$subObj" "$($type.ToUpper()) PRIVILEGE" $false $msg $srcPriv $null
					
					$PrivErr += 1
					$errFound = $true
				}
				else
				{
					foreach ($trgPriv in $trgPrivs) #Begin looping through Target Privileges, this should be 1 row almost exclusively
					{
						switch ($type)
						{
							"Role" { $status = Analyze-Role_Privs $srcPriv $trgPriv $file; }
							"Table" { $status = Analyze-Tab_Privs $srcPriv $trgPriv $file; }
							"Column" { $status = Analyze-Col_Privs $srcPriv $trgPriv $file; }
							"System" { $status = Analyze-Sys_Privs $srcPriv $trgPriv $file; }
						}
						if (!($status)) { $PrivErr += 1; $errFound = $true; }
					}
				}
				#Increment the number analyzed, calculate percent complete, and display to the user
				$global:Analyzed++
				$pct = [math]::floor(($global:Analyzed/$global:TotalPrivs)*100)
				Write-Progress -Activity "Analyzing: $grantee" -Status "Completed: $global:Analyzed of $global:TotalPrivs - $pct%" -id $progressID -parentID 0
			}
		}
		else #If no privileges found in Source
		{
			add-content $file "`tNo $type Privileges Found in $srcDB"
			$errFound = $true
		}
		
		if (!($errFound)) #Log if no errors were found
		{
			add-content $file "`t`tNo Errors Found"
		}
		
		return $PrivErr
	}
	
	$path = "$rootPath\$srcDB-$trgDB" #Establish the working path
	$file = "$path\$type\$grantee.log" #Establish the file to log to
	$global:LogFile = "$path\CSV-LOGS\$grantee.csv" #Establish the CSV to log to
	
	$Err = 0
    $PrivErr = 0
	
	add-content $file "Analyzing $Type: $grantee"
	
	#This query gets the total count of all the GRANTEE's privilegs for use with write-progress
	$sql = " select count(*) PRIVS from (`r`n"
	$sql = "$sql select grantee from dba_role_Privs where grantee = '$grantee'`r`n"
  	$sql = "$sql union all`r`n"
	$sql = "$sql select grantee from dba_sys_privs where grantee = '$grantee'`r`n"
	$sql = "$sql union all`r`n"
	$sql = "$sql select grantee from dba_tab_privs where grantee = '$grantee'`r`n"
	$sql = "$sql union all`r`n"
	$sql = "$sql select grantee from dba_col_privs where grantee = '$grantee')`r`n"
	$privs = Get-DataTable $srcConn $sql
	
	$global:TotalPrivs = [int]$privs.PRIVS
	$global:Analyzed = 0
	
	switch ($type)#Switch on Type for getting records
	{
		"USER" { $sql = "select * from dba_users where username = '$grantee'"}
		"ROLE" { $sql = "select * from dba_roles where role = '$grantee'"}
	}
	
	$srcGrantee = Get-DataTable $srcConn $sql
	$trgGrantee = Get-DataTable $trgConn $sql
	
	if ($trgGrantee -eq $null) #trgGrantee will be NULL if the GRANTEE doesn't exist in the Target database
	{
		add-content $file "`tError: $type not found in $trgDB"
		write-log-entry $global:LogFile "$grantee" "" "" "$type DEFINITION" $false "ERROR: $type NOT FOUND" $srcGrantee $trgGrantee
		$Err++
	}
	else #If the GRANTEE is found in the Target database
	{
		add-content $file "`tAnalyzing $type Definition"
		
		switch ($type)#Switch on Type for analysis of grantee definition
		{
			"USER" { $status = Analyze-User $srcGrantee $trgGrantee $file }
			"ROLE" { $status = Analyze-Role $srcGrantee $trgGrantee $file }
		}
		if (!($status)) { $Err++; }
		else { add-content $file "`t`tNo Errors Found"}
		
		$PrivErr += Privs $srcConn $trgConn $file $grantee "Role"
		$PrivErr += Privs $srcConn $trgConn $file $grantee "Table"
		$PrivErr += Privs $srcConn $trgConn $file $grantee "Column"
		$PrivErr += Privs $srcConn $trgConn $file $grantee "System"
	}
	
	switch ($type)#Switch on Type for result recording
	{
		"USER"
		{
			 if ($Err -ne 0 -or $PrivErr -ne 0)
			{
				$userFail.Add(@("$grantee definition errors: $Err", $path)) | out-null
				$userFail.Add(@("$grantee privilege errors: $PrivErr`r`n", $path)) | out-null
				
			}
			else
			{
				$userSuc.Add(@("$grantee definition error: $Err", $path)) | out-null
				$userSuc.Add(@("$grantee privilege errors: $PrivErr`r`n", $path)) | out-null
			}
		}
		"ROLE"
		{
			if ($Err -ne 0 -or $PrivErr -ne 0)
			{
				$roleFail.Add(@("$grantee definition errors: $Err", $path)) | out-null
				$roleFail.Add(@("$grantee privilege errors: $PrivErr`r`n", $path)) | out-null
			}
			else
			{
				$roleSuc.Add(@("$grantee definition errors: $Err", $path)) | out-null
				$roleSuc.Add(@("$grantee privilege errors: $PrivErr`r`n", $path)) | out-null
			}
		}
	}	
	
	$msg = "Completed: $srcDB - $type - $grantee" -replace "$", ""
	$threadMsgs.Add($msg) | out-null
	
	if ($Err -ne 0 -or $PrivErr -ne 0) { rename-item $file "!$grantee.log"; }
	
	$global:Analyzed++
	$pct = [math]::floor(($global:Analyzed/$global:TotalPrivs)*100)
	Write-Progress -Activity "Analyzing: $grantee" -Status "Completed: $global:Analyzed of $global:TotalPrivs - $pct%" -id $progressID -parentID 0 -completed
	
	$srcGrantee.Dispose()
	$trgGrantee.Dispose()
	
	[System.GC]::Collect()
} #End Script Block





if ($storedPass -eq $false) #If the user has not stored the password in a file
{
    #Prompt user for Source DB Credentials
    if ($srcUser -eq "") { $srcUser = read-host "Enter $srcDB User Name" }
	#This collects the password as a SecureString and then decrypts it into plain text for use with the ConnectionString
    if ($srcPass -eq $null) { $srcPass = read-host "Enter $srcDB Password" -AsSecureString; $srcUnenc = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($srcPass)); }
        else { $srcUnenc = $srcPass }
   

    #Prompt user for Target DB credentials
    if ($trgUser -eq "") { $trgUser = read-host "Enter $trgDB User Name" }
	#This collects the password as a SecureString and then decrypts it into plain text for use with the ConnectionString
    if ($trgPass -eq $null) { $trgPass = read-host "Enter $trgDB Password" -AsSecureString; $trgUnenc = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($trgPass)); }
        else { $trgUnenc = $trgPass }
}
else
{
    $srcUnenc = Unprotect-Password $srcDB "$ScriptPath\Encrypted-Pass.txt"
    $trgUnenc = Unprotect-Password $trgDB "$ScriptPath\Encrypted-Pass.txt"
        
    if ($srcUnenc -eq $false)
    {
        write-host "Stored Password not found for $srcDB"
        exit
    }

    if ($trgUnenc -eq $false)
    {
        write-host "Stored Password not found for $trgDB"
        exit
    }
}

#Create the Connection Objects
$srcConn = New-Object System.Data.OleDb.OleDbConnection
$trgConn = New-Object System.Data.OleDb.OleDbConnection

#Set the Connection String
$srcConn.ConnectionString = "User ID=$srcUser;password=$srcUnenc;Data Source=$srcDB;Provider=OraOLEDB.Oracle"
$trgConn.ConnectionString = "User ID=$trgUser;password=$trgUnenc;Data Source=$trgDB;Provider=OraOLEDB.Oracle"

#Set the base path for writing log files
if ($LogPath -eq "") #if no LogPath was provided, set to desktop of user who ran script
{
    $LogPath = [Environment]::GetFolderPath("Desktop") 
}
else 
{
    if ($LogPath.EndsWith("\") -or $LogPath.EndsWith("/")) { $LogPath = $LogPath -replace ".{1}$" } #Trim off last character if it is a \ or a /
}
$rootPath = "$LogPath\Security_Analysis_v2-$(get-date -f 'yyyyMMddHHmmss')" #set RootPath

#Create the folders necessary for logging
New-Path "$LogPath"
New-Path "$rootPath"

New-Path "$rootPath\$srcDB-$trgDB"
New-Path "$rootPath\$srcDB-$trgDB\User"
New-Path "$rootPath\$srcDB-$trgDB\Role"
New-Path "$rootPath\$srcDB-$trgDB\CSV-LOGS"

New-Path "$rootPath\$trgDB-$srcDB"
New-Path "$rootPath\$trgDB-$srcDB\User"
New-Path "$rootPath\$trgDB-$srcDB\Role"
New-Path "$rootPath\$trgDB-$srcDB\CSV-LOGS"

#If threads was not specified, calculate the number of threads with a Max of 8 based on the processing cores available
if ($threads -eq 0)
{
	#Determine the number of Cores on the executing machine
	$processor = get-wmiobject win32_processor;
	[int]$procs = $processor.NumberofLogicalProcessors
	$threads = $procs
}

#Instantiate variables for use in Session Variables and communication between threads
$threadMsgs = New-Object System.Collections.ArrayList
$userSuc = New-Object System.Collections.ArrayList
$userFail = New-Object System.Collections.ArrayList
$roleSuc = New-Object System.Collections.ArrayList
$roleFail = New-Object System.Collections.ArrayList

#Add the variables to the current Session of Powershell for use in Cross-Thread communication
$sessionState = [system.management.automation.runspaces.initialsessionstate]::CreateDefault()
$sessionState.Variables.Add((New-Object System.Management.Automation.Runspaces.SessionStateVariableEntry('threadMsgs', $threadMsgs, $null)))
$sessionState.Variables.Add((New-Object System.Management.Automation.Runspaces.SessionStateVariableEntry('userSuc', $userSuc, $null)))
$sessionState.Variables.Add((New-Object System.Management.Automation.Runspaces.SessionStateVariableEntry('userFail', $userFail, $null)))
$sessionState.Variables.Add((New-Object System.Management.Automation.Runspaces.SessionStateVariableEntry('roleSuc', $roleSuc, $null)))
$sessionState.Variables.Add((New-Object System.Management.Automation.Runspaces.SessionStateVariableEntry('roleFail', $roleFail, $null)))

#Create the pool in which the threads will run
$Pool = Get-RunspacePool $threads -SessionState $sessionState

#Instantiate the Pipes variable that will contain the threads
$pipes = @()

if ($Principals -ne "")
{
	$Principals = $Principals -replace " ", ""
	$Principals = $Principals.ToUpper() -replace ",", "','"
	$Principals = " where GRANTEE in ('$Principals')"
}
$sql = "select * from (`r`n"
$sql = "$sql select username GRANTEE, 'USER' TYPE from dba_users`r`n"
$sql = "$sql union all`r`n"
$sql = "$sql select role GRANTEE, 'ROLE' TYPE from dba_roles)`r`n"
$sql = "$sql$Principals"
$sql = "$sql order by type, grantee"

#Get records sets of the Security Principals from both the Source and Target databases
write-progress -Activity "Performing Security Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Retrieving list of Pricinipals from $($srcDB.ToUpper())" -id 0
$srcGrantees = Get-DataTable $srcConn $sql

write-progress -Activity "Performing Security Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Retrieving list of Pricinipals from $($trgDB.ToUpper())" -id 0
$trgGrantees = Get-DataTable $trgConn $sql

$start = get-date #Get the time for calculating to run time

if ($srcGrantees -eq $null) #If no Grantees are found in the Source Database srcGrantees will be NULL
{
	write-host "No Users/Roles Found in $srcDB"
	write-host "$sql"
}
else
{
	$records = 0
	$srcGrantees | foreach-object { $records++; } #Count the returned Records
	
	$x = 1
	foreach ($grantee in $srcGrantees) #Loop throught recordset and queue a thread for each
	{
        write-progress -Activity "Performing Security Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Queuing $($srcDB.ToUpper()) Threads: $x of $records" -id 0
        $pipes += Invoke-Async -RunspacePool $pool -ScriptBlock $sb -Parameters $srcConn, $trgConn, $srcDB, $trgDB, $rootPath, $ScriptPath, $grantee.GRANTEE, $grantee.TYPE, $x
		$x++
	}
}

if ($trgGrantees -eq $null) #If no Grantees are found in the Target Database
{
	write-host "No Users/Roles found in $trgDB"
	write-host "$sql"
}
else
{
	$records = 0
	$trgGrantees | foreach-object { $records++; } #Count the Returned Records
	
	foreach ($grantee in $trgGrantees) #Loop throughout recordset and queue a thread for each
	{
		write-progress -Activity "Performing Security Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Queuing $($trgDB.ToUpper()) Threads: $x of $records" -id 0
        $pipes += Invoke-Async -RunspacePool $pool -ScriptBlock $sb -Parameters $trgConn, $srcConn, $trgDB, $srcDB, $rootPath, $ScriptPath, $grantee.GRANTEE, $grantee.TYPE, $x
		$x++
	}
}

write-host "`r`n`tALL THREADS QUEUED, PLEASE BE PATIENT WHILE THEY EXECUTE"

Write-Progress -Activity "Performing Security Analysis of $($srcDB.ToUpper()) and $($trgDB.ToUpper())" -Status "Completed: 0 of $($Pipes.Length) - $pct%" -id 0

#Begin watching for completion messages
Watch-Messages $pipes $srcDB $trgDB

#Sort the results of Analysis
$userSuc = $userSuc | Sort-Object
$userFail = $userFail | Sort-Object
$roleSuc = $roleSuc | Sort-Object
$roleFail = $roleFail | Sort-Object

#Write section headers for both files
$file = "$rootPath\$srcDB-$trgDB\Analysis_Summary_Users.log"
add-content $file "------The following Users had mismatches------`r`n"

$file = "$rootPath\$trgDB-$srcDB\Analysis_Summary_Users.log"
add-content $file "------The following Users had mismatches------`r`n"

#Write section data
foreach ($msg in $userFail)
{
	$file = "$($msg[1])\Analysis_Summary_Users.log"
	add-content $file $msg[0]
}

#Write section headers for both files
$file = "$rootPath\$srcDB-$trgDB\Analysis_Summary_Users.log"
add-content $file "`r`n------The following Users had no errors------`r`n"

$file = "$rootPath\$trgDB-$srcDB\Analysis_Summary_Users.log"
add-content $file "`r`n------The following Users had no errors------`r`n"

#Write section data
foreach($msg in $userSuc)
{
	$file = "$($msg[1])\Analysis_Summary_Users.log"
	add-content $file $msg[0]
}

#Write section headers for both files
$file = "$rootPath\$srcDB-$trgDB\Analysis_Summary_Roles.log"
add-content $file "------The following Roles had mismatches------"

$file = "$rootPath\$trgDB-$srcDB\Analysis_Summary_Roles.log"
add-content $file "------The following Roles had mismatches------"

#Write section data
foreach ($msg in $roleFail)
{
	$file = "$($msg[1])\Analysis_Summary_Roles.log"
	add-content $file $msg[0]
}

#Write section headers for both files
$file = "$rootPath\$srcDB-$trgDB\Analysis_Summary_Roles.log"
add-content $file "`r`n------The following Roles had no errors------`r`n"

$file = "$rootPath\$trgDB-$srcDB\Analysis_Summary_Roles.log"
add-content $file "`r`n------The following Roles had no errors------`r`n"

#Write section data
foreach ($msg in $roleSuc)
{
	$file = "$($msg[1])\Analysis_Summary_Roles.log"
	add-content $file $msg[0]
}

$end = get-date
$x = $end - $start

$file = "$rootPath\$srcDB-$trgDB\SecurityAnalysis-Log.csv"
write-log-headers $file $srcDB $trgDB

Join-CSVlogs "$rootPath\$srcDB-$trgDB\CSV-LOGS" $file

$file = "$rootPath\$trgDB-$srcDB\SecurityAnalysis-Log.csv"
write-log-headers $file $trgDB $srcDB

Join-CSVlogs "$rootPath\$trgDB-$srcDB\CSV-LOGS" $file

Write-Host "`r`n------ Analysis Complete ------"
Write-Host "`r`nAccess Log Files At:`r`n$rootPath`r`n"

write-host "`r`n`tExecution Time: $x`r`n"

#Cleanup objects
foreach ($pipe in $pipes)
{
	$pipe.pipeline.dispose()
}
$pool.close()
$pool.dispose()

[System.GC]::Collect()