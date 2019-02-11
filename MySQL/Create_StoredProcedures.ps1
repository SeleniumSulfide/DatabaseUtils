[Cmdletbinding()]
Param
(
	[Parameter(Mandatory=$True)]$Server,
	[Parameter(Mandatory=$True)]$Database,
	[Parameter(Mandatory=$True)][string]$User,
	[Parameter(Mandatory=$false)]$Pass
)

function Get-Data($Conn, $sql)
{
	if ($Conn.State -ne "Open")
	{
		$Conn.Open()
	}
	
	$Comm = New-Object MySql.Data.MySqlClient.MySqlCommand($sql, $Conn)
	$da = New-Object MySql.Data.MySqlClient.MySqlDataAdapter($Comm)
	$ds = New-Object System.Data.DataSet
	[void]$da.Fill($ds, "data")
	return $ds
}

if ($Pass -eq $null) 
{ 
	$Pass = read-host "Enter $($database.ToUpper()) Password" -AsSecureString;
	$Unenc = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($Pass)); 
}
else 
{ 
	$Unenc = $Pass 
}

[System.Reflection.Assembly]::LoadWithPartialName("MySql.Data") | out-null
$Conn = New-Object MySql.Data.MySqlClient.MySqlConnection
$ConnStr = "server=$server;port=3306;database=$Database;uid=$User;pwd=$Unenc"
$Conn.ConnectionString = $ConnStr
$Conn.Open()

$sql = "select * from information_Schema.tables where table_schema = '$database'"

$ds = get-data $Conn $sql
$tables = $ds.tables[0]

$path = "c:\users\dan.gentry\desktop\scripts"
if(!(Test-Path -path $path)){ new-item -itemtype directory -path $path | out-null }

set-content "$path\All.sql" ""

foreach ($table in $tables)
{
	$sql = "select table_schema, table_name, column_name, is_nullable, data_type, column_type, column_key, EXTRA"
	$sql = "$sql from information_schema.columns"
	$sql = "$sql where table_schema = '$($table.TABLE_SCHEMA)' and"
	$sql = "$sql table_name = '$($table.TABLE_NAME)'"
	$sql = "$sql order by ordinal_position"
	
	$ds = get-data $Conn $sql
	$columns = $ds.tables[0]
	
	$InsParams = ""
	$SelUpdParams = ""
	$DelParams = ""
	
	$DelUpdWhere = ""
	$SelWhere = ""
	
	$InsUpdVals = ""
	
	$InsParamDelim = ""
	$SelUpdParamDelim = ""
	$DelParamDelim = ""
	
	$InsColumns = ""
	$InsValue = ""
	$UpdSet = ""
	
	$DelUpdWhereDelim = ""
	$SelWhereDelim = ""
	
	foreach ($column in $columns)
	{
		if ($InsParams -ne "")
		{
			$InsParamDelim = ",`r`n"
		}
		if ($SelUpdParams -ne "")
		{
			$SelUpdParamDelim = ",`r`n"
			$SelWhereDelim = " and`r`n"
		}
		if ($DelParams -ne "")
		{
			$DelParamDelim = ",`r`n"
			$DelUpdWhereDelim = " and`r`n"
		}
		
		switch ($column.DATA_TYPE)
		{
			"INT" { $IfNull = 0 }
			"datetime" { $IfNull = "str_to_date('01/01/1870', '%c/%e/%Y')" }
			"float" { $IfNull = 0 }
			"tinyint" { $IfNull = 0 }
			"varchar" { $IfNull = "''" }
		}
		
		$SelUpdParams = "$SelUpdParams$($SelUpdParamDelim)P_$($column.COLUMN_NAME) $($column.COLUMN_TYPE)"
		$SelWhere = "$SelWhere$SelWhereDelim IFNULL($($column.COLUMN_NAME), $IfNull) = IFNULL(P_$($column.COLUMN_NAME), $IfNull)"
		
		if ($column.COLUMN_KEY -eq "PRI")
		{
			$DelParams = "$DelParams$($DelParamDelim)P_$($column.COLUMN_NAME) $($column.COLUMN_TYPE)"
			$DelUpdWhere = "$DelUpdWhere$DelUpdWhereDelim $($column.COLUMN_NAME) =  P_$($column.COLUMN_NAME)"
			
			if ($column.EXTRA -ne "auto_increment")
			{
				$InsParams = "$InsParams$($InsParamDelim)P_$($column.COLUMN_NAME) $($column.COLUMN_TYPE)"
				$InsColumns = "$InsColumns$InsParamDelim$($column.COLUMN_NAME)"
				$InsValue = "$InsValue$($InsParamDelim)P_$($column.COLUMN_NAME)"
				$UpdSet = "$UpdSet$($InsParamDelim)$($column.COLUMN_NAME) = IFNULL(P_$($column.COLUMN_NAME), $($column.COLUMN_NAME))"
			}
		}
		else
		{
			$InsParams = "$InsParams$($InsParamDelim)P_$($column.COLUMN_NAME) $($column.COLUMN_TYPE)"
			$InsColumns = "$InsColumns$InsParamDelim$($column.COLUMN_NAME)"
			$InsValue = "$InsValue$($InsParamDelim)P_$($column.COLUMN_NAME)"
			$UpdSet = "$UpdSet$($InsParamDelim)$($column.COLUMN_NAME) = IFNULL(P_$($column.COLUMN_NAME), $($column.COLUMN_NAME))"
		}
	}
	
	$insert = "" 
	$insert = $insert + "DROP PROCEDURE IF EXISTS ``$($database)``.``insert_$($table.TABLE_NAME)``;`r`n"
	$insert = $insert + 'DELIMITER $$' + "`r`n"
	$insert = $insert + 'USE `tsheets`$$' + "`r`n"
	$insert = $insert + "CREATE PROCEDURE ``insert_$($table.TABLE_NAME)```r`n"
	$insert = $insert + "($InsParams)`r`n"
	$insert = $insert + "BEGIN`r`n"
	$insert = $insert + "insert into ``$($database)``.``$($table.TABLE_NAME)```r`n"
	$insert = $insert + "($InsColumns)`r`n"
	$insert = $insert + "values ($InsValue);`r`n"
	$insert = $insert + 'END$$' + "`r`n"
	#$insert = $insert + "DELIMITER ;"
	
	$update = ""
	$update = $update + "DROP PROCEDURE IF EXISTS ``$($database)``.``update_$($table.TABLE_NAME)``;`r`n"
	$update = $update + 'DELIMITER $$' + "`r`n"
	$update = $update + 'USE `tsheets`$$' + "`r`n"
	$update = $update + "CREATE PROCEDURE ``update_$($table.TABLE_NAME)```r`n"
	$update = $update + "($SelUpdParams)`r`n"
	$update = $update + "BEGIN`r`n"
	$update = $update + "/*This is written such that only what has changed need be passed in.`r`n"
	$update = $update + " *Unfortunately, as MySQL does not support optional paramters ALL parameters must be`r`n"
	$update = $update + " *utilized when executing SP, though other values that are not being utilized can be passed`r`n"
	$update = $update + " *in as null.*/`r`n"
	$update = $update + "update ``$($database)``.``$($table.TABLE_NAME)```r`n"
	$update = $update + "SET $UpdSet`r`nWhere $DelUpdWhere;`r`n"
	$update = $update + 'END$$' + "`r`n"
	#$update = $update + "DELIMETER ;"
	
	$delete = ""
	$delete = $delete + "DROP PROCEDURE IF EXISTS ``$($database)``.``delete_$($table.TABLE_NAME)``;`r`n"
	$delete = $delete + 'DELIMITER $$' + "`r`n"
	$delete = $delete + 'USE `tsheets`$$' + "`r`n"
	$delete = $delete + "CREATE PROCEDURE ``delete_$($table.TABLE_NAME)```r`n"
	$delete = $delete + "($DelParams)`r`n"
	$delete = $delete + "BEGIN`r`n"
	$delete = $delete + "delete from ``$($database)``.``$($table.TABLE_NAME)```r`n"
	$delete = $delete + "where $DelUpdWhere;`r`n"
	$delete = $delete + 'END$$' + "`r`n"
	#$delete = $delete + "DELIMETER ;"
	
	$select = ""
	$select = $select + "DROP PROCEDURE IF EXISTS ``$($database)``.``select_$($table.TABLE_NAME)``;`r`n"
	$select = $select + 'DELIMITER $$' + "`r`n"
	$select = $select + 'USE `tsheets`$$' + "`r`n"
	$select = $select + "CREATE PROCEDURE ``select_$($table.TABLE_NAME)```r`n"
	$select = $select + "($SelUpdParams)`r`n"
	$select = $select + "BEGIN`r`n"
	$select = $select + "/*This is written such that only what is being queried on need be passed in.`r`n"
	$select = $select + " *Unfortunately, as MySQL does not support optional paramters ALL parameters must be`r`n"
	$select = $select + " *utilized when executing SP, though other values that are not being utilized can be passed`r`n"
	$select = $select + " *in as null.*/`r`n"
	$select = $select + "select * from ``$($database)``.``$($table.TABLE_NAME)```r`n"
	$select = $select + "where $SelWhere;`r`n"
	$select = $select + 'END$$' + "`r`n"
	#$select = $select + "DELIMETER ;"
	
	
	$file = "$path\$($table.TABLE_NAME).sql"
	
	set-content $file "$insert`r`n`r`n$update`r`n`r`n$delete`r`n`r`n$select"
	add-content "$path\All.sql" "$insert`r`n`r`n$update`r`n`r`n$delete`r`n`r`n$select`r`n`r`n"
}