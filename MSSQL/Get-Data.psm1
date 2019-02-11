 function Get-DataReader
{ 
	[Cmdletbinding()]
	Param
	(
		[Parameter(Position=0,Mandatory=$True)]$Conn,
		[Parameter(Position=1,Mandatory=$True)]$sql
	)
	try
	{
		
		#Open the connection to the DB if closed
		if($Conn.state -eq 'Closed')
		{
			$Conn.open()
		}
		
		#Create objects for querying the DB
		[System.Data.OleDB.OleDbCommand]$readcmd = New-Object system.Data.OleDb.OleDbCommand($sql,$Conn)
		$readcmd.CommandTimeout = '300' 
		[System.Data.OleDb.OleDbDataReader]$dr = $readcmd.ExecuteReader() #Get DataReader (This is a Cursor in the database lexicon)
		$readcmd.Dispose() #clean up read command
		
		# , is necessary between return and $dr DO NOT DELETE IT
		return , $dr
	}
	catch
	{
		add-content "$rootpath\Exceptions.log" "`r`nException:"
		add-content "$rootpath\Exceptions.log" $_
		add-content "$rootpath\Exceptions.log" $sql
		add-content "$rootpath\Exceptions.log" $conn.ConnectionString
		exit
	}
}

 function Get-DataTable
{ 
	[Cmdletbinding()]
	Param
	(
		[Parameter(Position=0,Mandatory=$True)]$Conn,
		[Parameter(Position=1,Mandatory=$True)]$sql
	)
	try
	{
		#Open the connection to the DB if closed
        if($Conn.state -eq 'Closed')
        {
            $Conn.open()
        }

        #Create objects for querying the DB
        $cmd = New-Object System.Data.SqlClient.SqlCommand
        $cmd.CommandText = $sql
		$cmd.Connection = $srcConn

        $da = New-Object System.Data.SqlClient.SqlDataAdapter
		$da.SelectCommand = $cmd
        
        #Query the DB and fill the DataTabe with records
    	$ds = New-Object System.Data.DataSet
		$da.fill($ds) | out-null
		
		#If only one record is returned then PowerShell will return a DataRow instead of a DataTable
		#More than one record results in a DataTable being returned as expected
        return $ds.tables[0]
	}
	catch
	{
		add-content "$rootpath\Exceptions.log" "`r`nException:"
		add-content "$rootpath\Exceptions.log" $_
		add-content "$rootpath\Exceptions.log" $sql
		add-content "$rootpath\Exceptions.log" $conn.ConnectionString
		write-host $_
		exit
	}
}