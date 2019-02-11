function Get-SQLConnectionObject
{
    [Cmdletbinding()]
	Param
	(
		[Parameter(Position=0,Mandatory=$True)]$Server,
		[Parameter(Position=1,Mandatory=$True)]$Port,
        [Parameter(Position=2,Mandatory=$True)]$DB
	)

    $Conn = New-Object System.Data.SqlClient.SqlConnection

    #Set the Connection String
    $ConnStr = "Server=$Server,$Port;Database=$DB;Trusted_Connection=True;"

    #Set the ConnectionString property
    $Conn.ConnectionString = $ConnStr

    return $conn
}

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
		$cmd.Connection = $Conn

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

function Execute-SQLNonQuery
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
		$cmd.Connection = $Conn
        $cmd.ExecuteNonQuery()
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

function Insert-BITStats
{
    [Cmdletbinding()]
    Param
    (
        [Parameter(Position=0,Mandatory=$True)]$conn,
        [Parameter(Position=1,Mandatory=$True)]$Machine,
        [Parameter(Position=2,Mandatory=$True)]$Iteration,
        [Parameter(Position=3,Mandatory=$True)]$BITStats
    )

    $StartDate = $BITStats[0]
    $Stats = $BITStats[1]

    foreach ($Test in $Stats)
    {
        switch($Test.Operations.Split(" ")[1])
        {
            "Thousand" { $x = 1000 }
            "Million" { $x = 1000000 }
            "Billion" { $x = 1000000000 }
            "Trillion" { $x = 1000000000000 }
            "Quadrillion" { $x = 1000000000000000 }
            Default { $x = 1 }
        }

        $Ops = [int64]$Test.Operations.Split(" ")[0]*[int64]$x

        $sql = "insert into dbo.TestStats (MachineName, StartDate, Iteration, Test, Cycles, Operations, Result, Errors, LastError) 
                values ('$Machine', convert(datetime, '$StartDate', 101), $Iteration, '$($Test.TestName)', $($Test.Cycles), $Ops, '$($Test.Result)', $($Test.Errors), '$($Test.LastError)')"
        
        Execute-SQLNonQuery $conn $sql
    }
}