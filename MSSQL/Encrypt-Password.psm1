function Protect-Password
{
    Param
    (
        [Parameter(Mandatory=$true)][string]$Database,
        [Parameter(Mandatory=$false)][string]$file = '.\Encrypted-Pass.txt'
    )
    
    $pass = read-host "Enter $Database Password" -AsSecureString | ConvertFrom-SecureString    
    $Database = $Database.ToUpper()
    
    if (test-path $file)
    {
        $content = Get-Content $file #Get the contents of the file
        if ($content -match "^$Database\s*,") #Look for an entry that matches the specified database
        {
            $content = $content -replace "^$Database\s*,.*", "$Database,$pass" #Replace the old password for this database with the new one
            set-content $file $content #Overwrite the content of the file
        }
        else
        {
            add-content $file "$Database,$pass" #Add the new password
        }
    }
    else
    {
        add-content $file "$Database,$pass" #Add the new password and create the file
    }
}

function Unprotect-Password
{
    Param
    (
        [Parameter(Mandatory=$true)][string]$Database,
        [Parameter(Mandatory=$false)][string]$file = 'Encrypted-Pass.txt'
    )
    
    $Database = $Database.ToUpper()
    
    $contents = get-content $file #Get the contents of the password file
    
    foreach ($content in $contents)
    {
        $line = $content -split ","
        if ($line[0] -eq $database) {break} #Break if the database is found
    }
    
    
    if ($line[0] -eq $database)
    {
        $cred = New-Object -TypeName System.Management.Automation.PSCredential -argumentlist $line[0],($line[1] | ConvertTo-SecureString) #Decrypt the password
        return $cred.GetNetworkCredential().password
    }
    else #Database was not found
    {
        return $false
    }
}