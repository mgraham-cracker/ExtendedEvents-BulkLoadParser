﻿function Get-SqlType  
{  
    param([string]$TypeName)  
  
    switch ($TypeName)   
    {  
        'Boolean' {[Data.SqlDbType]::Bit}  
        'Byte[]' {[Data.SqlDbType]::VarBinary}  
        'Byte'  {[Data.SQLDbType]::VarBinary}  
        'Datetime'  {[Data.SQLDbType]::DateTime}  
        'Decimal' {[Data.SqlDbType]::Decimal}  
        'Double' {[Data.SqlDbType]::Float}  
        'Guid' {[Data.SqlDbType]::UniqueIdentifier}  
        'Int16'  {[Data.SQLDbType]::SmallInt}  
        'Int32'  {[Data.SQLDbType]::Int}  
        'Int64' {[Data.SqlDbType]::BigInt}  
        'UInt16'  {[Data.SQLDbType]::SmallInt}  
        'UInt32'  {[Data.SQLDbType]::Int}  
        #'UInt64' {[Data.SqlDbType]::BigInt}
        'UInt64' {[Data.SqlDbType]::Decimal} #Added to resolve issue with large integers     
        'Single' {[Data.SqlDbType]::Decimal} 
        default {[Data.SqlDbType]::VarChar}  
    }  
      
}
 
function Add-SqlTable  
{  
  
    [CmdletBinding()]  
    param(  
    [Parameter(Position=0, Mandatory=$true)] [string]$ServerInstance,  
    [Parameter(Position=1, Mandatory=$true)] [string]$Database,  
    [Parameter(Position=2, Mandatory=$true)] [String]$TableName,  
    [Parameter(Position=3, Mandatory=$true)] [System.Data.DataTable]$DataTable,  
    [Parameter(Position=4, Mandatory=$false)] [Int32]$MaxLength=0, 
    [Parameter(Position=5, Mandatory=$false)] [switch]$AsScript 
    )  
  
  try {add-type -AssemblyName "Microsoft.SqlServer.ConnectionInfo, Version=10.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91" -EA Stop} 
catch {add-type -AssemblyName "Microsoft.SqlServer.ConnectionInfo"} 
 
try {add-type -AssemblyName "Microsoft.SqlServer.Smo, Version=10.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91" -EA Stop}  
catch {add-type -AssemblyName "Microsoft.SqlServer.Smo"} 

 try { 

    $con = new-object ("Microsoft.SqlServer.Management.Common.ServerConnection") $ServerInstance 
      
    $con.Connect()  
  
    $server = new-object ("Microsoft.SqlServer.Management.Smo.Server") $con  
    $db = $server.Databases[$Database]  
    $table = new-object ("Microsoft.SqlServer.Management.Smo.Table") $db, $TableName  
  
    foreach ($column in $DataTable.Columns)  
    {  
        $sqlDbType = [Microsoft.SqlServer.Management.Smo.SqlDataType]"$(Get-SqlType $column.DataType.Name)"  
        if ($sqlDbType -eq 'VarBinary' -or $sqlDbType -eq 'VarChar')  
        {  
            if ($MaxLength -gt 0)  
            {$dataType = new-object ("Microsoft.SqlServer.Management.Smo.DataType") $sqlDbType, $MaxLength} 
            else 
            { $sqlDbType  = [Microsoft.SqlServer.Management.Smo.SqlDataType]"$(Get-SqlType $column.DataType.Name)Max" 
              $dataType = new-object ("Microsoft.SqlServer.Management.Smo.DataType") $sqlDbType 
            } 
        }
        elseif ($sqlDbType -eq 'Decimal')  
        {
            $dataType = new-object ("Microsoft.SqlServer.Management.Smo.DataType") $sqlDbType, 20
        }  
        else  
        { $dataType = new-object ("Microsoft.SqlServer.Management.Smo.DataType") $sqlDbType }  
        $col = new-object ("Microsoft.SqlServer.Management.Smo.Column") $table, $column.ColumnName, $dataType  
        $col.Nullable = $column.AllowDBNull  
        $table.Columns.Add($col)  
    }  
  
    if ($AsScript) { 
        $table.Script() 
    } 
    else { 
        $table.Create() 
    } 
} 
catch { 
    $message = $_.Exception.GetBaseException().Message 
    Write-Error $message 
} 
   
}

function ExtendedEvents-BulkLoadParser
{
[CmdletBinding()]
    param (
    [Parameter(ValuefromPipeline=$true,mandatory=$true,Position=0
        ,HelpMessage="Supply Extended Event File Path")][String[]] $SourceFile,
    [Parameter(mandatory=$true,Position=1
        ,HelpMessage="Supply Target DB Server")][String] $TargetServer,
    [Parameter(mandatory=$true,Position=2)][String] $TargetDB,
    [Parameter(mandatory=$true,Position=3)][String] $TargetTable,
    [Parameter(mandatory=$false,Position=4)][String] $Append='n'
    )
 
    #For SQL Server 2014:
    Add-Type -Path 'C:\Program Files\Microsoft SQL Server\120\Shared\Microsoft.SqlServer.XE.Core.dll'
    Add-Type -Path 'C:\Program Files\Microsoft SQL Server\120\Shared\Microsoft.SqlServer.XEvent.Linq.dll'
    #For SQL Server 2012:
    #Add-Type -Path 'C:\Program Files\Microsoft SQL Server\110\Shared\Microsoft.SqlServer.XEvent.Linq.dll'
 
    $connectionString = "Data Source = '$TargetServer'; Initial Catalog = '$TargetDB'; Integrated Security = SSPI"
 
     $bcp = New-Object -TypeName System.Data.SqlClient.SqlBulkCopy -ArgumentList @($connectionString)
 

     #PublishedEvent Class is the main class to traverse
     #https://msdn.microsoft.com/en-us/library/microsoft.sqlserver.xevent.linq.publishedevent.aspx

    $events = New-Object Microsoft.SqlServer.XEvent.Linq.QueryableXEventData($SourceFile)
    Write-Output "begin reading file: " $SourceFile (Get-Date).ToString()
    $table = New-Object system.Data.DataTable "XEventTable"
    $ii=0

    if(-Not ($table.Columns.Contains("EventName")))
    {
        $col = New-Object system.Data.DataColumn "EventName",([string])
        $table.Columns.Add($col)
        $bcp.ColumnMappings.Add("EventName", "EventName") | Out-Null
    }
    if(-Not ($table.Columns.Contains("TimeStamp")))
    {
        $col = New-Object system.Data.DataColumn "TimeStamp",([datetime])
        $table.Columns.Add($col)
        $bcp.ColumnMappings.Add("TimeStamp", "TimeStamp") | Out-Null
    }
    foreach ($evt in $events)
    {
          $row = $table.NewRow()
          $table.Rows.Add($row)
          $table.Rows[$ii]["EventName"] = $evt.Name
          $table.Rows[$ii]["TimeStamp"] = $evt.Timestamp.LocalDateTime

          foreach ($fld in $evt.Fields)
            {
                if(-Not ($table.Columns.Contains($fld.Name)))
                {
                    $type = ([System.Type]$fld.Type)
                
                    if($type.BaseType.ToString() -eq "System.Object")
                    {
                        $type = ([System.Type]"System.String")
                    }
                    $col = New-Object system.Data.DataColumn $fld.Name,($type)
                    $table.Columns.Add($col)

                    $bcp.ColumnMappings.Add($fld.Name, $fld.Name) | Out-Null
                }
                if([string]::IsNullOrWhitespace($fld.Value.ToString()))
                {
                    $value = $null
                }else
                {
                    $value = $fld.Value
                }
                $table.Rows[$ii][$fld.Name] = $value
            
            }

          foreach ($act in $evt.Actions)
            {
                if(-Not ($table.Columns.Contains($act.Name)))
                {
                    $type = ([System.Type]$act.Type)

                    if($type.BaseType.ToString() -eq "System.Object")
                    {
                        $type = ([System.Type]"System.String")                    
                    }
                    $col = New-Object system.Data.DataColumn $act.Name,($type)
                    $table.Columns.Add($col)
                
                    $bcp.ColumnMappings.Add($act.Name, $act.Name) | Out-Null
                }
                if([string]::IsNullOrWhitespace($act.Value.ToString()))
                {
                    $value = $null
                }else
                {
                    $value = $act.Value
                }
                $table.Rows[$ii][$act.Name] = $value
            } 
        $ii++
    }


    Write-Output "Begin Table Create: " $TargetTable (Get-Date).ToString()
    ##Create table first
    if($Append -eq "n")
    {Add-SqlTable -ServerInstance $TargetServer -Database $TargetDB -TableName $TargetTable -DataTable $table}

    $bcp.DestinationTableName = $TargetTable
    $bcp.Batchsize = 1000
    $bcp.BulkCopyTimeout = 0
    try
    { 
        $bcp.WriteToServer($table)
    }
    catch
    {
       Write-Error $_.Exception | format-list -force
    }

    $table.Rows.Clear()

}