param(
    # SQL Connection or String (Default for local dev)
    [Parameter(Mandatory=$False)]
    [System.Data.SqlClient.SqlConnection]
    $SqlConnection = 'Data Source=.\SQLSERVER2016;Integrated Security=true',
    # PagerDuty RO Key
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $PdRoApiKey,
    # Incremental Update Rewind Buffer
    [Parameter(Mandatory=$False)]
    [int]
    $IncrementalBufferSecs = (60*60),
    # Incremental Update Atomic Window
    [Parameter(Mandatory=$False)]
    [int]
    $IncrementalAtomicUpdateWindowSec = (60*60*24),
    # Default date to start incremental searches when table is empty
    [Parameter(Mandatory=$False)]
    [datetime]
    $PdDefaultStartDate = '2012-01-01T00:00Z',
    # Only syncs log_entries and not users/schedules/etc
    [switch]$IncrementalUpdateOnly,
    # Purge SyncLogs and SyncRuns tables
    [switch]$PurgeSyncMetadata,
    # Purge Incremental tables
    [switch]$PurgeIncrementalTables,
    # SQl table name
    [Parameter(Mandatory=$False)]
    [ValidateNotNullOrEmpty()]
    [string]
    $SqlDatabaseName = 'pagerdutysql',
    # Skip querying most recent logs after processing
    [switch]$SkipFinalErrorLogQuery,
    # Exit with code if issues occour
    [switch]$ExitOnIssues
)
# $logs = .\Import-Pd2Mssql -SqlConnection "Data Source=.\SQLSERVER2016;Initial Catalog=pagerdutysql;Integrated Security=true" -PdRoApiKey $pagerdutyROApiKey -PurgeSyncMetadata -PurgeIncrementalTables
# $logs = .\Import-Pd2Mssql -SqlConnection "Data Source=.\SQLSERVER2016;Initial Catalog=pagerdutysql;Integrated Security=true" -PdRoApiKey $pagerdutyROApiKey -PurgeIncrementalTables -IncrementalUpdateOnly
#######################################
# func
#######################################
# --- logging functions ---
function Format-ExceptionErrorRecord {
param(
    [Parameter(Mandatory=$True)]
    [System.Management.Automation.ErrorRecord]
    $ErrorRecord
)
    "Ex @ $($ErrorRecord.InvocationInfo.ScriptName):$($ErrorRecord.Exception.Line):$($ErrorRecord.Exception.Offset): $($ErrorRecord.Exception.Message)`n$($ErrorRecord.Exception.StackTrace)"
}
function Get-CleanSqlName {
param(
    [Parameter(Mandatory=$True,Position=0)]
    [ValidateNotNullOrEmpty()]
    [string]
    $String
)
    $String -replace "[^A-Za-z0-9_$]",""
}
function New-SyncRunEntry {
    $cleanDbName = Get-CleanSqlName $SqlDatabaseName
    $insertSql = "INSERT INTO [$($cleanDbName)].[dbo].[SyncRuns]
           ([SyncRunId]
           ,[StartTime]
           ,[EndTime]
           ,[IsFinished]
           ,[IsSuccessful])
     VALUES
           (@syncrunid
           ,@starttime
           ,NULL
           ,0
           ,0)"
    $sqlParams = @{
        syncrunid = $script:SyncRunId
        starttime = [datetimeoffset](get-date)
    }
    write-verbose "New SyncRuns Entry Id:$($script:SyncRunId)"
    $null = Invoke-SqlCmd2 -SqlConnection $SqlConnection -Query $insertSql -SqlParameters $sqlParams
}
function Update-SyncRunEntryAsFinished {
param(
    [switch]$SetSuccessful
)
    $endTime = [datetimeoffset](get-date)
    $isFinished = $True
    $issuccessful = $False
    iF($SetSuccessful) {
        $issuccessful = $True
    }
    $cleanDbName = Get-CleanSqlName $SqlDatabaseName
    $insertSql = "UPDATE [$($cleanDbName)].[dbo].[SyncRuns]
    SET [EndTime] = @endtime
      ,[IsFinished] = @isfinished
      ,[IsSuccessful] = @issuccessful
    WHERE [SyncRunId] = @syncrunid"
    $sqlParams = @{
        syncrunid = $script:SyncRunId
        endtime = $endTime
        isfinished = $isFinished
        issuccessful = $issuccessful
    }
    write-verbose "Updating SyncRuns Entry as Finished Id:$($script:SyncRunId) IsSuccessful:$($IsSuccessful)"
    $null = Invoke-SqlCmd2 -SqlConnection $SqlConnection -Query $insertSql -SqlParameters $sqlParams
}
function Write-SyncLog {
param(
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $Entry,
    [Parameter(Mandatory=$False)]
    [ValidateSet('info','error')]
    [string]
    $Type = 'info'
)
    $cleanDbName = Get-CleanSqlName $SqlDatabaseName
    $insertSql = "INSERT INTO [$($cleanDbName)].[dbo].[SyncLogs]
           ([DateTimeOffset]
           ,[Entry]
           ,[Type]
           ,[SyncRunId])
     VALUES
            (@datetimeoffset
            ,@entry
            ,@type
            ,@syncrunid)"
    write-verbose "SyncLogs: $($Entry)"
    $sqlParams = @{
        syncrunid = $script:SyncRunId
        datetimeoffset = [datetimeoffset](get-date)
        Type = $Type
        entry = $Entry
    }
    $null = Invoke-SqlCmd2 -SqlConnection $SqlConnection -Query $insertSql -SqlParameters $sqlParams
}
# --- PagerDuty to SQL mapping & SQL helper functions ---
function Get-SqlTableColumnMap {
param(
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $TableName
)
    $ret = @{}
    $cleanDbName = Get-CleanSqlName $SqlDatabaseName
    $selectSql = "SELECT *
        FROM [$($cleanDbName)].[INFORMATION_SCHEMA].[COLUMNS]
        WHERE [TABLE_NAME] = @tableName"
    $sqlParams = @{ tableName = $TableName }
    $columnsResult = Invoke-SqlCmd2 -SqlConnection $SqlConnection -Query $selectSql -SqlParameters $sqlParams -as psobject
    foreach($column in $columnsResult) {
        $ret[$column.COLUMN_NAME] = $column
    }
    return $ret
}
function Convert-JsonValueToSqlValue {
param(
    #  Target SQL Column type
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $SqlDataType,
    # JSON property value
    [Parameter(Mandatory=$True)]
    [AllowNull()]
    [object]
    $Value
)
    if($null -eq $Value) { return $null }
    $val = $Value
    try {
        switch($SqlDataType) {
            'datetimeoffset' {
                $val = [datetimeoffset]$val
            }
        }
    }
    catch {
        $script:issueCount++
        $entry = "Issue converting $($val.GetType().FullName) for sql data type: $($SqlDataType): "
        $entry += Format-ExceptionErrorRecord $_
        write-warning $entry
        Write-SyncLog -Entry $entry -Type 'error'
        throw "Cannot convert value for sql"
    }
    return $val
}
function Get-SqlInsertFromJson {
param(
    # SQL Table with matching column/data types
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $TableName,
    # Unwrapped API Records
    [Parameter(Mandatory=$True)]
    [psobject[]]
    $Records,
    # Skip these SQL columns (e.g. if Records doesn't have matching property)
    [Parameter(Mandatory=$False)]
    [string[]]
    $SqlSkipColumns = @()
)
    $cleanTableName = Get-CleanSqlName $TableName
    $columnMap = Get-SqlTableColumnMap -TableName $TableName
    [string[]]$columnNames = $columnMap.Keys | where-object { $SqlSkipColumns -notcontains $_ }
    [string[]]$columnBracketNames = $columnNames | foreach-object { "[$($_)]" }
    [string[]]$sqlParamNames = $columnNames | foreach-object { "@$($_)" }
    
    # Generate dynamic sql
    $cleanDbName = Get-CleanSqlName $SqlDatabaseName
    $dynamicInsertSql = "INSERT INTO [$($cleanDbName)].[dbo].[$($cleanTableName)] ($($columnBracketNames -join ','))" +
                        " VALUES ($($sqlParamNames -join ','))"
    # generate insert params
    $sqlInsertParams = @()
    $rowCount = 0
    foreach($record in $Records) {
        try {
            $sqlParams = @{}

            # Map record properties to sql params
            [string[]]$propertyNames = $record.psobject.properties.name
            [string[]]$unmappedColumns = ($columnNames | where-object { $propertyNames -notcontains $_ })
            foreach($propertyName in $propertyNames) {
                if($columnNames -notcontains $propertyName) { continue }
                $sqlColumn = $columnMap[$propertyName]
                $sqlParams[$propertyName] = Convert-JsonValueToSqlValue -SqlDataType $sqlColumn.DATA_TYPE -Value $record."$propertyName"
            }
            # If the record is missing the property, try to use NULL
            foreach($unmappedColumn in $unmappedColumns) {
                $sqlColumn = $columnMap[$unmappedColumn]
                if($sqlColumn.IS_NULLABLE -ne 'YES') {
                    Write-SyncLog -Type 'error' -Entry "Table $($cleanTableName) Column $($unmappedColumn) is not nullable and the source record is missing the property"
                    throw "Cannot assume null because target column is not nullable"
                }
                $sqlParams[$unmappedColumn] = $null
            }
            $sqlInsertParams += $sqlParams
            $rowCount++
        }
        catch {
            $script:issueCount++
            $recordJson = $record | ConvertTo-Json -Depth 10 -Compress
            $entry = "Cannot Converting Record for Table $($cleanTableName) Record:$($recordJson): "
            $entry += Format-ExceptionErrorRecord $_
            write-warning $entry
            Write-SyncLog -Entry $entry -Type 'error'
        }
    }
    
    # ret
    Write-SyncLog -Entry "Generated $($sqlInsertParams.count) INSERTS and SQL: $($dynamicInsertSql)"
    [pscustomobject]@{
        table = $cleanTableName
        sql = $dynamicInsertSql
        sqlinserts = $sqlInsertParams
        hasinserts = ($sqlInsertParams.count -gt 0)
        rowcount = $rowCount
    }
}
function Get-SqlTransactionInsertFromJson {
param(
    # SQL Table with matching column/data types
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $TableName,
    # Unwrapped API Records
    [Parameter(Mandatory=$True)]
    [psobject[]]
    $Records,
    # Skip these SQL columns (e.g. if Records doesn't have matching property)
    [Parameter(Mandatory=$False)]
    [string[]]
    $SqlSkipColumns = @(),
    # How many inserts to batch together
    [Parameter(Mandatory=$False)]
    [int]
    $SqlParamThreshold = 2000
)
    $cleanTableName = Get-CleanSqlName $TableName
    $columnMap = Get-SqlTableColumnMap -TableName $TableName
    [string[]]$columnNames = $columnMap.Keys | where-object { $SqlSkipColumns -notcontains $_ }
    [string[]]$columnBracketNames = $columnNames | foreach-object { "[$($_)]" }
    
    # generate insert params
    $transactionSets = @()
    $insertedRowsCount = 0
    $rowCount = 0
    $lastEmptiedBufferRow = 0
    $sqlInsertsSb = new-object System.Text.StringBuilder
    $sqlParamsBuffer = @{}
    $ignoredPropsBuffer = @()
    $cleanDbName = Get-CleanSqlName $SqlDatabaseName
    $dynamicInsertSqlFmt = "INSERT INTO [$($cleanDbName)].[dbo].[$($cleanTableName)] ($($columnBracketNames -join ',')) VALUES ({0});"
    foreach($record in $Records) {
        try {
            $insertedRowsCount++
            $rowCount++

            # Map record properties to numbered sql params
            [string[]]$propertyNames = $record.psobject.properties.name
            [string[]]$unmappedColumns = ($columnNames | where-object { $propertyNames -notcontains $_ })
            $tmpSqlParamBuffer = @{}
            foreach($propertyName in $propertyNames) {
                if($columnNames -notcontains $propertyName) {
                    if($ignoredPropsBuffer -notcontains $propertyName) {
                        $ignoredPropsBuffer += $propertyName
                    }
                    continue
                }
                $sqlColumn = $columnMap[$propertyName]
                $tmpSqlParamBuffer["$($propertyName)$($rowCount)"] = Convert-JsonValueToSqlValue -SqlDataType $sqlColumn.DATA_TYPE -Value $record."$propertyName"
            }
            # If the record is missing the property, try to use NULL
            foreach($unmappedColumn in $unmappedColumns) {
                $sqlColumn = $columnMap[$unmappedColumn]
                if($sqlColumn.IS_NULLABLE -ne 'YES') {
                    Write-SyncLog -Type 'error' -Entry "Table $($cleanTableName) Column $($unmappedColumn) is not nullable and the source record is missing the property"
                    throw "Cannot assume null because target column is not nullable"
                }
                $tmpSqlParamBuffer["$($unmappedColumn)$($rowCount)"] = $null
            }

            # Page buffer before it goes over SQL Param threshold
            $newSqlParamCount = $sqlParamsBuffer.Count + $tmpSqlParamBuffer.Count
            if($newSqlParamCount -ge $SqlParamThreshold) {
                $transactionSets += [pscustomobject]@{
                    table = $cleanTableName
                    sql = Format-SqlTransaction -Sql $sqlInsertsSb.ToString()
                    sqlparams = @{} + $sqlParamsBuffer
                    ignoredproperties = $ignoredPropsBuffer
                    rowcount = $rowCount
                }
                [void]$sqlInsertsSb.Clear()
                $sqlParamsBuffer.Clear()
                $ignoredPropsBuffer = @()
                $lastEmptiedBufferRow = $insertedRowsCount
            }

            # Generate INSERT with numbered params
            [string[]]$sqlParamNames = $columnNames | foreach-object { "@$($_)$($rowCount)" }
            [void]$sqlInsertsSb.AppendLine(($dynamicInsertSqlFmt -f ($sqlParamNames -join ',')))
            $sqlParamsBuffer += $tmpSqlParamBuffer
        }
        catch {
            $script:issueCount++
            $recordJson = $record | ConvertTo-Json -Depth 10 -Compress
            $entry = "Cannot convert Record for Table $($cleanTableName) Record:$($recordJson): "
            $entry += Format-ExceptionErrorRecord $_
            write-warning $entry
            Write-SyncLog -Entry $entry -Type 'error'
        }
    }
    # Add the remaining buffer
    if($sqlParamsBuffer.Count -gt 0) {
        $transactionSets += [pscustomobject]@{
            table = $cleanTableName
            sql = Format-SqlTransaction -Sql $sqlInsertsSb.ToString()
            sqlparams = @{} + $sqlParamsBuffer
            ignoredproperties = $ignoredPropsBuffer
            rowcount = $rowCount
        }
    }

    # ret
    Write-SyncLog -Entry "Generated $($transactionSets.Count) INSERT TRANSACTION Sets with SqlParam Threshold of $($SqlParamThreshold) for $($records.count) records with SQL fmt: $($dynamicInsertSqlFmt)"
    return $transactionSets
}
function Add-PdRecordToSqlTable {
param(
    # SQL Table with matching column/data types
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $TableName,
    # Unwrapped API Records
    [Parameter(Mandatory=$True)]
    [psobject[]]
    $Records,
    # Skip these SQL columns (e.g. if Records doesn't have matching property)
    [Parameter(Mandatory=$False)]
    [string[]]
    $SqlSkipColumns = @()
)
    $generatedInserts = Get-SqlInsertFromJson -TableName $TableName -Records $Records -SqlSkipColumns $SqlSkipColumns
    if(!$generatedInserts.hasinserts) {
        Write-SyncLog -Entry "Table $($TableName) has no generating inserts"
        return
    }
    $script:newRowCount += $generatedInserts.rowcount
    $insertEntry =  "Table $($generatedInserts.table) INSERTING $($generatedInserts.sqlinserts.Count) Records"
    Write-SyncLog -Entry $insertEntry
    write-host $insertEntry -f yellow
    foreach($sqlParams in $generatedInserts.sqlinserts) {
        try {
            $null = Invoke-SqlCmd2 -SqlConnection $SqlConnection -Query $generatedInserts.sql -SqlParameters $sqlParams
            $script:insertedRowCount++
        }
        catch {
            $script:issueCount++
            $recordJson = $record | ConvertTo-Json -Depth 10 -Compress
            $entry = "Error Updating Record in Table $($cleanTableName) Record:$($recordJson): "
            $entry += Format-ExceptionErrorRecord $_
            write-warning $entry
            Write-SyncLog -Entry $entry -Type 'error'
        }
    }
}
function Clear-SqlTable {
param(
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $TableName,
    [switch]$NoLog
)
    $cleanTableName = Get-CleanSqlName $TableName
    write-host "Truncating table $($cleanTableName)" -f gray
    if(!$NoLog) {
        Write-SyncLog -Entry "Table $($cleanTableName) Truncating"
    }
    $cleanDbName = Get-CleanSqlName $SqlDatabaseName
    $truncateQuery = "TRUNCATE TABLE [$($cleanDbName)].[dbo].[$($cleanTableName)]"
    $null = Invoke-SqlCmd2 -SqlConnection $SqlConnection -Query $truncateQuery
}
function Format-SqlTransaction {
param(
    [Parameter(Mandatory=$False)]
    [string]
    $Sql
)
# Try/Catch lets us raise the original error message unlike xact_abort
"BEGIN TRY
    BEGIN TRANSACTION
    $($Sql)
    COMMIT TRAN
END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT; 
    DECLARE @ErrorState INT;
    SELECT @ErrorMessage = ERROR_MESSAGE(),@ErrorSeverity = ERROR_SEVERITY(),@ErrorState = ERROR_STATE();
    
    IF @@TRANCOUNT > 0
        ROLLBACK TRAN
    
    RAISERROR (@ErrorMessage,@ErrorSeverity,@ErrorState);
END CATCH;"
}

# --- pagerduty api ---
# Invoke-WebRequest/Invoke-RestMethod -Body do not support duplicate query params
# PagerDuty API Convention for GET is to duplicate array[] types
# https://v2.developer.pagerduty.com/docs/includes
function Format-PagerDutyResourceUri {
param(
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $Resource,
    [Parameter(Mandatory=$False)]
    [datetime]
    $Since,
    [Parameter(Mandatory=$False)]
    [datetime]
    $Until,
    [Parameter(Mandatory=$False)]
    [ValidateNotNullOrEmpty()]
    [string[]]
    $Includes = @(),
    [Parameter(Mandatory=$False)]
    [ValidateScript({ $_ -ge 0 })]
    [int]
    $Offset
)
    $sb = new-object System.Text.StringBuilder
    [void]$sb.Append("https://api.pagerduty.com/$Resource")
    [void]$sb.Append("?limit=100")
    switch($PsBoundParameters.Keys) {
        "Since" {
            [void]$sb.Append("&since=" + [System.Uri]::EscapeDataString($Since.ToUniversalTime().ToString("O")))
        }
        "Until" {
            [void]$sb.Append("&until=" + [System.Uri]::EscapeDataString($Until.ToUniversalTime().ToString("O")))
        }
        "Offset" {
            [void]$sb.Append("&offset=" + [System.Uri]::EscapeDataString($Offset.ToString()))
        }
    }
    if($Includes.Count -gt 0) {
        $includesQueryStr = ''
        $Includes | Foreach-Object {
            # include[]=val
            $includesQueryStr += ("&" + [System.Uri]::EscapeDataString('include[]') + "=" + [System.Uri]::EscapeDataString($_))
        }
        if(![string]::IsNullOrEmpty($includesQueryStr)) {
            [void]$sb.Append($includesQueryStr)
        }
    }
    $sb.ToString()
}
function Get-PagerDutyRecords {
param(
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $Resource,
    [Parameter(Mandatory=$False)]
    [datetime]
    $Since,
    [Parameter(Mandatory=$False)]
    [datetime]
    $Until,
    [Parameter(Mandatory=$False)]
    [ValidateNotNullOrEmpty()]
    [string[]]
    $Includes = @()
)
    $ret = @()
    $pdSplat = @{
        Headers = @{ 'Authorization' = "Token token=$($PdRoApiKey)" }
        UseBasicParsing = $True
        ContentType = 'applicaion/json'
    }
    $resourceStem = $resource.TrimStart('/')
    $resourceName = $resourceStem.Split('/')[0]
    $collectionPropertyName = $resourceName
    
    $reqFmtArgs = @{
        Resource = $Resource
    }
    switch($PsBoundParameters.Keys) {
        'Since' { $reqFmtArgs['since'] = $Since }
        'Until' { $reqFmtArgs['until'] = $Until }
    }
    $reqUri = Format-PagerDutyResourceUri @reqFmtArgs
    $offset = 0
    try {
        $resp = Invoke-RestMethod -Uri $reqUri @pdSplat
        $respCount = $resp."$collectionPropertyName".Count
        if($respCount -gt 0) {
            write-verbose "[PD $($Resource)+$($offset)] $($respCount) Items"
            $ret += $resp."$collectionPropertyName"
            while($resp.more) {
                $offset += 100
                $reqFmtArgs['offset'] = $offset
                $reqUri = Format-PagerDutyResourceUri @reqFmtArgs
                $resp = Invoke-RestMethod -Uri $reqUri @pdSplat
                $respCount = $resp."$collectionPropertyName".Count
                if($respCount -gt 0) {
                    write-verbose "[PD $($Resource)+$($offset)] $($respCount) Items"
                    $ret += $resp."$collectionPropertyName"
                }
            }
        }
    }
    catch {
        $script:issueCount++
        $entry = "Cannot GET PagerDuty Resource $($Resource) Offset $($offset): "
        $entry += Format-ExceptionErrorRecord $_
        write-warning $entry
        Write-SyncLog -Entry $entry -Type 'error'
    }
    return $ret
}

# --- core functions ---
function Add-IncrementalPdReocrdsToSqltable {
param(
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $TableName,
    [Parameter(Mandatory=$True)]
    [object[]]
    $Records,
    [Parameter(Mandatory=$False)]
    [string[]]
    $SqlSkipColumns = @()
)
    $transactionSuccess = $False
    try {
        $generatedSqlTransactions = @(Get-SqlTransactionInsertFromJson -TableName $TableName -Records $Records -SqlSkipColumns $SqlSkipColumns)
        $transactionsCount = 0
        foreach($generatedSqlTransaction in $generatedSqlTransactions) {
            $transactionsCount++
            $script:newRowCount += $generatedSqlTransaction.rowcount
            Write-SyncLog -Entry "Table $($cleanTableName) INSERT TRANSACTION $($transactionsCount)/$($generatedSqlTransactions.Count) for $($generatedSqlTransaction.rowcount) rows spanning time range ($($rangeStr))"
            $null = Invoke-SqlCmd2 -Query $generatedSqlTransaction.sql -SqlParameters $generatedSqlTransaction.sqlparams -SqlConnection $SqlConnection
            $script:insertedRowCount += $generatedSqlTransaction.rowcount
        }
        $transactionSuccess = $True
    }
    catch {
        $script:issueCount++
        $entry = "Ex during sql trans for table $($cleanTableName): $($_.Exception.Message)"
        write-warning $entry
        Write-SyncLog -Entry $entry -Type 'error'
    }
    return $transactionSuccess
}
function Sync-IncrementalPdRecordsToSqlTable {
param(
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $TableName,
    [Parameter(Mandatory=$True,ParameterSetName='ByApi')]
    [ValidateNotNullOrEmpty()]
    [string]
    $PdResource,
    [Parameter(Mandatory=$False)]
    [string[]]
    $SqlSkipColumns = @(),
    [Parameter(Mandatory=$False)]
    [scriptblock]
    $RecordMutator = { $args[0] },
    [Parameter(Mandatory=$False)]
    [ValidateNotNullOrEmpty()]
    [string]
    $DateColumnName = 'created_at',
    [Parameter(Mandatory=$False)]
    [ValidateNotNullOrEmpty()]
    [string[]]
    $PagerDutyIncludes = @(),
    [Parameter(Mandatory=$False)]
    [hashtable]
    $ExtraMutators = @{}
)
    $cleanDbName = Get-CleanSqlName $SqlDatabaseName
    $cleanTableName = Get-CleanSqlName $TableName
    $cleanColumnName = Get-CleanSqlName $DateColumnName
    Write-SyncLog -Entry "Table $($cleanTableName) Incremental Sync Starting"
    # Calculate starting point for incremental updates
    $latest = $null
    $selectSql = "SELECT TOP 1 *
                  FROM [$($cleanDbName)].[dbo].[$($cleanTableName)]
                  ORDER BY [$($cleanColumnName)] DESC"
    $lastRecord = Invoke-SqlCmd2 -Query $selectSql -SqlConnection $SqlConnection -as psobject
    if($lastRecord -and $lastRecord.created_at) {
        [datetime]$latest = $lastRecord.created_at.date
        Write-SyncLog -Entry "Table $($cleanTableName) Column $($cleanColumnName) latest is: $($latest.ToUniversalTime().ToString('O'))"
    } else {
        $latest = $PdDefaultStartDate
        Write-SyncLog -Entry "Table $($cleanTableName) using default latest: $($latest.ToUniversalTime().ToString('O'))"
    }
    # Rewind a bit so we don't miss anything
    # then page in chunks, e.g. 24h windows
    $since = $latest.AddSeconds($IncrementalBufferSecs)
    $dtNow = get-date
    while($since -lt $dtNow) {
        $until = $since.AddSeconds($IncrementalAtomicUpdateWindowSec)
        $records = @()
        $missingRecords = @()
        $rangeStr = "$($since.ToUniversalTime().ToString('O')) to $($until.ToUniversalTime().ToString('O'))"
        
        # Query PD
        $getRecordsArgs = @{
            Resource = $PdResource
            Since = $since
            Until = $until
        }
        if($PagerDutyIncludes.Count -gt 0) {
            $getRecordsArgs['Includes'] = $PagerDutyIncludes
        }
        $records = @(Get-PagerDutyRecords @getRecordsArgs)

        # Find missing records
        if($records.count -gt 0) {
            $returnRecordsEntry = "$($PdResource) returned $($records.Count) TOTAL records spanning ($($rangeStr))"
            Write-SyncLog -Entry $returnRecordsEntry
            write-host $returnRecordsEntry -f green
            Write-SyncLog -Entry "Table $($cleanTableName) QUERYING $($records.count) IDs"
            $sqlIds = "'" + ($records.id -join "','") + "'"
            $cleanDbName = Get-CleanSqlName $SqlDatabaseName
            $selectExistingRecordSql = "SELECT Id FROM [$($cleanDbName)].[dbo].[$($cleanTableName)] WHERE Id IN ($($sqlIds));"
            $existingRecords = Invoke-SqlCmd2 -Query $selectExistingRecordSql -SqlConnection $SqlConnection -as psobject
            $existingReocrdIds = @()
            if($existingRecords) {
                $existingReocrdIds = $existingRecords.id
            }
            $missingRecords = @($records | where-object { $existingReocrdIds -notcontains $_.id })
        }
        if($missingRecords.count -gt 0) {
            # Add missing records
            $addRecordsResult = $False
            $missingRecordsEntry = "Table $($cleanTableName) has $($missingRecords.Count) MISSING records spanning ($($rangeStr))"
            Write-SyncLog -Entry $missingRecordsEntry
            write-host "  $($missingRecordsEntry)" -f yellow
            $mutatedRecords = @($missingRecords | foreach-object { $RecordMutator.Invoke($_) })
            $addRecordsResult = Add-IncrementalPdReocrdsToSqltable -TableName $cleanTableName -Records $mutatedRecords -SqlSkipColumns $SqlSkipColumns
            if(!$addRecordsResult) {
                break
            }
            # Add extra mutated records by table
            [string[]]$extraMutatorsKeys = $ExtraMutators.Keys
            foreach($k in $extraMutatorsKeys) {
                $addExtraMutationResult = $False
                $val = $ExtraMutators[$k]
                if($val -isnot [scriptblock]) {
                    write-warning "ExtraMutators key $($k) is of type $($val.GetType().FullName) rather than [scriptblock]"
                    continue
                }
                $cleanExtraTableName = Get-CleanSqlName -String $k
                $sb = $val
                $extraMutatedRecords = @($missingRecords | foreach-object { $sb.Invoke($_) })
                if($extraMutatedRecords.Count -gt 0) {
                    $extraMutationEntry = "Generated from PD Resource $($PdResource): $($extraMutatedRecords.Count) records for $($cleanExtraTableName)"
                    Write-SyncLog -Entry $extraMutationEntry
                    Write-Host "  $($extraMutationEntry)" -f yellow    
                    $addExtraMutationResult = Add-IncrementalPdReocrdsToSqltable -TableName $cleanExtraTableName -Records $extraMutatedRecords -SqlSkipColumns $SqlSkipColumns
                    if(!$addExtraMutationResult) {
                        break
                    }
                }
            }
        }
        # Next
        $since = $until
    }
    Write-SyncLog -Entry "Table $($cleanTableName) Incremental Sync Finished"
}
function Sync-PdRecordsToSqlTable {
param(
    [Parameter(Mandatory=$True)]
    [ValidateNotNullOrEmpty()]
    [string]
    $TableName,
    [Parameter(Mandatory=$True,ParameterSetName='ByApi')]
    [ValidateNotNullOrEmpty()]
    [string]
    $PdResource,
    [Parameter(Mandatory=$True,ParameterSetName='ByRecords')]
    [psobject[]]
    $Records,
    [Parameter(Mandatory=$False)]
    [string[]]
    $SqlSkipColumns = @(),
    [switch]$ReturnRecords
)
    $recordsToInsert = @()
    switch($PsCmdlet.ParameterSetName) {
        'ByApi' { 
             $recordsToInsert = @(Get-PagerDutyRecords -Resource $PdResource)
        }
        'ByRecords' {
            $recordsToInsert = $Records
        }
    }
    $tableEntry = "Syncing Table $($TableName) syncing $($recordsToInsert.Count) records"
    Write-SyncLog -Entry $tableEntry
    write-host $tableEntry -f green
    Clear-SqlTable -TableName $TableName
    if($recordsToInsert.Count -eq 0) {
        return
    }
    Add-PdRecordToSqlTable -TableName $TableName -SqlSkipColumns $SqlSkipColumns -Records $recordsToInsert
    if($ReturnRecords) {
        return $recordsToInsert
    }
}
# --- transformation and flattening ---
function Sync-PdEscalationPoliciesData {
    $escalationPolicies = Sync-PdRecordsToSqlTable -TableName 'escalation_policies' -PdResource 'escalation_policies' -ReturnRecords
    Write-SyncLog -Entry 'Calculating escalation_rules, escalation_rule_users, escalation_rule_schedules'
    $escalationRules = @()
    $escalationRuleUsers = @()
    $escalationRuleSchedules = @()
    foreach($escalationPolicy in $escalationPolicies) {
        $i = 0
        foreach($escalationRule in $escalationPolicy.escalation_rules) {
            $escalationRules += [pscustomobject]@{
                id = $escalationRule.id
                escalation_policy_id = $escalationPolicy.id
                escalation_delay_in_minutes = $escalationRule.escalation_delay_in_minutes
                level_index = $i+1
            }
            foreach($escalationRuleTarget in $escalationRule.targets) {
                switch($escalationRuleTarget.type) {
                    'user_reference' {
                        $escalationRuleUsers += [pscustomobject]@{
                            id = "$($escalationRule.id)_$($escalationRuleTarget.id)"
                            escalation_rule_id = $escalationRule.id
                            user_id = $escalationRuleTarget.id
                        }
                    }
                    default {
                        $escalationRuleSchedules += [pscustomobject]@{
                            id = "$($escalationRule.id)_$($escalationRuleTarget.id)"
                            escalation_rule_id = $escalationRule.id
                            schedule_id = $escalationRuleTarget.id
                        }
                    }
                }
            }
            $i++
        }
    }
    Sync-PdRecordsToSqlTable -TableName 'escalation_rules' -Records $escalationRules
    Sync-PdRecordsToSqlTable -TableName 'escalation_rule_users' -Records $escalationRuleUsers
    Sync-PdRecordsToSqlTable -TableName 'escalation_rule_schedules' -Records $escalationRuleSchedules
}
function Sync-PdSchedulesData {
    $schedules = Sync-PdRecordsToSqlTable -TableName 'schedules' -PdResource 'schedules' -ReturnRecords
    Write-SyncLog -Entry 'Calculating values for user_schedule'
    $userSchedules = @()
    foreach($schedule in $schedules) {
        foreach($user in $schedule.users) {
            $userSchedules += [pscustomobject]@{
                id = "$($schedule.id)_$($user.id)"
                user_id = $user.id
                schedule_id = $schedule.id
            }
        }
    }
    Sync-PdRecordsToSqlTable -TableName 'user_schedule' -Records $userSchedules
}
function Sync-PdIncidentsAndLogEntries {
    [scriptblock]$incidentsMutator = {
        $record = $args[0]
        [pscustomobject]@{
            id = $record.id
            incident_number = $record.incident_number
            created_at = $record.created_at
            html_url = $record.html_url
            incident_key = $record.incident_key
            service_id = $record.service.id
            escalation_policy_id = $record.escalation_policy.id
            trigger_summary_subject = $record.summary
            trigger_summary_description = $record.description
            first_trigger_log_entry_id = $record.first_trigger_log_entry.id
        }
    }
    [scriptblock]$logEntryMutator = {
        $record = $args[0]
        [pscustomobject]@{
            id = $record.id
            type = $record.type
            created_at = $record.created_at
            incident_id = $record.incident.id
            agent_type = $record.agent.type
            agent_id = $record.agent.id
            channel_type = $record.channel.type
            user_id = $record.user.id
            notification_type = $record.notification.type
            assigned_user_id = $record.assigned_user.id
            escalation_policy_id = $record.escalation_policy.id
        }
    }
    $incidentExtraMutators = @{
        'incident_involved_teams' = {
            $record = $args[0]
            foreach($team in $record.teams) {
                [pscustomobject]@{
                    id = "$($record.id)_$($team.id)"
                    incident_id = $record.id
                    source = 'incident'
                    source_type = $null
                    team_id = $team.id
                }
            }
        }
    }
    $logEntryExtraMutators = @{
        'incident_involved_teams' = {
            $record = $args[0]
            foreach($team in $record.escalation_policy.teams) {
                [pscustomobject]@{
                    id = "$($record.id)_$($team.id)"
                    incident_id = $record.incident.id
                    source = 'log_entries'
                    source_type = $record.type
                    team_id = $team.id
                }
            }
        }
    }
    $script:timings['IncrementalSync_incidents'].Start()
    Sync-IncrementalPdRecordsToSqlTable -TableName 'incidents' -PdResource 'incidents' -RecordMutator $incidentsMutator -ExtraMutators $incidentExtraMutators
    $script:timings['IncrementalSync_incidents'].Stop()

    $script:timings['IncrementalSync_logentries'].Start()
    Sync-IncrementalPdRecordsToSqlTable -TableName 'log_entries' -PdResource 'log_entries' -RecordMutator $logEntryMutator -ExtraMutators $logEntryExtraMutators -PagerDutyIncludes @('incident','teams')
    $script:timings['IncrementalSync_logentries'].Stop()
}

#######################################
# init
#######################################
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# SQL
. $PsScriptRoot\Invoke-SqlCmd2.ps1
write-verbose "Checking SQL Connection"
$isSqlConnValid = $False
try {
    $null = Invoke-SqlCmd2 -SqlConnection $SqlConnection -Query 'SELECT getdate()'
    $isSqlConnValid = $True
}
catch {
    write-warning "Ex Checking SQL Connection: $($_.Exception.Message)"
}
if(!$isSqlConnValid) {
    if($ExitOnIssues) {
        [environment]::Exit(1)
    }
    throw "Could not validate sql connection"
}

# Purge sync data
if($PurgeSyncMetadata) {
    $truncateSuccess = $False
    try {
        Clear-SqlTable -TableName 'synclogs' -NoLog
        Clear-SqlTable -TableName 'syncruns' -NoLog
        $truncateSuccess = $True
    }
    catch {
        write-warning "Ex purging metadata tables: $($_.Exception.Message)"
    }
    if(!$truncateSuccess) {
        if($ExitOnIssues) {
            [environment]::Exit(2)
        }
        throw "Issues purging metadata tables"
    }
}

# New SyncRun
$script:SyncRunId = [guid]::NewGuid().Guid
write-host "SyncRunId: $($script:SyncRunId)" -f cyan
Write-SyncLog "New Sync Run Started: $($SyncRunId)"
$paramsStr = @()
$paramsStr += "IncrementalUpdateOnly=$($IncrementalUpdateOnly)"
$paramsStr += "PurgeIncrementalTables=$($PurgeIncrementalTables)"
$paramsStr += "PurgeSyncMetadata=$($PurgeSyncMetadata)"
$paramsStr += "IncrementalAtomicUpdateWindowSec=$($IncrementalAtomicUpdateWindowSec)"
$paramsStr += "PagerDutyStartEpoch=$($PdDefaultStartDate)"
$syncRunInitSuccess = $False
try {
    New-SyncRunEntry
    Write-SyncLog "PARAMS: $($paramsStr -join ';')"
    if($PurgeIncrementalTables) {
        Clear-SqlTable -TableName 'incidents'
        Clear-SqlTable -TableName 'log_entries'
    }
    $syncRunInitSuccess = $True
}
catch {
    write-warning "Ex Initializing SyncRun: $($_.Exception.Message)"
}
if(!$syncRunInitSuccess) {
    if($ExitOnIssues) {
        [environment]::Exit(3)
    }
    throw "Could not initialize new SyncRun"
}

#######################################
# main
#######################################
$script:issueCount = 0
$script:newRowCount = 0
$script:insertedRowCount = 0
$script:timings = ([ordered]@{
    'total' = [System.Diagnostics.Stopwatch]::StartNew()
    'CompleteSync' = [System.Diagnostics.Stopwatch]::new()
    'IncrementalSync' = [System.Diagnostics.Stopwatch]::new()
    'IncrementalSync_logentries' = [System.Diagnostics.Stopwatch]::new()
    'IncrementalSync_incidents' = [System.Diagnostics.Stopwatch]::new()
})
try {
    # Complete Sync
    if(!$IncrementalUpdateOnly) {
        $script:timings['CompleteSync'].Start()
        Sync-PdRecordsToSqlTable -TableName 'users' -PdResource 'users'
        Sync-PdRecordsToSqlTable -TableName 'teams' -PdResource 'teams'
        Sync-PdRecordsToSqlTable -TableName 'services' -PdResource 'services'
        Sync-PdSchedulesData
        Sync-PdEscalationPoliciesData
        $script:timings['CompleteSync'].Stop()
    }
    
    # Incremental Updates
    $script:timings['IncrementalSync'].Start()
    Sync-PdIncidentsAndLogEntries
    $script:timings['IncrementalSync'].Stop()
    
    # Processing Done
    Write-SyncLog -Entry "Processing Done"
    write-host "Processing Done" -f white -b darkgreen
}
catch {
    $issueCount++
    $entry = Format-ExceptionErrorRecord $_
    write-host $entry -f white -b red
    Write-SyncLog -Entry $entry -Type 'error'
}

# summary
$summaryEntry = "$($script:insertedRowCount)/$($script:newRowCount) records inserted"
write-host $summaryEntry -f yellow -b darkgray
Write-SyncLog -Entry $summaryEntry
$script:timingstrs = @()
$script:timings.Keys | foreach-object {
    $v = $script:timings[$_].Elapsed
    $script:timingstrs += "$($_):$($v)"
    write-host "  $($_): $($v)" -f yellow
}
$timingsEntry = "TIMINGS: $($script:timingstrs -join ' ')"
Write-SyncLog -Entry $timingsEntry

# Log Issues
$entry = "$($script:issueCount) Issues During Processing"
Write-SyncLog -Entry $entry
if($script:issueCount -gt 0) {
    Update-SyncRunEntryAsFinished
    write-host $entry -f white -b darkyellow
} else {
    Update-SyncRunEntryAsFinished -SetSuccessful
    write-host $entry -f white -b darkgreen
}

# Most recent log query
if($SkipFinalErrorLogQuery) {
    return
}
try {
    $cleanDbName = Get-CleanSqlName $SqlDatabaseName
    $logs = Invoke-SqlCmd2 -Query "EXEC [$($cleanDbName)].[dbo].[sp_GetLatestLogs]" -SqlConnection $SqlConnection -as psobject
    $logs | where-object { $_.Type -eq 'error' }
}
catch {
    write-verbose "Ex: $($_.Exception.Message)"
}

# Exit on issues
if($ExitOnIssues) {
    if($script:issueCount -ne 0) {
        [environment]::Exit(-1)
    }
}