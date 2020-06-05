SELECT *
FROM [pagerdutysql].[dbo].[SyncLogs]
WHERE [SyncRunId] = (SELECT TOP 1 [SyncRunId]
                     FROM [pagerdutysql].[dbo].[SyncRuns]
                     ORDER BY [StartTime] DESC)
ORDER BY [DatetimeOffset] DESC