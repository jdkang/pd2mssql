
CREATE PROCEDURE sp_GetLatestLogs
AS
SELECT *
FROM [dbo].[SyncLogs]
WHERE [SyncRunId] = (SELECT TOP 1 [SyncRunId]
					FROM [dbo].[SyncRuns]
					ORDER BY [StartTime] DESC)
ORDER BY [DateTimeOffset] DESC