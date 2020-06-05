CREATE TABLE [dbo].[SyncRuns] (
    [SyncRunId]    UNIQUEIDENTIFIER   NOT NULL,
    [StartTime]    DATETIMEOFFSET (7) NULL,
    [EndTime]      DATETIMEOFFSET (7) NULL,
    [IsFinished]   BIT                DEFAULT ((0)) NOT NULL,
    [IsSuccessful] BIT                DEFAULT ((0)) NOT NULL,
    PRIMARY KEY CLUSTERED ([SyncRunId] ASC)
);


GO
CREATE NONCLUSTERED INDEX [IX_SyncRuns_StartTimeDsc]
    ON [dbo].[SyncRuns]([StartTime] DESC);

