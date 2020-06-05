CREATE TABLE [dbo].[SyncLogs] (
    [Id]             INT                IDENTITY (1, 1) NOT NULL,
    [DateTimeOffset] DATETIMEOFFSET (7) NOT NULL,
    [Entry]          NVARCHAR (MAX)     NOT NULL,
    [SyncRunId]      UNIQUEIDENTIFIER   NULL,
    [Type]           NVARCHAR (50)      NULL,
    PRIMARY KEY CLUSTERED ([Id] ASC)
);

