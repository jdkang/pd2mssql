CREATE TABLE [dbo].[user_schedule] (
    [id]          VARCHAR (255) NOT NULL,
    [user_id]     VARCHAR (255) NULL,
    [schedule_id] VARCHAR (255) NULL,
    PRIMARY KEY CLUSTERED ([id] ASC)
);

