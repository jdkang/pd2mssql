CREATE TABLE [dbo].[log_entries] (
    [id]                VARCHAR (255)      NOT NULL,
    [type]              VARCHAR (255)      NOT NULL,
    [created_at]        DATETIMEOFFSET (7) NOT NULL,
    [incident_id]       VARCHAR (255)      NOT NULL,
    [agent_type]        VARCHAR (255)      NULL,
    [agent_id]          VARCHAR (255)      NULL,
    [channel_type]      VARCHAR (255)      NULL,
    [user_id]           VARCHAR (255)      NULL,
    [notification_type] VARCHAR (255)      NULL,
    [assigned_user_id]  VARCHAR (255)      NULL,
    [escalation_policy_id] VARCHAR(255) NULL, 
    PRIMARY KEY CLUSTERED ([id] ASC)
);

