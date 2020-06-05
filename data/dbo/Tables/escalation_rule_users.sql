CREATE TABLE [dbo].[escalation_rule_users] (
    [id]                 VARCHAR (255) NOT NULL,
    [escalation_rule_id] VARCHAR (255) NOT NULL,
    [user_id]            VARCHAR (255) NULL,
    PRIMARY KEY CLUSTERED ([id] ASC)
);

