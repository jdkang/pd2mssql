CREATE TABLE [dbo].[escalation_rules] (
    [id]                          VARCHAR (255) NOT NULL,
    [escalation_policy_id]        VARCHAR (255) NOT NULL,
    [escalation_delay_in_minutes] INT           NULL,
    [level_index]                 INT           NULL,
    PRIMARY KEY CLUSTERED ([id] ASC)
);

