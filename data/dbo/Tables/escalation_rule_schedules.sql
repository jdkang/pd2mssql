CREATE TABLE [dbo].[escalation_rule_schedules] (
    [id]                 VARCHAR (255) NOT NULL,
    [escalation_rule_id] VARCHAR (255) NOT NULL,
    [schedule_id]        VARCHAR (255) NULL,
    PRIMARY KEY CLUSTERED ([id] ASC)
);

