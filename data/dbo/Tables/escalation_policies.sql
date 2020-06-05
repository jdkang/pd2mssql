CREATE TABLE [dbo].[escalation_policies] (
    [id]        VARCHAR (255) NOT NULL,
    [name]      VARCHAR (255) NOT NULL,
    [num_loops] INT           NOT NULL,
    PRIMARY KEY CLUSTERED ([id] ASC)
);

