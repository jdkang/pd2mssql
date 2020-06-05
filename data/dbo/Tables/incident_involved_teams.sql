CREATE TABLE [dbo].[incident_involved_teams]
(
	[Id] VARCHAR(255) NOT NULL PRIMARY KEY, 
    [incident_id] VARCHAR(255) NOT NULL, 
    [source] VARCHAR(255) NOT NULL, 
    [source_type] VARCHAR(255) NULL, 
    [team_id] VARCHAR(255) NOT NULL
)
