CREATE TABLE [dbo].[teams]
(
	[Id] VARCHAR(255) NOT NULL PRIMARY KEY, 
    [name] VARCHAR(255) NOT NULL, 
    [description] VARCHAR(MAX) NULL, 
    [type] VARCHAR(255) NOT NULL, 
    [summary] VARCHAR(255) NOT NULL, 
    [html_url] VARCHAR(255) NULL, 
    [default_role] VARCHAR(255) NULL, 
    [parent] VARCHAR(255) NULL
)
