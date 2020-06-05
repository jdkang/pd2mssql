/*
 Pre-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be executed before the build script.	
 Use SQLCMD syntax to include a file in the pre-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the pre-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------
*/
-- SQL Login: Syncinjg scripts
if not exists(select 1 from sys.server_principals where name = '$(sqlloginuser)')
    CREATE LOGIN [$(sqlloginuser)]
        WITH PASSWORD = '$(sqlloginpassword)'
            ,DEFAULT_DATABASE = [master]
            ,DEFAULT_LANGUAGE = [us_english];
-- SQL Login: Consuming database (read-only)
if not exists(select 1 from sys.server_principals where name = '$(sqlconsumerloginuser)')
    CREATE LOGIN [$(sqlconsumerloginuser)]
        WITH PASSWORD = '$(sqlconsumerloginpassword)'
            ,DEFAULT_DATABASE = [master]
            ,DEFAULT_LANGUAGE = [us_english];

-- EXAMPLE LOGONS
-- PLEASE DELETE/MODIFY TO MATCH YOUR NEEDS
if not exists(select 1 from sys.server_principals where name = 'ABCCORP\Developers')
    CREATE LOGIN [ABCCORP\Developers] FROM WINDOWS;

if not exists(select 1 from sys.server_principals where name = 'ABCCORP\SRE')
    CREATE LOGIN [ABCCORP\SRE] FROM WINDOWS;

if not exists(select 1 from sys.server_principals where name = 'ABCCORP\QA Engineers')
    CREATE LOGIN [ABCCORP\QA Engineers] FROM WINDOWS;