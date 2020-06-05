/*
Post-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be appended to the build script.		
 Use SQLCMD syntax to include a file in the post-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the post-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------
*/
-- database logins and rights
USE [$(DatabaseName)]

-- SQL User: Syncinjg scripts
if not exists(select 1 from sys.database_principals
			  where name = '$(sqlloginuser)')
BEGIN
    CREATE USER [$(sqlloginuser)]
		FOR LOGIN [$(sqlloginuser)]
		WITH DEFAULT_SCHEMA = [dbo];
END
EXEC sys.sp_addrolemember 'db_owner', '$(sqlloginuser)'
-- SQL User: Consuming database (read-only)
if not exists(select 1 from sys.database_principals
			  where name = '$(sqlconsumerloginuser)')
BEGIN
    CREATE USER [$(sqlconsumerloginuser)]
		FOR LOGIN [$(sqlconsumerloginuser)]
		WITH DEFAULT_SCHEMA = [dbo];
END
EXEC sys.sp_addrolemember 'db_datareader', '$(sqlconsumerloginuser)'
GRANT EXECUTE TO [$(sqlconsumerloginuser)]

-- EXAMPLE USERS AND PERMISSIONS
-- PLEASE DELETE/MODIFY TO MATCH YOUR NEEDS
if not exists(select 1 from sys.database_principals
			  where name = 'ABCCORP\Developers')
BEGIN
    CREATE USER [ABCCORP\Developers]
		FOR LOGIN [ABCCORP\Developers]
		WITH DEFAULT_SCHEMA = [dbo];
END
EXEC sys.sp_addrolemember 'db_datareader', 'ABCCORP\Developers'
GRANT EXECUTE TO [ABCCORP\Developers]

if not exists(select 1 from sys.database_principals
			  where name = 'ABCCORP\SRE')
BEGIN
    CREATE USER [ABCCORP\SRE]
		FOR LOGIN [ABCCORP\SRE]
		WITH DEFAULT_SCHEMA = [dbo];
END
EXEC sys.sp_addrolemember 'db_datareader', 'ABCCORP\SRE'
GRANT EXECUTE TO [ABCCORP\SRE]

if not exists(select 1 from sys.database_principals
			  where name = 'ABCCORP\QA Engineers')
BEGIN
    CREATE USER [ABCCORP\QA Engineers]
		FOR LOGIN [ABCCORP\QA Engineers]
		WITH DEFAULT_SCHEMA = [dbo];
END
EXEC sys.sp_addrolemember 'db_datareader', 'ABCCORP\QA Engineers'
GRANT EXECUTE TO [ABCCORP\QA Engineers]