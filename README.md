Port of [pd2pg](https://github.com/stripe/pd2pg) written in Powershell targeting MSSQL.

This project was created because PD still lacks decent analytics. The paid analytics package _might_ be okay, but this offers a free alternative.

It is my hope that this project can surface your teams very real battle against toil, burnout, and stress. Hopefully, it can also show how your team goes on to make on-call tolerable through a combination of analysis, high impact automation, and thoughtful post-mortems.

- [Setup](#Setup)
  - [Environment / Requirements](#Environment--Requirements)
- [About the data](#About-the-data)
  - [Table Types](#Table-Types)
  - [log_entries table](#logentries-table)
  - [incident_involved_teams table](#incidentinvolvedteams-table)
- [Database Deployment](#Database-Deployment)
  - [DACPAC](#DACPAC)
  - [Direct schema creation](#Direct-schema-creation)
- [Running the Script](#Running-the-Script)
  - [Example Args](#Example-Args)
  - [Other useful args](#Other-useful-args)
- [Development and TODO](#Development-and-TODO)
  - [TODO](#TODO)

# Setup
## Environment / Requirements
- Powershell 5.1 (though might work with PS Core)
- Windows + .NET 4.5+ (though might work on different OS)

It might not be a lot of work to get thsi running on Linux/MacOS but hasn't yet been tested.

# About the data
## Table Types
Tables come in 3 major types:
- **Full Sync** tables (truncated/repopulated every time)
- **Incremental Sync** tables which will try to use the "lastest" record as a reference point and rewind/replay from there.
  - `incidents` table
  - `log_entries` table

Some tables are direct binds onto the raw API data and others are tranforms/normalized into new tables.

## log_entries table
when looking for "pages" there are two types of log entries of main concern:
- `responder_request_for_escalation_policy_log_entry` - is a request for response on an existing page (e.g. by an incident commander). This is generally made to an "escalation policy"
- `notify_log_entry` - is a direct page out to a user

## incident_involved_teams table
To make referencing teams easier, the table `incident_involved_teams` is genearted while syncing `incidents` and `log_entries`. 

- `source` = `incidents|log_entries`
- `source_type` = `null|<log_entry_type>`
- `team_id`

# Database Deployment
Database DACPAC in `data\`

## DACPAC
The DACPAC is useful because it can be modified to easily scaffold permissions and make deployments easier (assuming you have the tooling installed).

- `local.publish.xml` - targets `.\SQLSERVER2016` instance with windows auth
- `octopus.publish.xml` - contains Octopus `#{replacement}` style variables
  - `#{pagerduty.loginuser}`
  - `#{pagerduty.consumer.loginuser}`
  - `#{pagerduty.loginpassword}`
  - `#{pagerduty.consumer.loginpassword}`

The DACPAC will create (2) users: a `db_owner` user meant for the sync process and a "consumer" user meant for read/sproc execute.

**Make sure** to modify the following files below "EXAMPLE" comments to match your needs. These include boilerplate examples for extending access to the database programatically.
- `Script.PostDeployment.sql`
- `Script.PreDeployment.sql`

## Direct schema creation
You can also directly create the schema using the files in `data/dbo/**.sql`

# Running the Script
## Example Args
```
# example using a local instance
$pdMssqlArgs = @{
    SqlConnection = "Data Source=.\SQLSERVER2016;Initial Catalog=pagerdutysql;Integrated Security=true"
    PdRoApiKey = 'xxxxxx-xxxxxx'
    # ...
}
Sync-Pd2Mssql.ps1 @pdMssqlArgs
```
## Other useful args
- `$PdDefaultStartDate = '2012-01-01T00:00Z'` - Change when incremental start for the first time. Setting this closer to when you first started using PD can increase the initial sync.
- `IncrementalUpdateOnly = $True` - Skip truncating and repopulating services/users/etc and only update incremental data.
- `ExitOnIssues = $True` - This is useful for reporting errors back when running inside a CI system like Octopus or Jenkins

# Development and TODO
## TODO
- Port [examples](https://github.com/stripe/pd2pg/tree/master/examples) and other queries to sproc
- Shore up any new API changes/diffs.

