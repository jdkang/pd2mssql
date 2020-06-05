CREATE TABLE [dbo].[incidents] (
    [id]                          VARCHAR (255)      NOT NULL,
    [incident_number]             INT                NOT NULL,
    [created_at]                  DATETIMEOFFSET (7) NOT NULL,
    [html_url]                    VARCHAR (255)      NOT NULL,
    [incident_key]                VARCHAR (255)      NULL,
    [service_id]                  VARCHAR (255)      NULL,
    [escalation_policy_id]        VARCHAR (255)      NULL,
    [trigger_summary_subject]     VARCHAR (8000)     NULL,
    [trigger_summary_description] VARCHAR (8000)     NULL,
    [trigger_type]                VARCHAR (255)      NULL,
    [first_trigger_log_entry_id] VARCHAR(255) NULL, 
    PRIMARY KEY CLUSTERED ([id] ASC)
);

