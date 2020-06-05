-- original: https://github.com/stripe/pd2pg/blob/master/examples/individual-detail.sql
USE pagerdutysql
GO

DECLARE @daysOld int, @email NVARCHAR(255);
SET @daysOld = -28;
SET @email = 'jkang@ABCCORP.com'

select  incidents.html_url as incident_url,
        log_entries.created_at at time zone 'Central Standard Time' as notification_time_cst,
        log_entries.notification_type as notification_type,
        incidents.trigger_summary_subject,
        services.name as service_name
from users,log_entries,incidents,services
where   users.email = @email and
        log_entries.user_id = users.id and
        log_entries.type = 'notify_log_entry' and
        log_entries.created_at > TODATETIMEOFFSET(DATEADD(DAY,@daysOld,getdate()), 0) and
        incidents.id = log_entries.incident_id and
        incidents.service_id = services.id
order by log_entries.created_at desc;