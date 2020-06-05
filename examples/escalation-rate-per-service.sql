-- original: https://github.com/stripe/pd2pg/blob/master/examples/escalation-rate-per-service.sql
use pagerdutysql
GO

DECLARE @daysOld int;
SET @daysOld = -28;

with incident_counts as (
    select  i.id as incident_id,
            i.service_id,
            (select count(*)
                from log_entries
                where log_entries.incident_id = i.id 
                    and log_entries.type = 'escalate_log_entry'
            ) as escalations
    from
        incidents as i
    where
        i.created_at > TODATETIMEOFFSET(DATEADD(DAY,@daysOld,getdate()), 0)
    group by
      i.id,
      i.service_id
),
counts as (
    select  incident_counts.service_id,
            count(incident_counts.incident_id) as incidents,
            sum(incident_counts.escalations) as escalations
    from
      incident_counts
    group by
      incident_counts.service_id
)

select  services.name as service,
        round(counts.escalations / counts.incidents, 1) as escalation_rate,
        counts.incidents as incidents,
        counts.escalations as escalations
from services,counts
where services.id = counts.service_id
order by escalation_rate desc;

