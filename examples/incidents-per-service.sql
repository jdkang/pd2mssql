-- original: https://github.com/stripe/pd2pg/blob/master/examples/incidents-per-service.sql
use pagerdutysql
GO

DECLARE @daysOld int;
SET @daysOld = -28;

select  services.name,
        count(incidents.id) AS incident_count
from incidents,services
where incidents.created_at > TODATETIMEOFFSET(DATEADD(DAY,@daysOld,getdate()), 0)
      AND incidents.service_id = services.id
group by services.name
order by count(incidents.id) desc;
