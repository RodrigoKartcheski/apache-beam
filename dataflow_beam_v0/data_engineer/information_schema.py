SELECT

 job_id,

 creation_time,

 error_result,

 start_time

FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT

where state != 'DONE'

and start_time is not null