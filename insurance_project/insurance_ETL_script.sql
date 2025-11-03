create or replace database insurance_db;

use database insurance_db;

create or replace schema insurance_schema;

--- create a atbel definetion  for loading the data into  table ---
create or replace table insurance_table(
PolicyNumber varchar(10),
CustomerID	varchar(10),
Gender	char(10),
Age	int,
PolicyType  char(20),
PolicyStartDate date,
PolicyEndDate	date,
PremiumAmount	float,
CoverageAmount	float,
ClaimNumber	varchar(10),
ClaimDate	date,
ClaimAmount float,
ClaimStatus varchar(20)
);

desc table insurance_table;


--- creating the file format to ingest the data into table ---
create or replace file format insurance_csv
type = 'CSV'
field_delimiter =','
skip_header =1
field_optionally_enclosed_by = '"';

desc file format insurance_csv;

---integration object ----
create or replace storage integration insurance_integration_s3
type= 'EXTERNAL_STAGE'
enabled = TRUE
storage_provider = 'S3'
storage_aws_role_arn='arn:aws:iam::008971663025:role/insurancecsv'
storage_allowed_locations=('s3://insurancecsv/datasets/')
comment = ' setting up the integration';

desc storage integration insurance_integration_s3;

----CREATE a stage ------
create or replace stage insurace_stage
url = 's3://insurancecsv/datasets/'
file_format = insurance_csv
storage_integration =insurance_integration_s3
comment = 'satge to load the data from s3 to stage';

desc stage insurace_stage;

----LIST all the files present in the S3-----
list @INSURANCE_DB.INSURANCE_SCHEMA.INSURACE_STAGE;


--- type casting the date fields----

select $1,$2,$3,$4,$5, to_date($6,'DD-MM-YYYY'),to_date($7,'DD-MM-YYYY'),$8,$9,$10,
when $11 !='NULL' then to_date($11,'DD-MM-YYYY')
end,$12,$13,
from @INSURANCE_DB.INSURANCE_SCHEMA.INSURACE_STAGE;


----pipe and copy STATEMENT----
create or replace pipe insurance_pipe 
auto_ingest = true as 
copy into insurance_table
from (
select $1,$2,$3,$4,$5, to_date($6,'DD-MM-YYYY'),to_date($7,'DD-MM-YYYY'),$8,$9,$10,case 
when $11 !='NULL' then to_date($11,'DD-MM-YYYY')
end,$12,$13
from @INSURANCE_DB.INSURANCE_SCHEMA.INSURACE_STAGE
)
file_format = (format_name=INSURANCE_CSV);


-----refresh the pipe-----
alter pipe insurance_pipe refresh;

select* from insurance_table;

-----  add arn notification into S3 SQS -----
show pipes;


---shows the status of pipe---
SELECT SYSTEM$PIPE_STATUS('insurance_pipe');

----- show the errors in the pipe -----
select * from table(validate_pipe_load(
  pipe_name=>'INSURANCE_DB.INSURANCE_SCHEMA.INSURANCE_PIPE',
  start_time=>dateadd(hour, -1, current_timestamp())));



show columns in table  insurance_table;

desc table insurance_table;

