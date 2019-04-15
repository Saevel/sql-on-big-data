CREATE EXTERNAL TABLE SALARY_DATA(employeeId BIGINT, salary DOUBLE)
STORED AS PARQUET
-- TODO: Add actual file path
LOCATION '/user/mapr/parquet';