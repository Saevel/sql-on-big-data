CREATE EXTERNAL TABLE EMPLOYEE_DETAILS(employeeId BIGINT, experience INT)
PARTITIONED BY (department STRING, country STRING)
ROW FORMAT DELIMITED
STORED AS TEXTFILE
-- TODO: Add actual location on copy
LOCATION '/path/to/dataFile/';

SELECT department, country, COUNT(*) as employee_count FROM EMPLOYEE_DETAILS GROUP BY department, country;