CREATE EXTERNAL TABLE SALARY_DATA(employeeId BIGINT, salary DOUBLE) STORED AS PARQUET LOCATION '/user/cloudera/salaryData' TBLPROPERTIES('parquet.compression'='snappy');

SELECT salary FROM SALARY_DATA ORDER BY salary LIMIT 10;