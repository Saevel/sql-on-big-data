CREATE TABLE EMPLOYEES(id BIGINT, name STRING, surname STRING, age INT) CLUSTERED BY(age) INTO 16 BUCKETS STORED AS AVRO;

LOAD DATA INPATH '/user/cloudera/employees/employees.avro' INTO TABLE employees;

SELECT * FROM employees ORDER BY age LIMIT 10;