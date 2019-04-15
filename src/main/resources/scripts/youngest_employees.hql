CREATE TABLE EMPLOYEES(id BIGINT, name STRING, age INT)
CLUSTERED BY(age) INTO 16 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat';

-- TODO: Set actual locatgion
LOAD DATA INPATH 'path/to/avro_file' OVERWRITE INTO TABLE EMPLOYEES;

SELECT id, name, surname, age FROM EMPLOYEES AS e ORDER BY age ASC LIMIT 10;