SQL on Big Data

Welcome the the "SQL on Big Data" course exercises. We will put the theoretical part of the training to practice and
make sure we know how to process massive datasets on Hadoop platforms using the SQL language.

0. Preparation

    Pull the latest version of Cloudera Quickstart Docker Image by calling: "docker pull cloudera/quickstart:lastest"
    
    WARNING: You need a Docker installation with a least 8 GB RAM allocated to it on your machine to be able to do this.
    
    Add the following mapping to your "hosts" file: "127.0.0.1 quickstart.cloudera quickstart localhost localhost.domain"
    
    Create a directory on your machine, which will server as a "proxy disc" for the Cloudera Image.
    
    Run the image with: 
    
    docker run --name=cloudera --hostname=quickstart.cloudera --privileged=true -t -i --memory="8g" -p 8888:8888 
    -p 18080:18080 -p 18081:18081 -p 7078:7078 -p 7077:7077 -p 8032:8032 -p 8088:8088 -p 8042:8042 -p 8020:8020 
    -p 8022:8022 -p 50470:50470 -p 50070:50070 -p 50010:50010 -p 50020:50020 -p 50075:50075 -p 50090:50090 -p 50495:50495
    -p 9092:9092 -p 2181:2181 -p 7180:7180 -p 19888:19888 -p 9083:9083 
    -v <proxy_disc_path>:/gft-host cloudera/quickstart:latest /usr/bin/docker-quickstart

    Where <proxy_disc_path> is the path to the special directory we created to serve as Cloudera's proxy disc.
    
    Access Hue on via "localhost:8888" and log in with the cloudera / cloudera credentials. 

1. Take the Avro data from the "src/main/resources/employees" folder and use Hue to place it in a chosen directory on HDFS. Then
   create a new table called "EMPLOYEES" in Hive / Impala through Hue with the following fields:
    * id (BIGINT)
    * name (STRING)
    * surname (STRING)
    * age (INT)
    
   bucketed into 16 buckets using the "age" field.
    
   Use the "LOAD DATA INPATH" command to load data from the HDFS path of your choice to this table. 
   
   In Hue, write a query, that will use the EMPLOYEES table to check for 10 youngest employees in the whole company and
   display the results in Hue query editor.
   
2. Take the CSV data from the "src/resoures/employee_details" folder and use Hue to place it in a chosen directory on HDFS,
   so that data is partitioned physically by "country" and "department" columns.     
   
   Create an external table that will point to this data called "EMPLOYEE_DETAILS".
   
   Write a HQL query that for each country and each department, will calculate the number of employees working for it as
   "employee_count".
   
3. Take the Parquet data from the "src/resources/salary_data" folder and use Hue to place it in a chosen directory on 
   HDFS. Declare an external table called "SALARY_DATA" on that table.     
   
4. Inside the "prv.saevel.bigdata.sql.udf.IsEuCountryUdf" class, implement a Hive UDF function, which for a given Text 
   field (Hadoop wrapper for a String) will check if the text field contains the name of a country from the EuropeanUnion.
   You can look up the names of those countries in the EuropeanUnion.members field. Run the "IsEuCountryUdfTest" test 
   class to verify the correctness of your implementation.
   
5. Inside the "prv.saevel.bigdata.sql.udaf.UniqueIdUDAF" class, implement a hive UDAF function, which for a given long
   field checks if all the ids that appear on that field are actually unique over the entire dataset and returns a 
   Boolean as an indicator. Run the "UniqueIdUDAFTest" test class to verify the correctness of your implementation.
   
6. Write a HQL script, to be run from console, to do the following: 

   * Take the external params: "top-earners", "top-experience", "top-both", all numeric
   
   * for each department and country in the company, find top n employees in that department and country that have the 
   highest salary and store their: employeeId, name, surname, department, country and salary in the "TOP_EARNERS" table,
   stored as Avro files and partitioned by department and country, where n is equal to the value passed in the "top-earners" 
   parameter.
   
   * for each department and country in the company, find top m employees in that department and country that have the
   highest number of years of experience and store their: employeeId, name, surname, department, country and years of 
   experience in the "TOP_EXPERIENCED" table, stored as Avro files and partitioned by department, country, where m is equal
   to the value passed in the "top-experience" parameter.
   
   * for employees in the "TOP_EARNERS" and "TOP_EXPERIENCED" tables, find the k top employees that appear in both of them
   and store their: employeeId, name, surname, department, country, salary and years of experience in the "TOP_BOTH" 
   table, stored as ORC files and partitioned by department and country, where k is equal to the value passed in the 
   "top-both" input parameter.