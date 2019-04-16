SQL on Big Data

Welcome the the "SQL on Big Data" course exercises. We will put the theoretical part of the training to practice and
make sure we know how to process massive datasets on Hadoop platforms using the SQL language.

0. Preparation

    WARNING: You need a Docker installation with a least 8 GB RAM allocated to it on your machine to be able to do this.

    - Pull the latest version of Cloudera Quickstart Docker Image by calling: "docker pull cloudera/quickstart:lastest"
    
    - Add the following mapping to your "hosts" file: "127.0.0.1 quickstart.cloudera quickstart localhost localhost.domain"
    
    - Create a directory on your machine, which will server as a "proxy disc" for the Cloudera Image.
    
    - Copy all the contents of the "src/main/resources" folder in the "proxy disc folder" on your machine
    
    - Run the image with: 
    
        docker run --name=cloudera --hostname=quickstart.cloudera --privileged=true -t -i --memory="8g" -p 8888:8888 
        -p 18080:18080 -p 18081:18081 -p 7078:7078 -p 7077:7077 -p 8032:8032 -p 8088:8088 -p 8042:8042 -p 8020:8020 
        -p 8022:8022 -p 50470:50470 -p 50070:50070 -p 50010:50010 -p 50020:50020 -p 50075:50075 -p 50090:50090 -p 50495:50495
        -p 9092:9092 -p 2181:2181 -p 7180:7180 -p 19888:19888 -p 9083:9083 
        -v <proxy_disc_path>:/gft-host cloudera/quickstart:latest /usr/bin/docker-quickstart

    where <proxy_disc_path> is the path to the special directory we created to serve as Cloudera's proxy disc.
    
    - Go into the newly started container's console and navigate to the "/gft-host" directory, where you should find the 
    aforementioned data
    
    - Run the "init.sh" script to transfer data to the "/users/clouders" directory on HDFS.
    
    - Start Hive console by calling "hive" or navigate to Hive UI in Hue via "localhost:8888" in your browser.
    
1. Create the "EMPLOYEES" managed table in Hive with the following fields:

    - id (BIGINT)
    - name (STRING)
    - surname (STRING)
    - age (INT)
    
   bucketed into 16 buckets using the "age" field and stored as Avro files.
    
   The following command pattern can be used to do this:
   
   <b>CREATE TABLE [name]([field] [type] ...) CLUSTERED BY([bucketing_columns]) INTO [buckets_count] BUCKETS STORED AS AVRO;</b>
   
   Load the data directly from the "/user/cloudera/employees/employees.avro" HDFS file into the "EMPLOYEES" table.
   
   The following command pattern can be used to achieve this: 
   
   <b>LOAD DATA INPATH '[file_path]' INTO TABLE [table_name];</b> 
   
   Write a HQL query, that lists the id, name, surname and age of the 10 youngest employees in the company.
   
   The result should be:
   
   |------------------------|-------|-----------|--|
   |9142663307278240080	    |Olivia	|Brown	    |20|
   |4642405334998336361	    |Olivia	|Smith	    |20|
   |-1903247989832727208	|Liam	|Miller	    |20|
   |-543441861784811070	    |Jacob	|Brown	    |20|
   |4846510212249437988	    |Amelia	|Davis	    |20|
   |-9210373312920532954	|James	|Williams	|20|
   |-7800748830028249231	|Mia	|Miller	    |20|
   |467667153472408797	    |Ava	|Smith	    |20|
   |-6147910367583713608	|Olivia	|Davis	    |20|
   |-4891817733283756829	|Liam	|Smith	    |20|
   
2. On data in the "/user/cloudera/employmentDetails" HDFS directory, declare an external table, with the following fields:

    - employeeId (BIGINT)
    - experience (INT)
    
   and the following partition columns:
   
    - country (STRING)
    - department (STRING)
    
    stored as CSV files with fields delimited by commas.
    
    The following pattern can be used to achieve this:
    
    <b>CREATE EXTERNAL TABLE [table_name]([field] [type] ...) PARTITIONED BY([field] [type] ...) ROW FORMAT DELIMITED FIELDS TERMINATED BY [separator] LOCATION '[path]';</b>
    
    Then, force the engine to re-evaluate partitions already ecnoded in underlying data.
    
    The following patterns can be used to achieve this:
    
    - Hive: <b>MSCK REPAIR TABLE [table_name];</b>
    
    - Impala: <b>invalidate metadata</b>
    
    Write a query that checks the total number of employees in each department of each country within the company.
    
    The results should look like:
   
   
    | H1            | H2                    |H3|
    |---------------|-----------------------|--|
    |Brazil		    |Accounting			    |26|
    |Brazil		    |Data					|25|
    |Brazil		    |HR					    |21|
    |Brazil		    |IT					    |19|
    |Brazil		    |Management			    |28|
    |Brazil		    |Project Development	|26|
    |Brazil		    |Recruitment			|27|
    |France		    |Accounting			    |30|
    |France		    |Data					|24|
    |France		    |HR					    |19|
    |France		    |IT					    |26|
    |France		    |Management			    |20|
    |France		    |Project Development	|10|
    |France		    |Recruitment			|20|
    |Malaysia	    |Accounting			    |25|
    |Malaysia	    |Data					|25|
    |Malaysia	    |HR					    |26|
    |Malaysia	    |IT					    |28|
    |Malaysia	    |Management			    |29|
    |Malaysia	    |Project Development	|27|
    |Malaysia	    |Recruitment			|23|
    |Poland		    |Accounting			    |19|
    |Poland		    |Data					|19|
    |Poland		    |HR					    |21|
    |Poland		    |IT					    |20|
    |Poland		    |Management			    |21|
    |Poland		    |Project Development	|24|
    |Poland		    |Recruitment			|32|
    |USA		    |Accounting			    |28|
    |USA		    |Data					|20|
    |USA		    |HR					    |24|
    |USA		    |IT					    |26|
    |USA		    |Management			    |22|
    |USA		    |Project Development	|20|
    |USA		    |Recruitment			|17|
    |United Kingdom |Accounting			    |22|
    |United Kingdom |Data	                |28|
    |United Kingdom |HR					    |29|
    |United Kingdom |IT					    |26|
    |United Kingdom |Management			    |25|
    |United Kingdom |Project Development	|26|
    |United Kingdom |Recruitment			|27|
                                                
3. Declare an external table named "SALARY_DATA" on Parquet data inside the "/user/cloudera/salaryData" HDFS directory
   with fields and types as stored in the Parquet file (employeeId BIGINT and salary DOUBLE)

   The following query can be used to achieve this:
   
   <b>CREATE EXTERNAL TABLE [table_name]([field] [type] ...) STORED AS PARQUET LOCATION [directory] TBLPROPERTIES('parquet.compression'='snappy');</b>        
   
   Write a query that would list the top 10 salaries overall to check the correctness of your implementation.
   
   The results should be:

      |-------------------|   
      |1861.0529092539268 |
      |1908.5762120457389 |
      |2075.0375852641528 |
      |2086.527015173785  |
      |2178.5400852918156 |
      |2222.300860652382  |
      |2283.795614966987  |
      |2292.1712861069036 |
      |2301.2717861995784 |
      |2443.314820603869  |
   
4. Inside the "prv.saevel.bigdata.sql.udf.IsEuCountryUdf" class, implement a Hive UDF function, which for a given Text 
   field (Hadoop wrapper for a String) will check if the text field contains the name of a country from the EuropeanUnion.
   You can look up the names of those countries in the EuropeanUnion.members field. Run the "IsEuCountryUdfTest" test 
   class to verify the correctness of your implementation.
   
5. Inside the "prv.saevel.bigdata.sql.udaf.UniqueIdUDAF" class, implement a hive UDAF function, which for a given long
   field checks if all the ids that appear on that field are actually unique over the entire dataset and returns a 
   Boolean as an indicator. Run the "UniqueIdUDAFTest" test class to verify the correctness of your implementation.
   
6. Write a HQL script, that would list the:

    * name
    * surname
    * salary
    * experience
    * country
    * department
    
    of all employees, who have both the highest salary and highest number of years of experience in their respective country
    and department at the same time.
    
    The results should look like this:
    
    |-----------|-----------|-------------------|---|---------------|-------------------|
    |Emma	    |Davis	    |6862.106202127102	|53	|France	        |Accounting         |
    |Isabella	|Wilson	    |7572.2007375490275	|54	|Malaysia	    |Accounting         |
    |James	    |Wilson	    |6751.775666458922	|53	|Malaysia	    |HR                 |
    |Sophia	    |Miller	    |9839.362719565705	|55	|Malaysia	    |Management         |
    |Olivia	    |Johnson	|8608.655648285097	|54	|Poland	        |Data               |
    |William    |Garcia	    |6655.366232692259	|51	|Poland	        |HR                 |
    |Jacob	    |Miller	    |9344.318531502442	|53	|Poland	        |Management         |
    |Alexander	|Jones	    |8250.257610419218	|55	|Poland	        |Project Development|
    |Mia	    |Jones	    |9275.645030263893	|55	|USA	        |Management         |
    |Sophia	    |Williams	|6893.57807046831	|55	|United Kingdom	|HR                 |
    |Liam	    |Miller	    |7363.438077498668	|54	|United Kingdom	|IT                 |
    |Jacob	    |Johnson	|8396.740335136059	|55	|United Kingdom	|Project Development|
    
    NOTE: In this assignment, it is practical to create intermediate, temporary tables, for instance
    storing maximal salaries and years of experience for each country and department