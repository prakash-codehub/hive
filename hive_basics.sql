CREATE DATABASE hive_class_b1;

USE hive_class_b1;

CREATE TABLE department_data                                                                                                            
(                                                                                                                                       
    dept_id          INT                                                                                                                            
  , dept_name        STRING                                                                                                                       
  , manager_id       INT                                                                                                                         
  , salary           INT
)                                                                                                                             
 ROW FORMAT DELIMITED                                                                                                                    
 FIELDS TERMINATED BY ','
; 

DESCRIBE department_data;

DESCRIBE FORMATTED department_data;

# For data load from local
LOAD DATA LOCAL INPATH '/home/cloudera/prakash_lfs/depart_data.csv' INTO TABLE department_data; 

SELECT * FROM hive_class_b1.department_data;

SET hive.cli.print.header=TRUE;


hdfs dfs -mkdir /prakash_hdfs
# Copy from Local to hdfs
hdfs dfs -put /home/cloudera/prakash_lfs/depart_data.csv /prakash_hdfs

CREATE TABLE department_data_from_hdfs                                                                                                            
(                                                                                                                                       
    dept_id          INT                                                                                                                            
  , dept_name        STRING                                                                                                                       
  , manager_id       INT                                                                                                                         
  , salary           INT
)                                                                                                                             
 ROW FORMAT DELIMITED                                                                                                                    
 FIELDS TERMINATED BY ','
; 

# Load data from hdfs location 
LOAD DATA INPATH '/prakash_hdfs/depart_data.csv' into table department_data_from_hdfs;

--After loading from hdfs to hdfs. Source hdfs file gets moved to target hdfs path.
--Internal tables moved data from source hdfs path to target warehouse dir. Can't use the same soruce path again for any other tables.

[cloudera@quickstart ~]$ hdfs dfs -ls /prakash_hdfs   
Found 3 items
-rw-r--r--   1 cloudera supergroup          0 2022-09-03 01:43 /prakash_hdfs/file1.txt
-rw-r--r--   1 cloudera supergroup         28 2022-09-03 01:47 /prakash_hdfs/test.txt
-rw-r--r--   1 root     supergroup          0 2022-09-03 01:35 /prakash_hdfs/text1.txt

hdfs dfs -mkdir /prakash_hdfs1
hdfs dfs -put /home/cloudera/prakash_lfs/depart_data.csv /prakash_hdfs1
[cloudera@quickstart ~]$ hdfs dfs -ls /prakash_hdfs1
Found 1 items
-rw-r--r--   1 cloudera supergroup        681 2022-09-17 07:49 /prakash_hdfs1/depart_data.csv

# Create external table 
CREATE EXTERNAL TABLE department_data_external                                                                                                           
(                                                                                                                                       
    dept_id          INT                                                                                                                            
  , dept_name        STRING                                                                                                                       
  , manager_id       INT                                                                                                                         
  , salary           INT
)                                                                                                                             
ROW FORMAT DELIMITED                                                                                                                    
FIELDS TERMINATED BY ','                                                                                                               
LOCATION '/prakash_hdfs1/'
;

[cloudera@quickstart ~]$ hdfs dfs -ls /user/hive/warehouse/hive_class_b1.db/department_data_from_hdfs;
Found 1 items
-rwxrwxrwx   1 cloudera supergroup        681 2022-09-17 01:05 /user/hive/warehouse/hive_class_b1.db/department_data_from_hdfs/depart_data.csv
 
#Drop Both internal & External tables
DROP TABLE department_data_external;
DROP TABLE department_data_from_hdfs;

#After Drop
[cloudera@quickstart ~]$ hdfs dfs -ls /prakash_hdfs1
Found 1 items
-rw-r--r--   1 cloudera supergroup        681 2022-09-17 07:49 /prakash_hdfs1/depart_data.csv
[cloudera@quickstart ~]$ hdfs dfs -ls /user/hive/warehouse/hive_class_b1.db/department_data_from_hdfs;
ls: `/user/hive/warehouse/hive_class_b1.db/department_data_from_hdfs`: No such file or directory

#Work with Array data types
CREATE TABLE employee_array                                                                                                           
(                                                                                                                                       
    id          INT                                                                                                                            
  , name        STRING                                                                                                                       
  , skills      ARRAY<STRING>                                                                                                                         
)                                                                                                                             
ROW FORMAT DELIMITED                                                                                                                    
FIELDS TERMINATED BY ','                                                                                                               
COLLECTION ITEMS TERMINATED BY ':'
;

LOAD DATA LOCAL INPATH '/home/cloudera/prakash_lfs/array_data.csv' INTO TABLE employee_array;

hive> select * from hive_class_b1.employee_array;
OK
employee_array.id       employee_array.name     employee_array.skills
101     Amit    ["HADOOP","HIVE","SPARK","BIG-DATA"]
102     Sumit   ["HIVE","OZZIE","HADOOP","SPARK","STORM"]
103     Rohit   ["KAFKA","CASSANDRA","HBASE"]
Time taken: 0.055 seconds, Fetched: 3 row(s)


# Get element by index in hive array data type

SELECT 
   id
 , name
 , skills[0] AS prime_skill 
FROM hive_class_b1.employee_array;

id      name    prime_skill
101     Amit    HADOOP
102     Sumit   HIVE
103     Rohit   KAFKA

SELECT                                                                                                                                  
   id
 , name
 , SIZE(skills)                    AS size_of_each_array                                                                                                     
 , ARRAY_CONTAINS(skills,"HADOOP") AS knows_hadoop                                                                                        
 , SORT_ARRAY(skills)              AS sorted_array                                                                                                                     
FROM hive_class_b1.employee_array; 

id      name    size_of_each_array      knows_hadoop    sorted_array
101     Amit    4       true    ["BIG-DATA","HADOOP","HIVE","SPARK"]
102     Sumit   5       true    ["HADOOP","HIVE","OZZIE","SPARK","STORM"]
103     Rohit   3       false   ["CASSANDRA","HBASE","KAFKA"]

# table for map data

CREATE TABLE hive_class_b1.employee_map_data                                                                                                          
(                                                                                                                                       
    id          INT                                                                                                                            
  , name        STRING                                                                                                                       
  , details     MAP<STRING,STRING>                                                                                                              
)
ROW FORMAT DELIMITED                                                                                                                    
FIELDS TERMINATED BY ','                                                                                                               
COLLECTION ITEMS TERMINATED BY '|'
MAP KEYS TERMINATED BY ':';

LOAD DATA LOCAL INPATH '/home/cloudera/prakash_lfs/map_data.csv' INTO TABLE employee_map_data;

select * from hive_class_b1.employee_map_data;
OK
employee_map_data.id    employee_map_data.name  employee_map_data.details
101     Amit    {"age":"21","gender":"M"}
102     Sumit   {"age":"24","gender":"M"}
103     Mansi   {"age":"23","gender":"F"}
 
SELECT                                                                                                                                  
   id
 , name
 , DETAILS["gender"]    AS employee_gender                                                                                                    
FROM employee_map_data; 

id      name    employee_gender
101     Amit    M
102     Sumit   M
103     Mansi   F
 
 # map functions
SELECT                                                                                                                                  
   id
 , name
 , details                                                                                                                                
 , SIZE(details)       AS size_of_each_map                                                                                                      
 , MAP_KEYS(details)   AS distinct_map_keys                                                                                                 
 , MAP_VALUES(details) AS distinct_map_values                                                                                              
FROM employee_map_data; 
id      name    details size_of_each_map        distinct_map_keys       distinct_map_values
101     Amit    {"age":"21","gender":"M"}       2       ["age","gender"]        ["21","M"]
102     Sumit   {"age":"24","gender":"M"}       2       ["age","gender"]        ["24","M"]
103     Mansi   {"age":"23","gender":"F"}       2       ["age","gender"]        ["23","F"]

# Assignment Dataset
https://www.kaggle.com/datasets/imdevskp/corona-virus-report