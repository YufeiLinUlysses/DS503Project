customer = LOAD 'file:///home/ds503/shared_folder/Project1/data/Customer.txt' USING PigStorage(',') 
   as (cid:int,name:chararray,age:int,gender:chararray,country_code:int,salary:float);

A = GROUP customer BY country_code;
B = FOREACH A GENERATE group, COUNT(customer.cid) AS c1;
C = FILTER B BY c1 >= 5000 or c1 <= 2000;
DUMP C;
