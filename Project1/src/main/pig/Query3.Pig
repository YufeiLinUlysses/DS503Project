
customer = LOAD '$customer' USING PigStorage(',') 
   as (cid:int,name:chararray,age:int,gender:chararray,country_code:int,salary:float);
A = GROUP customer BY country_code;
B = FOREACH A GENERATE group, COUNT(customer.cid) AS c1;
C = FILTER B BY c1 >= 5000 or c1 <= 2000;
D = FOREACH C GENERATE group;
STORE D INTO '$out' USING PigStorage(',');
