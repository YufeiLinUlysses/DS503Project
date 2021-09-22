customer = LOAD 'file:///home/ds503/shared_folder/Project1/data/Customer.txt' USING PigStorage(',')
   as (cid:int,name:chararray,age:int,gender:chararray,country_code:int,salary:float);

transaction = LOAD 'file:///home/ds503/shared_folder/Project1/data/Transaction.txt' USING PigStorage(',') 
   as (tid:int,cid:int,trans_total:float,trans_num_items:int,transdesc:chararray);

A = GROUP transaction by cid;
B = FOREACH A GENERATE group, COUNT(transaction.cid), SUM(transaction.trans_total), MIN(transaction.trans_num_items);
C = FOREACH customer GENERATE cid, name, salary;
D = JOIN C BY cid, B by group;
DUMP D;

