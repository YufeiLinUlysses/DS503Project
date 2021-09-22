-- file:///home/ds503/shared_folder/Project1/data/Customer.txt
customer = LOAD '$customer' USING PigStorage(',') 
   as (cid:int,name:chararray,age:int,gender:chararray,country_code:int,salary:float);
-- file:///home/ds503/shared_folder/Project1/data/Transaction.txt
transaction = LOAD '$transaction' USING PigStorage(',') 
   as (tid:int,cid:int,trans_total:float,trans_num_items:int,transdesc:chararray);

groupedTrans = GROUP transaction BY cid;
cntTrans = FOREACH groupedTrans GENERATE group, COUNT(transaction.cid) as cnt; 
combined = JOIN customer BY cid, cntTrans BY group;
tmp = FOREACH combined GENERATE name,cnt;
ordered = ORDER tmp BY cnt ASC;
min = LIMIT ordered 1;

STORE min INTO '$out' USING PigStorage(',');