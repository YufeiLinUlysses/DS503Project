customer = LOAD '$customer' USING PigStorage(',')
   as (cid:int,name:chararray,age:int,gender:chararray,country_code:int,salary:float);

transaction = LOAD '$transaction' USING PigStorage(',') 
   as (tid:int,cid:int,trans_total:float,trans_num_items:int,transdesc:chararray);


-- A = GROUP transaction BY cid;
-- B = FOREACH A GENERATE group, COUNT(transaction.cid), SUM(transaction.trans_total), MIN(transaction.trans_num_items);
-- B
Customer = FOREACH customer GENERATE cid, name, salary;
Transaction = FOREACH transaction GENERATE cid, trans_total, trans_num_items;
joined = JOIN Transaction BY cid, Customer BY cid USING 'replicated';

joined1 = FOREACH joined GENERATE $0 as id, $4 as name, $5 as salary,  $1 as trans_total, $2 as trans_num_items;
grouped = GROUP joined1 BY (id, name, salary);
C = FOREACH grouped GENERATE group, COUNT(joined1),SUM(joined1.trans_total),MIN(joined1.trans_num_items);
-- C = FOREACH grouped GENERATE group, joined.$1, joined.$2, COUNT(joined.$1), SUM(joined.$4), MIN(joined.$5);
-- C = FOREACH customer GENERATE cid, name, salary;
-- D = JOIN C BY cid, B BY group USING 'replicated';
-- E = FOREACH D GENERATE $0,$1,$2,$4,$5,$6;

STORE C into '$out' USING PigStorage(',');
-- DUMP C;

