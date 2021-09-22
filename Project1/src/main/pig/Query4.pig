customer = LOAD '$customer' USING PigStorage(',') 
   as (cid:int,name:chararray,age:int,gender:chararray,country_code:int,salary:float);

transaction = LOAD '$transaction' USING PigStorage(',') 
   as (tid:int,cid:int,trans_total:float,trans_num_items:int,transdesc:chararray);

Transaction = FOREACH transaction GENERATE cid, trans_total, trans_num_items;
cased = FOREACH customer GENERATE cid, gender, (
                           CASE
                              WHEN age < 20 THEN '[10,20)' 
                              WHEN age >=20 and age <30 THEN '[20,30)' 
                              WHEN age >=30 and age <40 THEN '[30,40)' 
                              WHEN age >=40 and age <50 THEN '[40,50)' 
                              WHEN age >=50 and age <60 THEN '[50,60)' 
                              ELSE '[60,70]' 
                           END) as ageRange;
joined = JOIN Transaction BY cid, cased by cid;
joined1 = FOREACH joined GENERATE $0 as id, $4 as gender, $5 as ageRange, $1 as trans_total, $2 as trans_num_items;
grouped = GROUP joined1 BY (gender, ageRange);
C = FOREACH grouped GENERATE group, MIN(joined1.trans_total), MAX(joined1.trans_total), AVG(joined1.trans_total);
STORE C INTO '$out' USING PigStorage(',');