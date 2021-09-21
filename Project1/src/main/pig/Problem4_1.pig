-- customer = LOAD 'Customers.txt' USING PigStorage(',') AS (ID, name, age, gender, countrycode, salary);
-- trans = LOAD 'Transaction.txt' USING PigStorage(',') AS (transID, ID, total, numitem, desc);

customer = LOAD 'file:///home/ds503/shared_folder/Project1/data/Customer.txt' USING PigStorage(',') 
   as (ID, name, age, gender, countrycode, salary);

trans = LOAD 'file:///home/ds503/shared_folder/Project1/data/Transaction.txt' USING PigStorage(',') 
   as (transID, ID, total, numitem, descr);

cus1 = FOREACH customer GENERATE ID, name;
trans1 = FOREACH trans GENERATE transID, ID;
transgrp = GROUP trans1 BY ID;
transcnt = FOREACH transgrp GENERATE ID, COUNT(transID) AS cnt;

result = JOIN cus1 BY ID, transcnt BY ID;
result1 = FOREACH result GENERATE name, cnt;
result2 = ORDER result1 BY cnt ASC;

final = LIMIT result2 1;
DUMP final;
-- STORE final into 'Problem4_1' USING PIGStorage(',');