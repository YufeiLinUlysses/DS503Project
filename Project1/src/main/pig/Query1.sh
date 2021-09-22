#!/bin/sh
pig -param customer=hdfs://localhost:9000/input/Customer.txt -param transaction=hdfs://localhost:9000/input/Transaction.txt -param out=hdfs://localhost:9000/output/ Query1.pig
