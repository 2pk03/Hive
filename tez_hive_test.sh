clear
echo -------------------------
echo Cleaning up previous runs
echo -------------------------
hive -e "drop table transactions" > /dev/null 2>&1
hadoop fs -rm -r -skipTrash transactions > /dev/null 2>&1
echo

echo "(Press enter to create a random dataset using MR)"
read
echo -----------------------
echo Generate Random Dataset
echo -----------------------
echo
echo hadoop jar impala-demo-0.1-SNAPSHOT.jar com.cloudera.tools.rmat.RMat -Drmat.nodes=200000 -Drmat.edges=600000 -Drmat.mappers=8 transactions
echo
hadoop jar impala-demo-0.1-SNAPSHOT.jar com.cloudera.tools.rmat.RMat -Drmat.nodes=200000 -Drmat.edges=600000 -Drmat.mappers=8 transactions
echo

echo "(Press enter to create table in Hive)"
read
echo ------------------------
echo Creating tables with Hive
echo ------------------------
echo
echo hive -e "CREATE EXTERNAL TABLE transactions (sender STRING, recipient STRING, time INT, amount INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/$USER/transactions'"
echo
hive -e "CREATE EXTERNAL TABLE transactions (sender STRING, recipient STRING, time INT, amount INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/$USER/transactions'"
echo

echo "(Press enter to display a sample from the dataset)"
read
echo ------------------------------------
echo Displaying a sample from the dataset
echo ------------------------------------
echo 
echo "select * from transactions limit 10"
echo
hive -e "select * from transactions limit 10"
echo

echo "(Press enter to count records with Hive)"
read
echo --------------------------
echo Counting records with Hive
echo --------------------------
echo 
echo "select count(*) from transactions"
echo
hive -e "select count (*) from transactions"
echo
echo -------------------
echo "the same with Tez"
echo ------------------- 
echo 
hive -e "set hive.execution.engine=tez; select count (*) from transactions"

echo "(Press enter to count records for account-0 with Hive)"
read
echo ---------------------------------------------
echo Counting transactions for account-0 with Hive
echo ---------------------------------------------
echo 
echo "select count(*) from (select * from transactions where sender = 'acc-0' or recipient = 'acc-0') i"
echo
hive -e "select count(*) from (select * from transactions where sender = 'acc-0' or recipient = 'acc-0') i"
echo

echo "(Press enter to count records for account-0 with Tez)"
read
echo -----------------------------------------------
echo Counting transactions for account-0 with Tez
echo -----------------------------------------------
echo 
echo "select count(*) from (select * from transactions where sender = 'acc-0' or recipient = 'acc-0') i"
echo
hive -e "set hive.execution.engine=tez;select count(*) from (select * from transactions where sender = 'acc-0' or recipient = 'acc-0') i"
echo

echo "(Press enter to summarise transactions with Hive)"
read
echo ----------------------------------
echo Summarising transactions with Hive
echo ----------------------------------
echo 
echo "select sender, sum(amount) as amount from transactions group by sender limit 10"
echo
hive -e "select sender, sum(amount) as amount from transactions group by sender limit 10"
echo

echo "(Press enter to summarise transactions with Tez)"
read
echo ---------------------------------
echo Summarising transactions with Tez 
echo ---------------------------------
echo 
echo "select sender, sum(amount) as amount from transactions group by sender limit 10"
echo
hive -e "set hive.execution.engine=tez;select sender, sum(amount) as amount from transactions group by sender limit 10"
echo

echo "(Press enter to summarise transactions with Tez)"
read
echo ----------------------------------------------------
echo "Summarising transactions with Tez (with order by)"
echo ----------------------------------------------------
echo 
echo "select sender, sum(amount) as amount from transactions group by sender order by amount desc limit 10"
echo
hive -e "set hive.execution.engine=tez;select sender, sum(amount) as amount from transactions group by sender order by amount desc limit 10"
echo

echo "(Press enter to see which accounts have gained the most with Hive)"
read
echo ---------------------------------
echo Finding highest gainers with Hive
echo ---------------------------------
echo 
echo "select id, sum(amount) as amount from (select sender as id, amount * -1 as amount from transactions union all select recipient as id, amount from transactions) unionResult group by id order by amount desc limit 10"
echo
hive -e "select id, sum(amount) as amount from (select sender as id, amount * -1 as amount from transactions union all select recipient as id, amount from transactions) unionResult group by id order by amount desc limit 10"
echo

echo "(Press enter to see which accounts have gained the most with Tez)"
read
echo -----------------------------------
echo Finding highest gainers with Tez 
echo -----------------------------------
echo 
echo "select id, sum(amount) as amount from (select sender as id, amount * -1 as amount from transactions union all select recipient as id, amount from transactions) unionResult group by id order by amount desc limit 10"
echo
hive -e "set hive.execution.engine=tez;select id, sum(amount) as amount from (select sender as id, amount * -1 as amount from transactions union all select recipient as id, amount from transactions) unionResult group by id order by amount desc limit 10"
echo

echo "(Press enter to find fraudsters with Tez)"
read
echo ---------------------------
echo Finding fraudsters with Tez 
echo ---------------------------
echo 
echo "select count(*) from (select a.sender, a.recipient, b.recipient as c from transactions a join transactions b on a.recipient = b.sender where a.time < b.time and b.time - a.time < 5) i"
echo
hive -e "set hive.execution.engine=tez;select count(*) from (select a.sender, a.recipient, b.recipient as c from transactions a join transactions b on a.recipient = b.sender where a.time < b.time and b.time - a.time < 5) i"
echo

echo "(Press enter to find fraudsters with Hive)"
read
echo ----------------------------
echo Finding fraudsters with Hive
echo ----------------------------
echo 
echo "select count(*) from (select a.sender, a.recipient, b.recipient as c from transactions a join transactions b on a.recipient = b.sender where a.time < b.time and b.time - a.time < 5) i"
echo
hive -e "select count(*) from (select a.sender, a.recipient, b.recipient as c from transactions a join transactions b on a.recipient = b.sender where a.time < b.time and b.time - a.time < 5) i"
echo

