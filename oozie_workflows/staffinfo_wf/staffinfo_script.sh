spark-submit --master yarn-client --class com.crg.initialLoad.staff_fac_BioLoad --num-executors 4 --driver-memory 1g --executor-memory 3g   historyLoadCassandra-0.0.1-SNAPSHOT-jar-with-dependencies.jar  -cassandra_conn_host prod-dlake01.creighton.edu -destination_keyspace prod_datalake