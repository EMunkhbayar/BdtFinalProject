# BdtFinalProject
**CS523** 

> Gets **the top-rated jobs** in the Data Science domain from csv dataset into the **Hbase** database using **Kafka** and **Spark Streaming**.

## Steps

### Run HBase:
```
sudo service hbase-master start
sudo service hbase-regionserver start
```

### Run Kafka:
Start Zookeeper
```
./bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
```

### Start Kafka server:
```
./bin/kafka-server-start.sh -daemon config/server.properties
```

### Create topic:
```
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-topic
```

### Produce message to topic:
```
tail -n +2  ~/workspace/BdtFinalProject/input/ds_salaries.csv | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic
```

### Run BdtFinalProject Spark Streaming project:
run from Eclipse
or
```
spark-submit --class cs523.App --master local target/BdtFinalProject-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

### See result in HBase:
```
hbase
scan 'ds_salaries'
```

### If you want to send new data to the topic:
```
sed -i -e '$a4400,2020,MI,FT,Data Engineer,88000,GBP,512872,GB,50,GB,L'  ~/workspace/BdtFinalProject/input/ds_salaries.csv
tail -n +2  ~/workspace/BdtFinalProject/input/ds_salaries.csv | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic
```
