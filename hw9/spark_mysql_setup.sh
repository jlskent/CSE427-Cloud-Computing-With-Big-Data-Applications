#!/bin/sh

# Set logging level to ERROR
echo "Setting logging level to ERROR on console..."
echo "HADOOP_ROOT_LOGGER=ERROR,console" | sudo tee -a /etc/hadoop/conf.pseudo/hadoop-env.sh

echo "Setting logging for spark..."
echo "spark.eventLog.dir=hdfs:///user/spark/applicationHistory" | sudo tee -a /etc/spark/conf.dist/spark-defaults.conf
echo "spark.eventLog.enabled           true" | sudo tee -a /etc/spark/conf.dist/spark-defaults.conf
echo "spark.serializer                 org.apache.spark.serializer.KryoSerializer" | sudo tee -a /etc/spark/conf.dist/spark-defaults.conf
echo "spark.yarn.historyServer.address=localhost:18088" | sudo tee -a /etc/spark/conf.dist/spark-defaults.conf
echo "spark.executor.memory 400M" | sudo tee -a /etc/spark/conf.dist/spark-defaults.conf

# Set up mySQL
echo "setting up mysql database..."
mysql -u root -p"cloudera" <<MYSQL_SCRIPT
# DROP DATABASE loudacre;
CREATE DATABASE loudacre;
MYSQL_SCRIPT
mysql -uroot -p"cloudera" loudacre < /home/cloudera/training_materials/dev1/data/loudacre.sql
