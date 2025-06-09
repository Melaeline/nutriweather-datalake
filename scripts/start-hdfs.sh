#!/bin/bash

# Wait for configuration to be available
sleep 10

# Format NameNode if not already formatted
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formatting NameNode..."
    $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
fi

# Start NameNode
echo "Starting NameNode..."
$HADOOP_HOME/bin/hdfs namenode
