#!/bin/bash

# Wait for NameNode to be ready
echo "Waiting for NameNode to be ready..."
sleep 20

# Start DataNode
echo "Starting DataNode..."
$HADOOP_HOME/bin/hdfs datanode
