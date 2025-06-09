#!/bin/bash

# Enable error handling
set -e

echo "Starting DataNode initialization..."

# Set JAVA_HOME with better detection
if [ -z "$JAVA_HOME" ]; then
    # Try to find Java 17 installation
    if [ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]; then
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    elif [ -d "/usr/lib/jvm/java-17-openjdk" ]; then
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
    elif [ -d "/opt/java/openjdk" ]; then
        export JAVA_HOME=/opt/java/openjdk
    else
        echo "ERROR: Could not find Java installation"
        echo "Available JVM directories:"
        ls -la /usr/lib/jvm/ || echo "No /usr/lib/jvm directory found"
        exit 1
    fi
fi

echo "Using JAVA_HOME: $JAVA_HOME"

# Verify Java is working
if ! $JAVA_HOME/bin/java -version 2>&1; then
    echo "ERROR: Java not working at $JAVA_HOME"
    exit 1
fi

# Wait for NameNode to be ready with more robust checking
echo "Waiting for NameNode to be ready..."
for i in {1..120}; do
    if curl -f http://namenode:9870/dfshealth.html > /dev/null 2>&1; then
        echo "NameNode is accessible, starting DataNode..."
        break
    fi
    if [ $i -eq 120 ]; then
        echo "ERROR: NameNode not accessible after 120 seconds"
        exit 1
    fi
    echo "Waiting for NameNode... ($i/120)"
    sleep 1
done

# Ensure proper permissions for DataNode data directory
mkdir -p /opt/hadoop/data/dataNode
chown -R root:root /opt/hadoop/data/dataNode
chmod 755 /opt/hadoop/etc/hadoop/*

# Start DataNode
echo "Starting DataNode..."
exec $HADOOP_HOME/bin/hdfs datanode
