#!/bin/bash

# Enable error handling
set -e

echo "Starting HDFS NameNode initialization..."

# Wait for configuration to be available
echo "Waiting for configuration files..."
sleep 5

# Ensure proper permissions
chmod 755 /opt/hadoop/etc/hadoop/*
chown -R root:root /opt/hadoop/data/nameNode

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

# Format NameNode if not already formatted
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formatting NameNode..."
    mkdir -p /opt/hadoop/data/nameNode
    $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
    echo "NameNode formatting completed"
else
    echo "NameNode already formatted, skipping format step"
fi

# Start NameNode in background
echo "Starting NameNode..."
$HADOOP_HOME/bin/hdfs namenode &
NAMENODE_PID=$!

# Wait for NameNode to start accepting connections
echo "Waiting for NameNode to become ready..."
for i in {1..60}; do
    if curl -f http://localhost:9870/dfshealth.html > /dev/null 2>&1; then
        echo "NameNode is ready and accepting connections"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "ERROR: NameNode failed to start within 60 seconds"
        exit 1
    fi
    echo "Waiting for NameNode... ($i/60)"
    sleep 1
done

# Create nutriweather directory structure in HDFS
echo "Creating HDFS directory structure for NutriWeather..."
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /nutriweather/raw/meals
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /nutriweather/raw/weather
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /nutriweather/formatted/meals
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /nutriweather/formatted/weather
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /nutriweather/usage/timeseries
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /nutriweather/usage/recommendations
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /nutriweather/indexed
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /nutriweather/analytics
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /nutriweather/backup

echo "HDFS directory structure created successfully"

# Test HDFS accessibility using curl (since python3 is not available)
echo "Testing HDFS Web UI accessibility..."
if curl -f http://localhost:9870/dfshealth.html > /dev/null 2>&1; then
    echo "✓ HDFS Web UI accessible"
else
    echo "⚠ HDFS Web UI test failed"
fi

echo "NameNode started successfully and ready for connections"

# Keep the container running by waiting for the NameNode process
wait $NAMENODE_PID
