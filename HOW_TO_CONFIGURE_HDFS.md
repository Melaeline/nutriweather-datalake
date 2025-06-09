# HDFS Docker Compose Setup

A complete HDFS (Hadoop Distributed File System) setup using Docker Compose with NameNode, DataNodes, and Python client for testing operations.

## 🚀 Quick Start

```bash
# Navigate to the project directory
cd nutriweather-datalake

# Start the HDFS cluster
docker-compose up -d

# Check logs
docker-compose logs -f

# Stop the cluster
docker-compose down
```

## 📁 Project Structure

```
nutriweather-datalake/
├── docker-compose.override.yml # Main orchestration file
├── hdfs_config/                # Hadoop configuration files
│   ├── core-site.xml          # Core Hadoop settings
│   ├── hdfs-site.xml          # HDFS-specific settings
│   └── log4j.properties       # Logging configuration
├── scripts/                   # Startup scripts
│   ├── start-hdfs.sh         # NameNode startup script
│   └── init-datanode.sh      # DataNode initialization script
├── include/                   # Shared data directory
├── namenode-data/            # NameNode data (auto-created)
├── datanode1-data/           # DataNode 1 data (auto-created)
└── datanode2-data/           # DataNode 2 data (auto-created)
```

## ⚙️ Configuration Files

### Core Configuration (`hdfs_config/core-site.xml`)
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode:8020</value>
  </property>
</configuration>
```

### HDFS Configuration (`hdfs_config/hdfs-site.xml`)
```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///opt/hadoop/data/nameNode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///opt/hadoop/data/dataNode</value>
  </property>
</configuration>
```

### Logging Configuration (`hdfs_config/log4j.properties`)
```properties
log4j.rootLogger=INFO, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{ISO8601} %-5p %c: %m%n
```

## 🐳 Docker Services

### NameNode
- **Container**: `namenode`
- **Ports**: 9870 (Web UI), 8020 (HDFS API)
- **Health Check**: HTTP check on port 9870
- **IP**: 172.30.0.2

### DataNodes
- **datanode1**: 172.30.0.3
- **datanode2**: 172.30.0.4
- Both depend on NameNode health check

### HDFS Client
- **Container**: `hdfs-client`
- Python 3.9 with HDFS client library
- **IP**: 172.30.0.5
- Runs test operations automatically

## 🔧 Integration into Existing Projects

### Option 1: Copy Services to Existing docker-compose.yml

Add these services to your existing `docker-compose.yml`:

```yaml
services:
  # Your existing services...
  
  namenode:
    image: apache/hadoop:3.3.5
    container_name: namenode
    hostname: namenode
    user: root
    environment:
      - HADOOP_HOME=/opt/hadoop
    volumes:
      - ./hdfs/hadoop_namenode:/opt/hadoop/data/nameNode
      - ./hdfs/hadoop_config:/opt/hadoop/etc/hadoop
      - ./hdfs/start-hdfs.sh:/start-hdfs.sh
    ports:
      - "9870:9870"
      - "8020:8020"
    command: ["/bin/bash", "/start-hdfs.sh"]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9870"]
      interval: 30s
      timeout: 10s
      retries: 5
    networks:
      - your_network

  datanode1:
    image: apache/hadoop:3.3.5
    container_name: datanode1
    hostname: datanode1
    user: root
    environment:
      - HADOOP_HOME=/opt/hadoop
    volumes:
      - ./hdfs/hadoop_datanode1:/opt/hadoop/data/dataNode
      - ./hdfs/hadoop_config:/opt/hadoop/etc/hadoop
      - ./hdfs/init-datanode.sh:/init-datanode.sh
    depends_on:
      namenode:
        condition: service_healthy
    command: ["/bin/bash", "/init-datanode.sh"]
    networks:
      - your_network

  datanode2:
    image: apache/hadoop:3.3.5
    container_name: datanode2
    hostname: datanode2
    user: root
    environment:
      - HADOOP_HOME=/opt/hadoop
    volumes:
      - ./hdfs/hadoop_datanode2:/opt/hadoop/data/dataNode
      - ./hdfs/hadoop_config:/opt/hadoop/etc/hadoop
      - ./hdfs/init-datanode.sh:/init-datanode.sh
    depends_on:
      namenode:
        condition: service_healthy
    command: ["/bin/bash", "/init-datanode.sh"]
    networks:
      - your_network

networks:
  your_network:
    # Your network configuration
```

### Option 2: Use as External Docker Compose

Keep HDFS as separate docker-compose and connect via external network:

```bash
# Create external network
docker network create hdfs_network

# In your main project's docker-compose.yml
networks:
  default:
    external: true
    name: hdfs_network
```

### Option 3: Include as Submodule/Directory

```bash
# Copy hdfs_test directory to your project
cp -r hdfs_test ./hdfs

# Modify your docker-compose.yml to include hdfs services
include:
  - path: ./hdfs/docker-compose.yml
```

## 📝 Python Client Usage

### Install Dependencies
```bash
pip install hdfs==2.7.0 requests
```

### Basic HDFS Operations
```python
from hdfs import InsecureClient

# Connect to HDFS
client = InsecureClient('http://namenode:9870', user='root')

# Write file
with client.write('/example.txt', encoding='utf-8') as writer:
    writer.write("Hello HDFS!")

# Read file
with client.read('/example.txt', encoding='utf-8') as reader:
    content = reader.read()

# List files
files = client.list('/')

# Create directory
client.makedirs('/data')

# Get file status
status = client.status('/example.txt')
```

## 🌐 Access Points

- **HDFS Web UI**: http://localhost:9870
- **HDFS API**: hdfs://localhost:8020
- **From containers**: Use hostname `namenode` instead of `localhost`

## 🔍 Monitoring & Troubleshooting

### Check Service Health
```bash
# Check all services
docker-compose ps

# Check specific service logs
docker-compose logs namenode
docker-compose logs datanode1

# Check HDFS status via web UI
curl http://localhost:9870/dfshealth.html
```

### Common Issues

1. **NameNode formatting**: If NameNode fails to start, remove `hadoop_namenode` directory and restart
2. **DataNode connection**: Ensure DataNodes can resolve `namenode` hostname
3. **Permission issues**: All containers run as root to avoid permission problems
4. **Port conflicts**: Change exposed ports if 9870 or 8020 are in use

### Useful Commands
```bash
# Access NameNode container
docker exec -it namenode bash

# Check HDFS filesystem
docker exec namenode hdfs dfsadmin -report

# Safe mode operations
docker exec namenode hdfs dfsadmin -safemode leave

# Format NameNode (destructive)
docker exec namenode hdfs namenode -format
```

## 🧪 Testing

The included Python client (`client.py`) performs comprehensive testing:
- Connection verification
- File write/read operations
- Directory operations
- File listing and status checks

Run tests manually:
```bash
docker-compose exec hdfs-client python client.py
```

## 📊 Performance Tuning

### Increase Replication Factor
Edit `hadoop_config/hdfs-site.xml`:
```xml
<property>
  <name>dfs.replication</name>
  <value>3</value>  <!-- Increase from 2 -->
</property>
```

### Memory Settings
Add to NameNode environment in docker-compose.yml:
```yaml
environment:
  - HADOOP_HOME=/opt/hadoop
  - HADOOP_HEAPSIZE=2048
  - HADOOP_NAMENODE_OPTS="-Xmx2g"
```

## 🔐 Security Notes

- This setup uses `InsecureClient` for simplicity
- All containers run as `root` user
- No authentication or encryption enabled
- Suitable for development/testing only

## 📚 Additional Resources

- [Hadoop Documentation](https://hadoop.apache.org/docs/stable/)
- [HDFS Architecture](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Python HDFS Client](https://hdfscli.readthedocs.io/)
