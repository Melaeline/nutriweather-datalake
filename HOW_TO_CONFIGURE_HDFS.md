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
│   ├── start-hdfs.sh         # NameNode startup script (creates full directory structure)
│   └── init-datanode.sh      # DataNode initialization script
├── include/                   # Shared data directory (all files auto-backup to HDFS)
│   ├── raw/                  # Raw data (auto-backup to /nutriweather/raw/)
│   ├── formatted/            # Formatted data (auto-backup to /nutriweather/formatted/)
│   ├── usage/                # Usage data (auto-backup to /nutriweather/usage/)
│   └── scripts/              # Processing scripts with HDFS integration
├── namenode-data/            # NameNode data (auto-created)
├── datanode1-data/           # DataNode 1 data (auto-created)
└── datanode2-data/           # DataNode 2 data (auto-created)

HDFS Structure (automatically populated):
/nutriweather/
├── raw/
│   ├── meals/                # All raw meal JSON files
│   └── weather/              # All raw weather JSON files
├── formatted/
│   ├── meals/                # All formatted meal Parquet files
│   └── weather/              # All formatted weather JSON files
├── usage/                    # All usage layer JSONL files
├── indexed/                  # Elasticsearch indexing metadata
├── analytics/                # Future analytics outputs
└── backup/                   # General backup location
```

**Automatic Backup Features:**
- **Universal Coverage**: Every file created by the pipeline is automatically backed up to HDFS
- **Intelligent Routing**: Local paths automatically mapped to appropriate HDFS directories
- **Zero Configuration**: Backup happens transparently without manual intervention
- **Fault Tolerance**: Multiple HDFS client fallback methods ensure backup reliability
- **Graceful Degradation**: Pipeline continues even if HDFS is unavailable

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
# For Python 3.12+ compatibility, use stable hdfs library
pip install hdfs>=2.7.3

# Backup: Pure requests-based WebHDFS (always available)
pip install requests>=2.31.0

# Optional: snappy compression (if needed)
# pip install snappy>=1.1.0
```

### Basic HDFS Operations via WebHDFS
```python
# Option 1: Using hdfs library (recommended, most stable)
from hdfs import InsecureClient

# Connect to HDFS via WebHDFS (recommended)
client = InsecureClient('http://namenode:9870', user='root')

# Test connection
try:
    files = client.list('/')
    print("✓ HDFS connection successful")
    print(f"Root directory contents: {files}")
except Exception as e:
    print(f"✗ HDFS connection failed: {e}")

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

# Check if file/directory exists
exists = client.status('/example.txt', strict=False) is not None
```

### Troubleshooting HDFS Client Issues

1. **hdfs3 build failures**: hdfs3 library often fails to build on newer Python versions
   ```bash
   # Solution: Use the stable hdfs library instead
   pip uninstall hdfs3
   pip install hdfs>=2.7.3
   
   # Fallback: Use pure WebHDFS with requests (no extra dependencies)
   # This is automatically handled by spark_utils.py
   ```

2. **"No module named 'imp'" error**: This occurs with older hdfs versions on Python 3.12+
   ```bash
   # Solution: Use newer hdfs version
   pip install hdfs>=2.7.3
   
   # Or use pure WebHDFS fallback (handled automatically)
   ```

3. **Build dependencies missing**: Some HDFS clients require system dependencies
   ```bash
   # For containers, this is handled in the Dockerfile
   # For local development, install build tools:
   sudo apt-get install build-essential python3-dev
   # or on macOS:
   xcode-select --install
   ```

4. **Connection refused**: Ensure NameNode is running and accessible
   ```bash
   # Check NameNode status
   curl http://localhost:9870/dfshealth.html
   
   # Check WebHDFS API
   curl "http://localhost:9870/webhdfs/v1/?op=LISTSTATUS"
   ```

5. **Python 3.12+ compatibility**: The updated spark_utils.py handles multiple client fallbacks
   ```python
   # spark_utils.py automatically tries:
   # 1. hdfs library (InsecureClient) - most stable
   # 2. Pure WebHDFS with requests - always works
   # 3. Graceful degradation - skips HDFS if unavailable
   ```

6. **Library installation order**: Install in this order for best compatibility
   ```bash
   # 1. Install stable hdfs client
   pip install hdfs>=2.7.3
   
   # 2. Ensure requests is available (usually pre-installed)
   pip install requests>=2.31.0
   
   # 3. Optional compression support
   # pip install snappy>=1.1.0  # Only if needed
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
