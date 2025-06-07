# Elasticsearch Python Client Compatibility Fix
# This addresses urllib3 version conflicts with Elasticsearch 8.15.0

## The Issue
The error `HTTPSConnectionPool._new_conn() got an unexpected keyword argument 'heb_timeout'` 
indicates a compatibility issue between:
- urllib3 >= 2.0.0
- elasticsearch-py client library
- Docker networking

## Solution Applied

### 1. urllib3 Version Pinning
```
urllib3>=1.26.0,<2.0.0
```
This ensures compatibility with the current Elasticsearch client.

### 2. Connection Configuration Updates
- Changed host from `localhost:9200` to `es01:9200` (Docker service name)
- Added connection pooling settings
- Enhanced timeout and retry configuration
- Added fallback authentication methods

### 3. Docker Service Communication
Within Docker Compose, services communicate using service names:
- ✅ `es01:9200` (correct for inter-container communication)
- ❌ `localhost:9200` (only works from host machine)

## Testing
After these changes, run:
```bash
# Restart containers to pick up new requirements
docker-compose down
docker-compose up -d

# Test the connection
python test_elastic_integration.py
```

## Alternative Connection Methods
If issues persist, consider:
1. Using HTTP instead of HTTPS for development
2. Updating to elasticsearch-py 8.16.0+ when available
3. Using connection pooling parameters for better stability
