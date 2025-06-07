@echo off
REM Nutriweather Datalake - Elasticsearch Stack Management Script
REM Usage: elastic-stack.bat [start|stop|restart|status|logs|setup]

if "%1"=="" goto usage
if "%1"=="start" goto start
if "%1"=="stop" goto stop 
if "%1"=="restart" goto restart
if "%1"=="status" goto status
if "%1"=="logs" goto logs
if "%1"=="setup" goto setup
goto usage

:start
echo Starting Nutriweather Datalake with Elasticsearch Stack...
docker-compose up -d
echo.
echo Stack started! Services available at:
echo    Kibana Dashboard: http://localhost:5601
echo    Elasticsearch API: https://localhost:9200
echo    Airflow UI: http://localhost:8080
echo    Spark Master: http://localhost:8081
echo.
echo Default credentials: elastic/elastic
echo.
echo Please wait 2-3 minutes for all services to initialize...
goto end

:stop
echo Stopping Nutriweather Datalake...
docker-compose down
echo All services stopped
goto end

:restart
echo Restarting Nutriweather Datalake...
docker-compose down
timeout /t 5 /nobreak > nul
docker-compose up -d
echo Stack restarted
goto end

:status
echo Service Status:
docker-compose ps
echo.
echo Health Checks:
echo Elasticsearch:
curl -k -u elastic:elastic -s https://localhost:9200/_cluster/health?pretty 2>nul || echo "   ❌ Not responding"
echo.
echo Kibana:
curl -s http://localhost:5601/api/status 2>nul | findstr "overall" || echo "   ❌ Not responding" 
goto end

:logs
if "%2"=="" (
    echo Showing logs for all services...
    docker-compose logs -f --tail=50
) else (
    echo Showing logs for %2...
    docker-compose logs -f --tail=50 %2
)
goto end

:setup
echo 🔧 Running Elasticsearch Stack Setup...
python setup_elastic_stack.py
echo.
echo Setup completed! Check output above for any issues.
goto end

:usage
echo.
echo Nutriweather Datalake - Elasticsearch Stack Manager
echo.
echo Usage: %0 [command]
echo.
echo Commands:
echo   start     - Start all services (Elasticsearch, Kibana, Airflow, Spark)
echo   stop      - Stop all services
echo   restart   - Restart all services  
echo   status    - Show service status and health
echo   logs      - Show logs (optional: specify service name)
echo   setup     - Run initial Elasticsearch setup and indexing
echo.
echo Examples:
echo   %0 start
echo   %0 logs kibana
echo   %0 setup
echo.

:end
