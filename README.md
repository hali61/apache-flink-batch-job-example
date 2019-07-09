# APACHE FLINK BATCH JOB
Apacehe Flink batch job on docker example

# How to build and run example
```javascript
./run.sh <FLINK_DOCKER_IMAGE_NAME>
```

## Build Docker Image
```javascript
docker build -t ${FLINK_DOCKER_IMAGE_NAME} ./Docker/Dockerfile .
```

## Run Docker Image With docker-compose
```javascript
FLINK_DOCKER_IMAGE_NAME=<FLINK_DOCKER_IMAGE_NAME> docker-compose -f ./Docker/docker-compose.yml up -d
```

#Monitoring Runnig Jobs

We can check the status of the tasks via Apache Flink web ui
 
Access to web ui => localhost:8081
