#!/usr/bin/env bash


if [ -z "$1" ]
then
      FLINK_DOCKER_IMAGE_NAME="flink_job:latest"
fi
export FLINK_DOCKER_IMAGE_NAME=$1

echo ${FLINK_DOCKER_IMAGE_NAME}

mvn clean package

rm -fr ./output

docker build -t ${FLINK_DOCKER_IMAGE_NAME} -f ./Docker/Dockerfile .

docker-compose -f ./Docker/docker-compose.yml up -d
