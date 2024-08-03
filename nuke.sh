#!/bin/bash

if (( $EUID != 0 )); then
    echo "Essa operação dever ser executada como root"
    exit
fi

DOCKER_COMPOSE_FILE=$(cat config_envs.json | jq -r '.DOCKER_COMPOSE_FILE')
AIRFLOW_WORKSPACE=$(cat config_envs.json | jq -r '.AIRFLOW_WORKSPACE')

echo 'parando serviços existentes...'
docker compose -f $DOCKER_COMPOSE_FILE down

echo 'apagando espaço de trabalho do airflow...'
sudo rm -rf $AIRFLOW_WORKSPACE


