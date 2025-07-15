#!/bin/bash

DEPENDENCIAS="jq";

function cria_diretorio() {
    DIR_NAME=$1
    if [ -d $DIR_NAME ]; then
        echo "$DIR_NAME já existe";
    else
        echo "criando diretório $DIR_NAME"
        mkdir $DIR_NAME && chown 1000:0 -R $DIR_NAME
    fi;
};

if ! apt install -y $DEPENDENCIAS; then
    echo "Não foi possível satisfazer dependências";
else
    AIRFLOW_WORKSPACE=$(cat config_envs.json | jq -r '.AIRFLOW_WORKSPACE');
    SUBDIRS=$(cat config_envs.json | jq -r '.SUBDIRS | @sh' | tr -d \');
    DOCKER_IMAGE_OWNER=$(cat config_envs.json | jq -r '.DOCKER_IMAGE_OWNER');
    DOCKER_IMAGE_NAME=$(cat config_envs.json | jq -r '.DOCKER_IMAGE_NAME');
    DOCKER_IMAGE_VERSION=$(cat config_envs.json | jq -r '.DOCKER_IMAGE_VERSION');
    DOCKER_FILE=$(cat config_envs.json | jq -r '.DOCKER_FILE');
    DOCKER_COMPOSE_FILE=$(cat config_envs.json | jq -r '.DOCKER_COMPOSE_FILE');

    echo 'criando espaço de trabalho do airflow...';

    cria_diretorio $AIRFLOW_WORKSPACE;

    for dir in $SUBDIRS;
    do
        cria_diretorio "$AIRFLOW_WORKSPACE/$dir";
    done;

    echo 'compilando airflow...';

    docker image build -t $DOCKER_IMAGE_OWNER/$DOCKER_IMAGE_NAME:$DOCKER_IMAGE_VERSION -f $DOCKER_FILE .;

    echo 'parando serviços existentes...';

    docker compose -f $DOCKER_COMPOSE_FILE down;

    echo 'iniciando...';

    docker compose -f $DOCKER_COMPOSE_FILE up -d;

    echo 'configurando conexões...';

    CONTAINER_ID=$(docker ps -q --filter 'name=airflow-webserver');

    echo "configurando coneções com o container $CONTAINER_ID...";

    docker container exec -t $CONTAINER_ID airflow connections import --overwrite /opt/airflow/dags/src/resources/postgres_connections.json;
fi;