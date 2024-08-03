#!/bin/bash

DOCKER_COMPOSE_FILE=$(cat config_envs.json | jq -r '.DOCKER_COMPOSE_FILE')

echo 'parando serviços existentes...'
docker compose -f $DOCKER_COMPOSE_FILE stop
