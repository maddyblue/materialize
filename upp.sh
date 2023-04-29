#!/bin/sh

docker-compose down -v --remove-orphans --timeout 1
docker-compose up -d
