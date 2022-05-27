#!/bin/bash
docker run --name pg \
      -p 5432:5432 \
      -e POSTGRES_DB="demo" \
      -e POSTGRES_USER="test_sde" \
      -e POSTGRES_PASSWORD=="@sde_password012" \
      -v //$(pwd)/db:/var/lib/postgresql/data \
      -d postgres:alpine

sleep 10
docker cp ./sql/init_db/demo.sql pg:/docker-entrypoint-initdb.d/dump.sql
docker exec -u postgres pg psql demo test_sde -f docker-entrypoint-initdb.d/dump.sql