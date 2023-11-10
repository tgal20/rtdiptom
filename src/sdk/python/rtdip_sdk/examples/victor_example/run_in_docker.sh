#!/usr/bin/env bash
docker container stop rtdip
docker container rm rtdip
#docker container stp rtdip_postgres
#docker container rm rtdip_postgres
docker system prune -a -f
docker image rm  "rtdip:Dockerfile"
docker build -t "rtdip:Dockerfile" .
docker run --name rtdip --publish 8080:8080 --publish 3000:3000 "rtdip:Dockerfile"
#docker image rm "rtdip_postgres:Dockerfile"
#docker build -t "rtdip_postgres:Dockerfile" ./postgres
#docker run -d --name rtdip_postgres_container -p 5432:5432 rtdip_postgres
