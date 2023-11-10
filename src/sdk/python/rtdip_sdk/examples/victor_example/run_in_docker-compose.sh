docker container stop rtdip
docker container rm rtdip
docker system prune -a -f
docker image rm  "rtdip:Dockerfile"
docker build -t "rtdip:Dockerfile" .
docker-compose down
docker-compose rm
docker-compose up
