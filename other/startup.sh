#!/bin/bash
# Ubuntu 

apt-get update

apt-get install docker

apt install docker.io -y

apt install docker-compose -y

dockerd

# needs restart to take effect, use sudo docker .... if you don't want to restart your instance
usermod -a -G docker ubuntu

service docker start

# Run docker container in detached mode (-d) on port internal 8888 and forward to port 8888 (-p 8888:8888) 
docker run -d -p 8888:8888 rutgerhofste/aqueduct30:v11