hello world

build docker:
`docker build -t friendlyhello testDocker`

`RutgerMacbook:Aqueduct30Docker rutgerhofste$ docker run -d -v /Users/rutgerhofste/GitHub/Aqueduct30Docker/notebooks/:/mnt/notebooks/ -p 8888:8888 eboraas/jupyter`


run docker:

`docker run -p 4000:80 friendlyhello`


share repo on hub.docker
`docker login`
`docker tag image username/repository:tag`
e.g.: `docker tag friendlyhello rutgerhofste/get-started:part1`
`docker push username/repository:tag`





cleanup

check containers
`docker ps -a`


`docker stop <containerID>`

`docker rm <containerID>`

check images
`docker images`

`docker rmi <imageID>`
