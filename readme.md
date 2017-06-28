hello world

build docker:
`docker build -t friendlyhello testDocker`

run docker:

`docker run -p 4000:80 friendlyhello`


share repo on hub.docker
`docker login`
`docker tag image username/repository:tag`
e.g.: `docker tag friendlyhello rutgerhofste/get-started:part1`
`docker push username/repository:tag`





cleanup

check containers
`docker pd -a`


`docker stop <containerID>`

`docker rm <containerID>`

check images
`docker images`

`docker rmi <imageID>`
