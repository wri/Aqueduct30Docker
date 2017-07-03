# Aqueduct 3.0 Data Processing workflow

this document explains each and every step for the data processing of Aqueduct 3.0. Everything is here, from raw data to code to explanation. We also epxlain how you could replicate the calculations on your local machine or in a cloud environment. 

The overall structure is as follows:

Data is stored on WRI's Amazon S3 Storage
Code and versionion is stored on Github 
The environment description is stored in a Docker file 
A jupyter notebook can be used to run the code in a virtual machine. 

For steps that do not include code, such as adding columns to a shapefile in QGIS, a description is included.  

1) Run locally

2) Run in the cloud


Instructions to run the calculations locally

install Docker






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
