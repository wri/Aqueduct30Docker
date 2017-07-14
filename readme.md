# Aqueduct 3.0 Data Processing workflow

View this readme on Github: https://github.com/rutgerhofste/Aqueduct30Docker

this document explains each and every step for the data processing of Aqueduct 3.0. Everything is here, from raw data to code to explanation. We also epxlain how you could replicate the calculations on your local machine or in a cloud environment. 

The overall structure is as follows:

* Data is stored on WRI's Amazon S3 Storage
* Code and versionion is stored on Github 
* The environment description is stored in a Docker file 
* A jupyter notebook can be used to run the code in a virtual machine. 

For steps that do not include code, such as adding columns to a shapefile in QGIS, a description is included to replicate the proces on your local machine.  


A link to the flowchart:
https://docs.google.com/drawings/d/1IjTVlQUHNYj2w0zrS8SKQV1Bpworvt0XDp7UE2tPms0/edit?usp=sharing

![Flowchart](/other/flowchart.png)

Each data source (pristine data), indicated with the open cylinder on the right side, is stord on our S3 drive on the rawData folder: wri-projects/Aqueduct30/rawData

The pristine data is also copied to step 0 in the data processing folder: wri-projects/Aqueduct30/processData

# Technical Setup

A link to edit the technical setup drawing:
https://docs.google.com/drawings/d/1UR62IEQwQChj2SsksMsYGBb5YnVu_VaZlG10ZGowpA4/edit?usp=sharing

![Setup](/other/setup.png)


# Getting started

There are two options to setup your working environment:

* Locally
* In the cloud

Both options are based on Docker and Jupyter. 

## Locally 

1. install docker 
2. run the following command: 

`docker run -d -p 8888:8888 rutgerhofste/aqueduct30:latest`

3. Open your browser and go to http://localhost:8888
4. Type in the password

## In the cloud

1. Using Amazon or Google Cloud, spin up a virtual instance (Ubuntu)

for this I reccomend using the tutorials that are available on Amazon's and Google's websites. 

For Windows machines you can use Putty and WinSCP to connect to your instance
http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/putty.html 

* add startup.sh to user data during setup in EC2

* Set up the appropriate security rules to open your instance for various connection methods, SSH, HTTPS, SCP etc.

* for now I opened the instance to all traffic coming from WRI's US IP address 

2. Go to your instance's external IP address appended by port 8888 xx.xxx.xx.xx:8888 
3. Login with password



# Commit to development

The docker image comes with git intalled and is linked to the following github remote branch:
https://github.com/rutgerhofste/Aqueduct30Docker

in order to commit, please run a terminal from the Jupyter main page (top right corner). 

#Cheatsheet
you can bash into the instance using 
`docker exec -it <container ID> bash`


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




Safe way:
run bash on docker container and use AWS configure
aws configure

us-east-1


aws configure 

Copy files to volume 

aws s3 cp s3://wri-projects/Aqueduct30/temp  /volumes/data/ /volumes/data/ --recursive


Using Putty and want to edit a file in nano/vim: 
`export TERM=xterm` 

due to some weird bug










