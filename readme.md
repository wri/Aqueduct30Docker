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
* In the cloud (recommended)

Both options are based on Docker and Jupyter. Although you might be able to do the lion's share of the data processing on your local machine, there are good reasons to work with a cloud based solution

* mount a large harddrive to store the data. you will need appr. 200GB
* easy to pick an appropratia intance size (number of CPU's and RAM)

There are also downsides

* Additional security steps required
* Account needed
* Costs


## Locally 

1. install docker cummunity edition  

[instructions](https://docs.docker.com/engine/installation/#time-based-release-schedule)  

For windows it requires some additional steps. 

2. Start docker  
you can check if docker is installed by typing `docker -v` in your terminal or command prompt  

2. run the following command: 

`docker run --name aqueduct -it -p 8888:8888 rutgerhofste/aqueduct30:v01 bash`  

This will download the docker image and run a container with name aqueduct in -it mode (interactive, tty), forward port 8888 on the container to the localhost port 8888 and execute a bash script. It will be helpful to understand the basics of Docker to understand what you are doing here. Docker will automatically put your terminal or command prompt in your container. It will say root@containerID instead of your normal user. 

3. Open your browser and go to http://localhost:8888

4. Type in the token that you see in your terminal

5. You can now start working on notebooks 

6. To save you progress, please see the section below.  

## Cloud based solution (recommended)

1. Using Amazon or Google Cloud, spin up a virtual instance (Ubuntu)

for this I reccomend using the tutorials that are available on Amazon's and Google's websites. 

For Windows machines you can use Putty and WinSCP to connect to your instance
http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/putty.html 

* add startup.sh to user data during setup in EC2

* you will nedd sigificant storage. I used 200GB of SSD storage ($)

* Set up the appropriate security rules to open your instance for various connection methods, SSH, HTTPS, SCP etc.

* for now I opened the instance to all traffic coming from WRI's US IP address 

* we will setup HTTPS access after we spin up our notebook


2. Connect to your instance using SSH
http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html

check if docker is installed
`docker version`

download the latest docker image for aqueduct. Check https://hub.docker.com/search/?isAutomated=0&isOfficial=0&page=1&pullCount=0&q=rutgerhofste&starCount=0

pull the latest image 

`docker pull rutgerhofste/xxx:vxx`

run your container 

`docker run --name aqueduct -it -p 8888:8888 rutgerhofste/aqueduct:v01 bash`

3. (recommended) Set up HTTPS access

in your container create a certificate by running

`cd /.keys`
`openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout mykey.key -out mycert.pem`

and answer some questions needed for the certificate

Copy config file that includes password and SSL for HTTPS

cp /Aqueduct30Docker/jupyter_notebook_config.py /root/.jupyter/.

now start your notebook with the certificates

jupyter notebook --no-browser --ip=0.0.0.0 --allow-root --certfile=/.keys/mycert.pem --keyfile=/.keys/mykey.key --notebook-dir= /Aqueduct30Docker/ 

Now go to 

https://<your public IP address>:8888

your browser will give you a warning because you are using a self created certificate. Do you trust yourself :) ? 

If you trust yourself, click advanced (Chrome) and proceed to the site. 
Copy the token from you terminal to access your notebooks. 

The notebook will ask you for a password. For now, this password is managed by Rutger Hofste. You can disable or change the password in the configuration file. This will change in the future. 


2. you will need to authenticate for a couple of services including using AWS and Google Earth Engin. 

* ssh into your machine and run bash in your Docker container using the following command  
`docker ps -a`  
check the container name and run  
`docker exec -it <container name> bash`  

now authenticate for AWS  
`aws configure`

and for earth engine (if needed, you can also do this from within Jupyter)  
`earthengine authenticate`  


sync WRI's bucket with your instance (into Docker Volume). You can choose to sync all or just the data you require. The bucket rawData is currently around 100GB so will probably download per task. 



2. Go to your instance's external IP address appended by port 8888 xx.xxx.xx.xx:8888 
3. Login with password (ask me)



# Commit to development

The docker image comes with git intalled and is linked to the following github remote branch:
https://github.com/rutgerhofste/Aqueduct30Docker

in order to commit, please run a terminal from the Jupyter main page (top right corner). 

# Cheatsheet
you can bash into the instance using 
`docker exec -it <container ID> bash`


share repo on hub.docker
`docker login`
`docker tag image username/repository:tag`
e.g.: `docker tag friendlyhello rutgerhofste/get-started:part1`
`docker push username/repository:tag`

Identify yourself on the server git
git config --global user.email "rutgerhofste@gmail.com"
git config --global user.name "Rutger Hofste"




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

aws s3 cp s3://wri-projects/Aqueduct30/rawData/Utrecht/yoshi20161219/waterdemand  /volumes/data/ --recursive


Using Putty and want to edit a file in nano/vim: 
`export TERM=xterm` 

due to some weird bug

# Earth Engine Javascript API files 

The javascript files for earth engine can be added to your earth engine code editor (code.earthengine.com) by using the following URL

https://code.earthengine.google.com/?accept_repo=aqueduct30

note to self: If you make changes in the online code editor and want to push to this github, use 

git clone https://earthengine.googlesource.com/aqueduct30

git pull origin


# Recommanded, add HTTPS Security

Run on your instance (not in docker container)

openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout mykey.key -out mycert.pem

Put the private and public key in folder that matches the patch in your jupyter config file

if needed change the path in your jupyter config file

run your container

copy files to container

docker run -it -p 8888:8888 testjupyter:v01 bash

cd /usr/local/bin/

