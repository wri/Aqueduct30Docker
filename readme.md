# Work in Progres!!

# Aqueduct 3.0 Data Processing workflow
## A modern geospatial analysis setup based on Jupyter

View this readme on Github: https://github.com/rutgerhofste/Aqueduct30Docker  

Docker Hub: https://hub.docker.com/u/rutgerhofste/  

this document explains each and every step for the data processing of Aqueduct 3.0. Everything is here, from raw data to code to explanation. We also epxlain how you could replicate the calculations on your local machine or in a cloud environment. 

The overall structure is as follows:

* Data is stored on WRI's Amazon S3 Storage
* Code and versionion is stored on Github 
* The environment description is stored in a Docker file 
* A jupyter notebook can be used to run the code in a virtual machine. 

For steps that do not include code, such as adding columns to a shapefile in QGIS, a description is included to replicate the proces on your local machine. Note that this setup is currently not compatible with ArcPY.


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

3. Run a Docker Container:  
`docker run --name aqueduct -it -p 8888:8888 rutgerhofste/gisdocker:latest bash`  
This will download the docker image and run a container with name aqueduct in -it mode (interactive, tty), forward port 8888 on the container to the localhost port 8888 and execute a bash script. It will be helpful to understand the basics of Docker to understand what you are doing here. Docker will automatically put your terminal or command prompt in your container. It will say root@containerID instead of your normal user. 

4. Update the git repository  
`cd /Aqueduct30docker/`  
`git pull origin`

5. Launch a Jupyter Notebook server  
`jupyter notebook --no-browser --ip=0.0.0.0 --allow-root --notebook-dir= /Aqueduct30Docker/`

6. Open your browser and go to http://localhost:8888

7. Type in the token that you see in your terminal  
You can now start working on notebooks 

8. To save you progress, please see the section below.  

## Cloud based solution (recommended)

1. Get familair with how to use Amazon (EC2) or Google Cloud (CE) virtual instances:  
for this I reccomend using the tutorials that are available on Amazon's and Google's websites.  
[Amazon tutorial](https://aws.amazon.com/ec2/getting-started/)

2. Use the specifics below when setting up you EC2 instance. If you miss one step, your instance will likely not work.  
    1. In step 1) select Ubuntu Server 16.04 LTS (HVM), SSD Volume Type
    2. In step 2), if your budget allows, choose T2.Medium  
    [calculate costs](https://calculator.s3.amazonaws.com/index.html)
    3. In step 3) make sure
        1. If you are within a VPC, allow IP addresses to be set  
        Auto-assign Public IP = enable
        2. Under advanced details, set user data to as file and upload the startup.sh script from the /other folder on Github.
    4. in step 4) add storage
    	depending on the steps in the data process, we recommend setting the size to 200GB.  
    	[calculate costs](https://calculator.s3.amazonaws.com/index.html)
    5. in step 5) add tags  
    	optionally you can set a name for your instance
    6. in step 6) Set the appropriate security rules.  
    	This is a crucial step. Eventually we will communicate over SSH (port 22) and HTTPS (port 443). You can whitelist your IP address or allow traffic from everywhere. As a minimum you need to allow SSH and HTTPS from your IP address. If you want to do testing with HTTP you can temporarily allow HTTP (port 80) traffic.
    7. Launch your instance
3. Connect to your instance using SSH
[Connect to your instance](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html)  
For windows [PUTTY](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/putty.html) is recommended, for Mac and Linux you can use your terminal. 

4. Once logged in into your system  
check if docker is installed
`docker version`

download the latest docker image for aqueduct. Check https://hub.docker.com/search/?isAutomated=0&isOfficial=0&page=1&pullCount=0&q=rutgerhofste&starCount=0

pull the latest image 

`docker pull rutgerhofste/xxx:vxx`

run your container 

`docker run --name aqueduct -it -p 8888:8888 rutgerhofste/gisdocker:latest bash`

3. (recommended) Set up HTTPS access

in your container create a certificate by running

`cd /.keys`
`openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout /.keys/mykey.key -out /.keys/mycert.pem`

and answer some questions needed for the certificate

Setup SSH access keys:
https://help.github.com/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent/

`ssh-keygen -t rsa -b 4096 -C "rutgerhofste@gmail.com"`

`cat /root/.ssh/id_rsa.pub`


Clone (using SSH) to get the appropriate config file. 

`cd /`

`git clone git@github.com:rutgerhofste/Aqueduct30Docker.git`

Copy config file that includes password and SSL for HTTPS

# no longer required
cp /Aqueduct30Docker/jupyter_notebook_config.py /root/.jupyter/.

now start your notebook with the certificates

jupyter notebook --no-browser --ip=0.0.0.0 --allow-root --certfile=/.keys/mycert.pem --keyfile=/.keys/mykey.key --notebook-dir= /Aqueduct30Docker/ --config=/Aqueduct30Docker/jupyter_notebook_config.py

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

Windows remove none images
FOR /f "tokens=*" %i IN ('docker images -q -f "dangling=true"') DO docker rmi %i


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


# Recommended, add HTTPS Security

Run on your instance (not in docker container)

openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout mykey.key -out mycert.pem

Put the private and public key in folder that matches the patch in your jupyter config file

if needed change the path in your jupyter config file

run your container

copy files to container

docker run -it -p 8888:8888 testjupyter:v01 bash

cd /usr/local/bin/

docker images -q --filter "dangling=true" | xargs -r docker rmi

# Jupyter Hub 

`docker run -it -p 8000:8000 rutgerhofste/jupyterhub:v02 bash`

clone latest git repo

Create certificates
`openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout /.keys/jupyterhub.key -out /.keys/jupyterhub.crt`


Set environment variables

# Create these values: https://github.com/settings/applications/new
export GITHUB_CLIENT_ID=from_github
export GITHUB_CLIENT_SECRET=also_from_github
export OAUTH_CALLBACK_URL=https://[YOURDOMAIN]/hub/oauth_callback

Run jupyterhub in folder with jupyterhub_config.py

`jupyterhub`



# Setup Github using SSH
https://jdblischak.github.io/2014-09-18-chicago/novice/git/05-sshkeys.html

cd ~/.ssh
ssh-keygen -t rsa -C "rutgerhofste@gmail.com"

no passphrase
default folder

cat ~/.ssh/id_rsa.pub

Add on github

git clone ssh://git@github.com:rutgerhofste/Aqueduct30Docker.git





