## Work in Progres!!

# WRI Aqueduct 3.0 Data Processing workflow

Througout the readme, variables that you need to replace with your own variable are indicated in greater than and smaller than signs `<variableYouNeedToReplace>`

If you are not viewing this document on Github, please find a stylized version [here](https://github.com/rutgerhofste/Aqueduct30Docker)   
The coding environment uses Docker images that can be found [here](https://hub.docker.com/r/rutgerhofste/gisdocker/)

this document explains each and every step for the data processing of Aqueduct 3.0. Everything is here, from raw data to code to explanation. We also epxlain how you could replicate the calculations on your local machine or in a cloud environment. 

The overall structure is as follows:

* Data is stored on WRI's Amazon S3 Storage
* Code and versionion is stored on Github 
* The Python environment description is stored in a Docker Image  
* Coding and dat operation are done in Jupyter Notebooks  

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

* mount a large harddrive to store the data. you will need appr. 300GB
* easy to pick an appropriata instance size (number of CPU's and RAM)

There are also downsides

* Additional security steps required
* Account(s) needed
* Costs


## Locally 

Requirements:
* The Docker image requires approximately 12GB of Storage and is not a lightweight solution. 
* If you want to replicate the Aqueduct data processing steps, you will need approximately 300GB of disk space. 

1. install docker cummunity edition  
[instructions](https://docs.docker.com/engine/installation/#time-based-release-schedule)  
For windows it requires some additional steps and might require enabling Hyper-V virtualization. There are cases in which you have to enable this in your BIOS. 

1. Start docker  
you can check if docker is installed by typing `docker -v` in your terminal or command prompt. If you ever got stuck in one of the next steps or closed your terminal window it is important to understand some basic docker commands. First, you need to understand the concpet of an [image and a container](https://stackoverflow.com/questions/23735149/docker-image-vs-container). You can list your images using `docker images` and you can list your active containers using `docker ps` and all your containers using `docker ps -a`. If your container is still running you can bash (terminal) into your container using `docker exec -it <container name> bash`. Furthermore you can delete containers using `docker rm -f <ContainerName>` and images using `docker rmi <imageName>`. I also created a couple of [cheatsheets](https://github.com/rutgerhofste/cheatsheets) for various tools. 

1. Run a Docker Container:  
`docker run --name aqueduct -it -p 8888:8888 rutgerhofste/gisdocker:latest bash`  
This will download the docker image and run a container with name aqueduct in -it mode (interactive, tty), forward port 8888 on the container to the localhost port 8888 and execute a bash script. It will be helpful to understand the basics of Docker to understand what you are doing here. Docker will automatically put your terminal or command prompt in your container. It will say root@containerID instead of your normal user. You can tell if you are in a container by the first characters in you terminal. It will state something like "root@240c3eb5620e:/#" indicating you are a root user on the virtual machine named "240c3eb5620e". The code will be different in your case. 

1. Setup Security certificates:  
in your container create a certificate by running:  
`openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout /.keys/mykey.key -out /.keys/mycert.pem`  
You are asked some quesions like country name etc. which you can leave blank. Just press return a couple of times.

1. Clone the Git repository
You have two options here: 1) Clone the Aqueduct Repository 2) Create a so-called fork of the Aqueduct Project and work in the fork. The first option requires you to be added as a collaborator in order to be able to push your edits to the repo. The latter option allows you to work independent from the official Aqueduct repo. You will need to make a pull request to have your edits incorporated in the main repo of Aqueduct3.0.
    1. Option 1) Clone original Aqueduct3.0 repository:  
While in your Docker Image (root@... $ )   
`mkdir /volumes/repos` (might already exist)    
`git clone https://github.com/rutgerhofste/Aqueduct30Docker.git /volumes/repos/Aqueduct30Docker/`  
    1. Option 2) Fork repository first  
Fork repository on [Github](https://github.com/rutgerhofste/Aqueduct30Docker)  
Learn more about how forking works [here](https://help.github.com/articles/fork-a-repo/)  
`mkdir /volumes/repos` (might already exist)  
`git clone https://github.com/<Replace with your Github username>/Aqueduct30Docker.git /volumes/repos/Aqueduct30Docker/`

1. Create a TMUX session before spinning up your Jupyter Notebook server  
Although this is an extra step, it will allow you to have multiple windows open and allows you to detach and attac in case you lose a connection. `tmux new -s aqueducttmux`.  

1. Split your session window into two panes using `crtl-b "`. The way TMUX works is that you press `crtl+b`, release it and then press `"`. [more info on TMUX](https://www.youtube.com/watch?v=BHhA_ZKjyxo&t=213s). You can change panes by using `ctrl-b o` (opposite).  

1. In one of your panes, launch a Jupyter Notebook server  
`jupyter notebook --no-browser --ip=0.0.0.0 --allow-root --certfile=/.keys/mycert.pem --keyfile=/.keys/mykey.key --notebook-dir= /volumes/repos/Aqueduct30Docker/ --config=/volumes/repos/Aqueduct30Docker/jupyter_notebook_config.py`

1. Open your browser and go to http://localhost:8888  
The standard password for your notebooks is `Aqueduct2017!`, you can change this [later](http://jupyter-notebook.readthedocs.io/en/latest/public_server.html)

1. Congratulations, you can start running code in your browser. This tuturial continues in the section [Additional Steps After Starting your jupyter Notebook server](#additional-steps-after-starting-your-jupyter-notebook-server)

## Cloud based solution (recommended)

1. Get familair with how to use Amazon (EC2) or Google Cloud (CE) virtual instances:  
for this I reccomend using the tutorials that are available on Amazon's and Google's websites.  
[Amazon tutorial](https://aws.amazon.com/ec2/getting-started/)

1. Use the specifics below when setting up you EC2 instance. If you miss one step, your instance will likely not work.  
    1. In step 1) select Ubuntu Server 16.04 LTS (HVM), SSD Volume Type
    1. In step 2), if your budget allows, choose T2.Medium  
    [calculate costs](https://calculator.s3.amazonaws.com/index.html)
    1. In step 3) make sure
        1. If you are within a VPC, allow IP addresses to be set  
        Auto-assign Public IP = enable
        2. Under advanced details, set user data to as file and upload the startup.sh script from the /other folder on Github.
    1. in step 4) add storage
    	depending on the steps in the data process, we recommend setting the size to 200GB.  
    	[calculate costs](https://calculator.s3.amazonaws.com/index.html)
    1. in step 5) add tags  
    	optionally you can set a name for your instance
    1. in step 6) Set the appropriate security rules.  
    	This is a crucial step. Eventually we will communicate over SSH (port 22) and HTTPS (port 443). You can whitelist your IP address or allow traffic from everywhere. As a minimum you need to allow SSH and HTTPS from your IP address. If you want to do testing with HTTP you can temporarily allow HTTP (port 80) traffic.
    1. Launch your instance
1. Connect to your instance using SSH
[Connect to your instance](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html)  
For windows [PUTTY](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/putty.html) is recommended, for Mac and Linux you can use your terminal. 
1. Once logged in into your system  
check if docker is installed
`docker version`
1. download the latest docker image for aqueduct. Check https://hub.docker.com/search/?isAutomated=0&isOfficial=0&page=1&pullCount=0&q=rutgerhofste&starCount=0
run your container  
`docker run --name aqueduct -it -p 8888:8888 rutgerhofste/gisdocker:latest bash`
1. (recommended) Set up HTTPS access
in your container create a certificate by running  
`openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout /.keys/mykey.key -out /.keys/mycert.pem`
and answer some questions needed for the certificate  

1. Optional: Setup SSH access keys:   
https://help.github.com/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent/  
`ssh-keygen -t rsa -b 4096 -C "rutgerhofste@gmail.com"`  
`cat /root/.ssh/id_rsa.pub`  
1. Clone your repo in a new folder    
`mkdir /volumes/repos`  
`cd /volumes/repos`  
If you setup github SSH (see above):  
`git clone git@github.com:rutgerhofste/Aqueduct30Docker.git`  
otherwise:  
`git clone https://github.com/<Replace with your Github username>/Aqueduct30Docker.git /volumes/repos/Aqueduct30Docker/`  
You might have to specify credentials.  

1. Create a TMUX session before spinning up your Jupyter Notebook server.    
Although this is an extra step, it will allow you to have multiple windows open and allows you to detach and attach in case you lose a connection. `tmux new -s aqueducttmux` 

1. Split your session window into two panes using `crtl-b "` The way TMUX works is that you press `crtl+b`, release it and then press `"`. [more info on TMUX](https://www.youtube.com/watch?v=BHhA_ZKjyxo&t=213s). You can change panes by using `ctrl-b o` (opposite).  

1. Start your notebook with the certificates  
`jupyter notebook --no-browser --ip=0.0.0.0 --allow-root --certfile=/.keys/mycert.pem --keyfile=/.keys/mykey.key --notebook-dir= /volumes/repos/Aqueduct30Docker/ --config=/volumes/repos/Aqueduct30Docker/jupyter_notebook_config.py`  
1. in your browser, go to: 
`https://<your public IP address>:8888`
You can find your public IP address on the overview page of amazon EC2. your browser will give you a warning because you are using a self created certificate. Do you trust your self created certificate?  
If you trust yourself, click advanced (Chrome) and proceed to the site. 
The current config file is password protected. I will change to something generic in the future. If you want to change this password please see this [link](http://jupyter-notebook.readthedocs.io/en/latest/public_server.html)  
1. The standard password for your notebooks is `Aqueduct2017!`, you can change this [later](http://jupyter-notebook.readthedocs.io/en/latest/public_server.html)  
1. Congratulations you are up and running. to make most use of these notebooks, you will need to authenticate for a couple of services including using AWS and Google Earth Engine. 

# Additional Steps After Starting your jupyter Notebook server  
Let's check what we've done so far. You are now able to connect to a jupyter notebook server that either runs locally or in the cloud. In addition to your browser, you have an open terminal (or command prompt) window open with two TMUX panes. One is logging what is happening on your Jupyter notebook server, the other is idle but connected to you container. You can tell if you are in a container by the username and machine name in your window. It should say something like root@240c3eb5620e:. remember that you can switch panes by `ctrl-b o` 

1. Authenticate for AWS  
In your tmux pane type
`aws configure`

you should now be able to provide your AWS credentials. Please ask Susan Minnemeyer if you haven't received those already. 

1. Autenticate for Google Cloud SDK  
similar to AWS, you might need Google Cloud acces. 

1. Autenticate for Earth Engine
and for earth engine (if needed, you can also do this from within Jupyter)  
`earthengine authenticate`  




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





