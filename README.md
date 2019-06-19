# frivenmeld

Doximity Coding Challenge Submission

- Consumes data from two source, merges and writes to mysql
- Throttles memory usage using maxsize on input queues
- Consumers work in their own threads
- Scalable to hundreds of workers by giving each worker a range of data to consume

## How to run the job

----------------------
1. Configuration
----------------------
    First, Set up environment variables in one or both of these two files,
    depending on if you want to run in docker or in a virtual env.

        local_env.sh  (for virtualenv runs)
        docker.env    (used for docker runs)

    Specifically, you'll need to set the username/password.

    I've set up the target mysql server with the same credentials
    so both sets of username/password are the same. Optionally,
    you can create the table in your own mysql server using
    the scripts in the schema/ folder. As a final option,
    you can leave the WRITE vars alone, and run the python
    script in dryrun mode.


----------------------
2. Build Virtual Env or Docker Image
----------------------

    2a. Run setup.sh to set up the virtual env.

        cd ./app
        ./setup.sh

        Note: setup.sh needs python3, pip, virtualenv

OR

    2b. Build the docker image

        cd ./app
        ./docker_build.sh

-----------------------
3. Run the app in Virtual Env or Docker
-----------------------

    3a. Run in a virtual env

        ./run_job.sh > output.txt

OR

    3b. Run in docker container    

        ./docker_run.sh


--------------------------
Appendix A - Running on EC2
--------------------------

sudo yum install git
git clone https://gitlab.com/rudeserver/data-engineering
cd data-engineering
git checkout matthew-flood

sudo yum install docker
sudo service docker status
sudo service docker start
sudo service docker status

cd app/
sudo ./docker_build.sh
vim docker.env    # <- configure docker.env
sudo ./docker_run.sh
