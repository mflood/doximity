# Dockerfile
#
# defines the docker image that is
# built using ./docker_build.sh
#

FROM python:3.7
COPY frivenmeld /app/frivenmeld
ADD setup.py /app
ADD requirements.txt /app

WORKDIR /app
RUN pip install -r requirements.txt
RUN pip install -e .

CMD ["python", "frivenmeld/driver.py", "--report-date=2017-02-02", "--delete-existing"]

