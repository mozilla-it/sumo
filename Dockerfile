FROM python:3

COPY . /workspace/

WORKDIR /workspace

RUN pip3 install --no-cache-dir .

#RUN apt-get update && apt-get -y install python3 \
#    && apt-get install python3-pip -y\
#    && pip3 install --no-cache-dir .
