FROM google/cloud-sdk

COPY . /workspace/

WORKDIR /workspace

RUN apt-get update && apt-get install python3 \
    && apt-get install python3-pip -y\
    && pip3 install --upgrade --no-cache-dir .

