FROM google/cloud-sdk:slim

COPY . /workspace/

WORKDIR /workspace

RUN apt update && \
    apt install -y python3 python3-pip && \
    apt clean

RUN pip3 install --upgrade --no-cache-dir .
