FROM google/cloud-sdk:slim

COPY . /workspace/
COPY nltk_data /usr/local/lib/nltk_data

WORKDIR /workspace

RUN apt update && \
    apt install -y python3 python3-pip cmake && \
    apt clean

RUN pip3 install --upgrade --no-cache-dir .
