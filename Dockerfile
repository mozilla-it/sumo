FROM google/cloud-sdk:slim

COPY . /workspace/

WORKDIR /workspace

RUN pip install --upgrade --no-cache-dir .
