from lambci/lambda:build-python3.8

ENV AWS_DEFAULT_REGION us-east-1

RUN yum install -y \
    boost-devel \
    jemalloc-devel \
    bison \
    flex \
    autoconf \
    ninja-build \
    python38-devel

ENTRYPOINT ["/bin/sh","-c"]
