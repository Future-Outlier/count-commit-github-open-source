FROM ubuntu:21.10

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y git curl golang git make

# add script
COPY loop.sh /
RUN chmod +x /loop.sh

# add user
ARG USER=jenkins
RUN groupadd $USER
RUN useradd -ms /bin/bash -g $USER $USER

# change user
USER $USER

# install golangci
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.33.0

# clone core
ARG CORE_REPO=https://github.com/apache/incubator-yunikorn-core.git
RUN git clone $CORE_REPO /home/$USER/incubator-yunikorn-core
WORKDIR /home/$USER/incubator-yunikorn-core

ARG BRANCH=master
RUN git config pull.rebase false
RUN git checkout $BRANCH
RUN make test

# clone k8shim
ARG K8SHIM_REPO=https://github.com/apache/incubator-yunikorn-k8shim.git
RUN git clone $K8SHIM_REPO /home/$USER/incubator-yunikorn-k8shim
WORKDIR /home/$USER/incubator-yunikorn-k8shim

ARG BRANCH=master
RUN git config pull.rebase false
RUN git checkout $BRANCH
RUN make test