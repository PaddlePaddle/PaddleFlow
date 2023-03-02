FROM paddleflow/ubuntu:18.04

ENV WORKDIR /home/paddleflow
RUN apt-get update && apt-get install -y software-properties-common

ENV GLUSTER_VERSON 7
RUN add-apt-repository ppa:gluster/glusterfs-${GLUSTER_VERSON} -y

RUN apt-get install glusterfs-client fuse tzdata -y && \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

ADD output/bin/paddleflow $WORKDIR/server/
RUN groupadd paddleflow && useradd -d /home/paddleflow -g paddleflow paddleflow && chown -R paddleflow:paddleflow $WORKDIR/server

USER root
