FROM paddleflow/ubuntu:20.04-base

ENV WORKDIR /home/paddleflow
RUN groupadd paddleflow && useradd -d /home/paddleflow -g paddleflow  paddleflow && \
    apt-get update -y && apt-get install -y openssl ca-certificates
ADD  output/bin/csi-plugin $WORKDIR/csi-plugin
ADD  output/bin/pfs-fuse $WORKDIR/pfs-fuse
ADD  output/bin/cache-worker $WORKDIR/cache-worker
ADD  output/bin/mount.sh $WORKDIR/mount.sh

WORKDIR /home/paddleflow
