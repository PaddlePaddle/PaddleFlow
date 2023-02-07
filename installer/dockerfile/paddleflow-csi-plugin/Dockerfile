FROM paddleflow/ubuntu:18.04

ENV WORKDIR /home/paddleflow
RUN groupadd paddleflow && useradd -d /home/paddleflow -g paddleflow  paddleflow
ADD  output/bin/csi-plugin $WORKDIR/csi-plugin
ADD  output/bin/pfs-fuse $WORKDIR/pfs-fuse
ADD  output/bin/cache-worker $WORKDIR/cache-worker
ADD  output/bin/mount.sh $WORKDIR/mount.sh

WORKDIR /home/paddleflow