FROM paddleflow/pfs-csi:base

RUN echo "Asia/shanghai" > /etc/timezone
ENV WORKDIR /home/paddleflow
RUN adduser -g paddleflow paddleflow -D
ADD  output/bin/csi-plugin $WORKDIR/csi-plugin
ADD  output/bin/pfs-fuse $WORKDIR/pfs-fuse
ADD  output/bin/cache-worker $WORKDIR/cache-worker
ADD  output/bin/mount.sh $WORKDIR/mount.sh

WORKDIR /home/paddleflow