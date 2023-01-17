FROM paddleflow/pfs-csi:base

RUN echo "Asia/shanghai" > /etc/timezone
ENV WORKDIR /home/paddleflow
RUN adduser -g paddleflow paddleflow -D
ADD  output/bin/csi-plugin $WORKDIR/csi-plugin
ADD  output/bin/pfs-fuse $WORKDIR/pfs-fuse
ADD  output/bin/cache-worker $WORKDIR/cache-worker
ADD  output/bin/nsenter $WORKDIR/nsenter
ADD  output/bin/start.sh $WORKDIR/start.sh
ADD output/bin/connector $WORKDIR/connector
ADD output/bin/paddleflow-csiplugin-connector.service $WORKDIR/paddleflow-csiplugin-connector.service
RUN chmod +x  $WORKDIR/csi-plugin && RUN chmod +x  $WORKDIR/pfs-fuse && RUN chmod +x $WORKDIR/cache-worker && \
    RUN chmod +x $WORKDIR/start.sh && RUN chmod +x  $WORKDIR/paddleflow-csiplugin-connector && RUN chmod +x  $WORKDIR/paddleflow-csiplugin-connector.service

WORKDIR /home/paddleflow