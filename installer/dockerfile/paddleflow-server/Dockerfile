FROM paddleflow/ubuntu:1.4.6

ENV WORKDIR /home/paddleflow
ADD output/bin/paddleflow $WORKDIR/server/
RUN groupadd paddleflow && useradd -d /home/paddleflow -g paddleflow paddleflow && chown -R paddleflow:paddleflow $WORKDIR/server
USER paddleflow