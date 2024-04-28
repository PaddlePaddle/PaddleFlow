FROM paddleflow/ubuntu:20.04-base

ENV WORKDIR /home/paddleflow
ADD output/bin/paddleflow $WORKDIR/server/
RUN groupadd paddleflow && useradd -d /home/paddleflow -g paddleflow paddleflow && chown -R paddleflow:paddleflow $WORKDIR/server && \
    apt-get update -y && apt-get install -y openssl ca-certificates
USER paddleflow
