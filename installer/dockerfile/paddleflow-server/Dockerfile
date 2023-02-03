FROM paddleflow/alpine:3.13-shanghai-tz

ENV WORKDIR /home/paddleflow
ADD output/bin/paddleflow $WORKDIR/server/

RUN adduser -g paddleflow paddleflow -D && chown -R paddleflow:paddleflow $WORKDIR/server
USER paddleflow