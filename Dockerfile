FROM ubuntu:22.04
RUN echo 'deb http://archive.ubuntu.com/ubuntu/ focal main restricted' > /etc/apt/sources.list
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y ca-certificates

RUN apt-get install -yq tzdata && \
    ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata


RUN wget https://github.com/Titannet-dao/titan-node/releases/download/v0.1.19/titan-l1-guardian; \
    unzip titan-l1-guardian -d /usr/bin; \
    chmod 0755 /usr/bin/titan-l1-guardian;

VOLUME /mnt/storage;
VOLUME /root/.titanguardian

ENV TITAN_METADATAPATH=/mnt/storage
ENV TITNA_ASSETSPATHS=/mnt/storage

ARG CODE
ARG URL="https://cassini-locator.titannet.io:5000/rpc/v0"

EXPOSE 2345
ENTRYPOINT ["/usr/bin/titan-l1-guardian", "daemon", "start", "--init", "--url $URL", "--code $CODE"]
