FROM ubuntu:jammy-20240405
# ubuntu 22.04

LABEL org.opencontainers.image.authors="Titan Network,Goudan Woo"

ARG TARGETOS
ARG TARGETARCH

RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -r /var/lib/apt/lists/*

COPY --chmod=0755 build/cross/${TARGETOS}/${TARGETARCH}/* /usr/bin/

VOLUME [ "/root/.titanedge" ]
EXPOSE 1234/tcp 1234/udp

ENTRYPOINT [ "titan-edge" ]
CMD [ "daemon", "start", "--init", "--url", "https://test-locator.titannet.io:5000/rpc/v0" ]
