FROM ubuntu:bionic

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -y update && \
    apt-get -y upgrade

RUN apt-get -y install sudo

COPY ./example /ci
ADD --chown=664 ./example/setup.sh /ci/setup.sh

RUN ls /ci

RUN /ci/setup.sh

ENTRYPOINT service slapd start && service slapd status && bash
