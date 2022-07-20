# The official Canonical Ubuntu Bionic image is ideal from a security perspective,
# especially for the enterprises that we, the RabbitMQ team, have to deal with
FROM ubuntu:20.04

RUN set -eux; \
        apt-get update; \
        apt-get install -y lsb-release ubuntu-dbgsym-keyring; \
        echo "deb http://ddebs.ubuntu.com $(lsb_release -cs) main restricted universe multiverse" > /etc/apt/sources.list.d/ddebs.list; \
        echo "deb http://ddebs.ubuntu.com $(lsb_release -cs)-updates main restricted universe multiverse" >> /etc/apt/sources.list.d/ddebs.list; \
        echo "deb http://ddebs.ubuntu.com $(lsb_release -cs)-proposed main restricted universe multiverse" >> /etc/apt/sources.list.d/ddebs.list; \
        apt-get update; \
        apt-get install -y --no-install-recommends \
 # grab gosu for easy step-down from root
        libc6-dbg \
        libgcc-s1-dbgsym \
        libstdc++6-dbgsym \
        libtinfo6-dbgsym \
        zlib1g-dbgsym
