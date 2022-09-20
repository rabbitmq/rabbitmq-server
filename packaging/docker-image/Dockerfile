# The official Canonical Ubuntu Bionic image is ideal from a security perspective,
# especially for the enterprises that we, the RabbitMQ team, have to deal with
ARG BASE=ubuntu
FROM ${BASE}:20.04 as otp

RUN set -eux; \
	\
	apt-get update; \
	apt-get install --yes --no-install-recommends \
		autoconf \
		ca-certificates \
		dpkg-dev \
		gcc \
		g++ \
		libncurses5-dev \
		make \
		wget \
	; \
	rm -rf /var/lib/apt/lists/*

ENV OPENSSL_VERSION 1.1.1g
ENV OPENSSL_SOURCE_SHA256="ddb04774f1e32f0c49751e21b67216ac87852ceb056b75209af2443400636d46"

# we skip the cert check because it's broken on arm32v7, and we have a checksum anyway
# https://github.com/dotnet/runtime/issues/66310
RUN set -eux; \
	\
	OPENSSL_SOURCE_URL="https://www.openssl.org/source/openssl-$OPENSSL_VERSION.tar.gz"; \
	OPENSSL_PATH="/usr/local/src/openssl-$OPENSSL_VERSION"; \
	OPENSSL_CONFIG_DIR=/usr/local/etc/ssl; \
	\
	wget \
		--no-check-certificate \
		--progress dot:giga \
		--output-document "$OPENSSL_PATH.tar.gz" \
		"$OPENSSL_SOURCE_URL"; \
	echo "$OPENSSL_SOURCE_SHA256 *$OPENSSL_PATH.tar.gz" | sha256sum --check --strict -; \
	mkdir -p "$OPENSSL_PATH"; \
	tar --extract --file "$OPENSSL_PATH.tar.gz" --directory "$OPENSSL_PATH" --strip-components 1

ADD build_install_openssl.sh /usr/local/src

RUN set -eux; \
	\
	/usr/local/src/build_install_openssl.sh; \
	rm /usr/local/src/build_install_openssl.sh

ARG OTP_VERSION=25.0.4
ENV OTP_VERSION ${OTP_VERSION}
# TODO add PGP checking when the feature will be added to Erlang/OTP's build system
# http://erlang.org/pipermail/erlang-questions/2019-January/097067.html
ARG OTP_SHA256=05878cb51a64b33c86836b12a21903075c300409b609ad5e941ddb0feb8c2120
ENV OTP_SOURCE_SHA256=${OTP_SHA256}

RUN set -eux; \
	OTP_SOURCE_URL="https://github.com/erlang/otp/archive/OTP-$OTP_VERSION.tar.gz"; \
	OTP_PATH="/usr/local/src/otp-OTP-$OTP_VERSION"; \
	\
# Download, verify & extract OTP_SOURCE
	mkdir -p "$OTP_PATH"; \
	wget \
		--no-check-certificate \
		--progress dot:giga \
		--output-document "$OTP_PATH.tar.gz" \
		"$OTP_SOURCE_URL"; \
	echo "$OTP_SOURCE_SHA256 *$OTP_PATH.tar.gz" | sha256sum --check --strict -; \
	tar --extract --file "$OTP_PATH.tar.gz" --directory "$OTP_PATH" --strip-components 1; \
	rm "$OTP_PATH.tar.gz"

ADD build_install_otp.sh /usr/local/src

RUN set -eux; \
	\
	/usr/local/src/build_install_otp.sh; \
	rm /usr/local/src/build_install_otp.sh

FROM ${BASE}:20.04 as rabbitmq

RUN set -eux; \
	\
	apt-get update; \
	apt-get install --yes --no-install-recommends \
		gosu \
		python3 \
		dstat \
		sysstat \
		htop \
		nmon \
		tmux \
		neovim \
	; \
	rm -rf /var/lib/apt/lists/*

ARG OPENSSL_CONFIG_DIR=/usr/local/etc/ssl
COPY --from=otp ${OPENSSL_CONFIG_DIR} ${OPENSSL_CONFIG_DIR}
RUN ln -sf /etc/ssl/certs /etc/ssl/private "${OPENSSL_CONFIG_DIR}"

ARG OTP_INSTALL_DIR=/opt/otp
COPY --from=otp ${OTP_INSTALL_DIR} ${OTP_INSTALL_DIR}

ADD 10-default-guest-user.conf /opt
ADD docker-entrypoint.sh /opt
ADD install_rabbitmq.sh /opt

ARG RABBITMQ_ARCHIVE=package-generic-unix.tar
ADD ${RABBITMQ_ARCHIVE} /opt

ENV RABBITMQ_DATA_DIR /var/lib/rabbitmq
ENV RABBITMQ_HOME /opt/rabbitmq
ENV RABBITMQ_LOGS -

ADD install_rabbitmq.sh /usr/local/src

RUN set -eux; \
	\
	/usr/local/src/install_rabbitmq.sh; \
	rm /usr/local/src/install_rabbitmq.sh

ENV HOME ${RABBITMQ_DATA_DIR}
ENV PATH /opt/otp/bin:${RABBITMQ_HOME}/sbin:${PATH}
ENV LANG C.UTF-8
ENV LANGUAGE C.UTF-8
ENV LC_ALL C.UTF-8

VOLUME ${RABBITMQ_DATA_DIR}

ENTRYPOINT ["docker-entrypoint.sh"]

# EPMD AMQP-TLS AMQP ERLANG
EXPOSE 4369 5671 5672 25672
CMD ["rabbitmq-server"]

# MANAGEMENT-TLS MANAGEMENT
EXPOSE 15671 15672
# PROMETHEUS-TLS PROMETHEUS
EXPOSE 15691 15692
# STREAM-TLS STREAM
EXPOSE 5551 5552
# MQTT-TLS MQTT
EXPOSE 8883 1883
# WEB-MQTT-TLS WEB-MQTT
EXPOSE 15676 15675
# STOMP-TLS STOMP
EXPOSE 61614 61613
# WEB-STOMP-TLS WEB-STOMP
EXPOSE 15673 15674
# EXAMPLES
EXPOSE 15670
