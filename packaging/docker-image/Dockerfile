# The official Canonical Ubuntu Bionic image is ideal from a security perspective,
# especially for the enterprises that we, the RabbitMQ team, have to deal with
ARG BASE=ubuntu
FROM ${BASE}:22.04 as openssl-builder

# Install dependencies required to build Erlang/OTP from source
# https://erlang.org/doc/installation_guide/INSTALL.html
# autoconf: Required to configure Erlang/OTP before compiling
# dpkg-dev: Required to set up host & build type when compiling Erlang/OTP
# gnupg: Required to verify OpenSSL artefacts
# libncurses5-dev: Required for Erlang/OTP new shell & observer_cli - https://github.com/zhongwencool/observer_cli
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
		make

ARG OPENSSL_VERSION
ENV OPENSSL_VERSION=${OPENSSL_VERSION}
ADD openssl-${OPENSSL_VERSION}.tar.gz /usr/local/src
ADD build_install_openssl.sh /usr/local/src

RUN /usr/local/src/build_install_openssl.sh

FROM openssl-builder as otp-builder

ADD OTP.tar.gz /usr/local/src
ADD build_install_otp.sh /usr/local/src

RUN /usr/local/src/build_install_otp.sh

ARG BASE=ubuntu
FROM ${BASE}:20.04

COPY --from=otp-builder /usr/local/lib/erlang /usr/local/lib/erlang

RUN \
# Check that OpenSSL still works after purging build dependencies
	openssl version; \
# Check that Erlang/OTP crypto & ssl were compiled against OpenSSL correctly
	erl -noshell -eval 'io:format("~p~n~n~p~n~n", [crypto:supports(), ssl:versions()]), init:stop().'


RUN set -eux; \
        apt-get update; \
        apt-get install -y --no-install-recommends \
# grab gosu for easy step-down from root
        gosu \
        ; \
	rm -rf /var/lib/apt/lists/*; \
# verify that the "gosu" binary works
	gosu nobody true

ENV RABBITMQ_DATA_DIR=/var/lib/rabbitmq
# Create rabbitmq system user & group, fix permissions & allow root user to connect to the RabbitMQ Erlang VM
RUN set -eux; \
	groupadd --gid 999 --system rabbitmq; \
	useradd --uid 999 --system --home-dir "$RABBITMQ_DATA_DIR" --gid rabbitmq rabbitmq; \
	mkdir -p "$RABBITMQ_DATA_DIR" /etc/rabbitmq /etc/rabbitmq/conf.d /tmp/rabbitmq-ssl /var/log/rabbitmq; \
	chown -fR rabbitmq:rabbitmq "$RABBITMQ_DATA_DIR" /etc/rabbitmq /etc/rabbitmq/conf.d /tmp/rabbitmq-ssl /var/log/rabbitmq; \
	chmod 777 "$RABBITMQ_DATA_DIR" /etc/rabbitmq /etc/rabbitmq/conf.d /tmp/rabbitmq-ssl /var/log/rabbitmq; \
	ln -sf "$RABBITMQ_DATA_DIR/.erlang.cookie" /root/.erlang.cookie

# https://www.rabbitmq.com/signatures.html#importing-gpg
# ENV RABBITMQ_PGP_KEY_ID="0x0A9AF2115F4687BD29803A206B73A36E6026DFCA"
ENV RABBITMQ_HOME=/opt/rabbitmq

# Add RabbitMQ to PATH, send all logs to TTY
ENV PATH=$RABBITMQ_HOME/sbin:$PATH \
	RABBITMQ_LOGS=-

RUN set -eux; \
	\
	savedAptMark="$(apt-mark showmanual)"; \
	apt-get update; \
	apt-get install --yes --no-install-recommends \
		ca-certificates \
		gnupg \
		wget \
		xz-utils \
	; \
	rm -rf /var/lib/apt/lists/*; \
	\
	apt-mark auto '.*' > /dev/null; \
	apt-mark manual $savedAptMark; \
	apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false

# Install RabbitMQ
ARG RABBITMQ_BUILD
COPY ${RABBITMQ_BUILD} $RABBITMQ_HOME

RUN set -eux; \
# Do not default SYS_PREFIX to RABBITMQ_HOME, leave it empty
	grep -qE '^SYS_PREFIX=\$\{RABBITMQ_HOME\}$' "$RABBITMQ_HOME/sbin/rabbitmq-defaults"; \
	sed -i 's/^SYS_PREFIX=.*$/SYS_PREFIX=/' "$RABBITMQ_HOME/sbin/rabbitmq-defaults"; \
	grep -qE '^SYS_PREFIX=$' "$RABBITMQ_HOME/sbin/rabbitmq-defaults"; \
	chown -R rabbitmq:rabbitmq "$RABBITMQ_HOME"; \
	\
# verify assumption of no stale cookies
	[ ! -e "$RABBITMQ_DATA_DIR/.erlang.cookie" ]; \
# Ensure RabbitMQ was installed correctly by running a few commands that do not depend on a running server, as the rabbitmq user
# If they all succeed, it's safe to assume that things have been set up correctly
	gosu rabbitmq rabbitmqctl help; \
	gosu rabbitmq rabbitmqctl list_ciphers; \
	gosu rabbitmq rabbitmq-plugins list; \
# no stale cookies
	rm "$RABBITMQ_DATA_DIR/.erlang.cookie"

# Added for backwards compatibility - users can simply COPY custom plugins to /plugins
RUN ln -sf /opt/rabbitmq/plugins /plugins

# set home so that any `--user` knows where to put the erlang cookie
ENV HOME $RABBITMQ_DATA_DIR
# Hint that the data (a.k.a. home dir) dir should be separate volume
VOLUME $RABBITMQ_DATA_DIR

# warning: the VM is running with native name encoding of latin1 which may cause Elixir to malfunction as it expects utf8. Please ensure your locale is set to UTF-8 (which can be verified by running "locale" in your shell)
# Setting all environment variables that control language preferences, behaviour differs - https://www.gnu.org/software/gettext/manual/html_node/The-LANGUAGE-variable.html#The-LANGUAGE-variable
# https://docs.docker.com/samples/library/ubuntu/#locales
ENV LANG=C.UTF-8 LANGUAGE=C.UTF-8 LC_ALL=C.UTF-8

COPY --chown=rabbitmq:rabbitmq 10-default-guest-user.conf /etc/rabbitmq/conf.d/
COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]

# EPMD AMQP-TLS AMQP ERLANG
EXPOSE 4369 5671 5672 25672
CMD ["rabbitmq-server"]

# rabbitmq_management
RUN rabbitmq-plugins enable --offline rabbitmq_management && \
    rabbitmq-plugins is_enabled rabbitmq_management --offline
# extract "rabbitmqadmin" from inside the "rabbitmq_management-X.Y.Z.ez" plugin zipfile
# see https://github.com/docker-library/rabbitmq/issues/207
# RabbitMQ 3.9 onwards uses uncompressed plugins by default, in which case extraction is
# unnecesary
RUN set -eux; \
	if [ -s /plugins/rabbitmq_management-*.ez ]; then \
		erl -noinput -eval ' \
			{ ok, AdminBin } = zip:foldl(fun(FileInArchive, GetInfo, GetBin, Acc) -> \
				case Acc of \
					"" -> \
						case lists:suffix("/rabbitmqadmin", FileInArchive) of \
							true -> GetBin(); \
							false -> Acc \
						end; \
					_ -> Acc \
				end \
			end, "", init:get_plain_arguments()), \
			io:format("~s", [ AdminBin ]), \
			init:stop(). \
		' -- /plugins/rabbitmq_management-*.ez > /usr/local/bin/rabbitmqadmin; \
	else \
		cp /plugins/rabbitmq_management-*/priv/www/cli/rabbitmqadmin /usr/local/bin/rabbitmqadmin; \
	fi; \
	[ -s /usr/local/bin/rabbitmqadmin ]; \
	chmod +x /usr/local/bin/rabbitmqadmin; \
	apt-get update; apt-get install -y --no-install-recommends python3 dstat sysstat htop nmon tmux neovim; rm -rf /var/lib/apt/lists/*; \
	rabbitmqadmin --version
# MANAGEMENT-TLS MANAGEMENT
EXPOSE 15671 15672

RUN rabbitmq-plugins enable --offline rabbitmq_prometheus && \
    rabbitmq-plugins is_enabled rabbitmq_prometheus --offline
# PROMETHEUS-TLS PROMETHEUS
EXPOSE 15691 15692

RUN rabbitmq-plugins enable --all
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
