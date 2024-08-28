#
# Based on the generated file from https://github.com/docker-library/rabbitmq
#

# The official Canonical Ubuntu Focal image is ideal from a security perspective,
# especially for the enterprises that we, the RabbitMQ team, have to deal with

FROM ubuntu:22.04 as build-base

ARG BUILDKIT_SBOM_SCAN_STAGE=true

RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
		build-essential \
		ca-certificates \
		gnupg \
		libncurses5-dev \
		wget

FROM build-base as openssl-builder

ARG BUILDKIT_SBOM_SCAN_STAGE=true

# Default to a PGP keyserver that pgp-happy-eyeballs recognizes, but allow for substitutions locally
ARG PGP_KEYSERVER=keyserver.ubuntu.com
# If you are building this image locally and are getting `gpg: keyserver receive failed: No data` errors,
# run the build with a different PGP_KEYSERVER, e.g. docker build --tag rabbitmq:4.0 --build-arg PGP_KEYSERVER=pgpkeys.eu 4.0/ubuntu
# For context, see https://github.com/docker-library/official-images/issues/4252

ENV OPENSSL_VERSION 3.3.1
ENV OPENSSL_SOURCE_SHA256="777cd596284c883375a2a7a11bf5d2786fc5413255efab20c50d6ffe6d020b7e"
# https://www.openssl.org/community/otc.html
# https://www.openssl.org/source/
ENV OPENSSL_PGP_KEY_IDS="0x8657ABB260F056B1E5190839D9C4D26D0E604491 0xB7C1C14360F353A36862E4D5231C84CDDCC69C45 0xC1F33DD8CE1D4CC613AF14DA9195C48241FBF7DD 0x95A9908DDFA16830BE9FB9003D30A3A9FF1360DC 0x7953AC1FBC3DC8B3B292393ED5E9E43F7DF9EE8C 0xA21FAB74B0088AA361152586B8EF1A6BA9DA2D5C 0xE5E52560DD91C556DDBDA5D02064C53641C25E5D 0xEFC0A467D613CB83C7ED6D30D894E2CE8B3D79F5"

ENV OTP_VERSION 26.2.5
# TODO add PGP checking when the feature will be added to Erlang/OTP's build system
# https://erlang.org/pipermail/erlang-questions/2019-January/097067.html
ENV OTP_SOURCE_SHA256="de155c4ad9baab2b9e6c96dbd03bf955575a04dd6feee9c08758beb28484c9f6"

# install openssl & erlang to a path that isn't auto-checked for libs to prevent accidental use by system packages
ENV ERLANG_INSTALL_PATH_PREFIX /opt/erlang
ENV OPENSSL_INSTALL_PATH_PREFIX /opt/openssl

# Install dependencies required to build Erlang/OTP from source
# https://erlang.org/doc/installation_guide/INSTALL.html
# dpkg-dev: Required to set up host & build type when compiling Erlang/OTP
# gnupg: Required to verify OpenSSL artefacts
# libncurses5-dev: Required for Erlang/OTP new shell & observer_cli - https://github.com/zhongwencool/observer_cli
RUN set -eux; \
	OPENSSL_SOURCE_URL="https://www.openssl.org/source/openssl-$OPENSSL_VERSION.tar.gz"; \
	OPENSSL_PATH="/usr/local/src/openssl-$OPENSSL_VERSION"; \
	OPENSSL_CONFIG_DIR="$OPENSSL_INSTALL_PATH_PREFIX/etc/ssl"; \
	\
# Required by the crypto & ssl Erlang/OTP applications
	wget --progress dot:giga --output-document "$OPENSSL_PATH.tar.gz.asc" "$OPENSSL_SOURCE_URL.asc"; \
	wget --progress dot:giga --output-document "$OPENSSL_PATH.tar.gz" "$OPENSSL_SOURCE_URL"; \
	export GNUPGHOME="$(mktemp -d)"; \
	for key in $OPENSSL_PGP_KEY_IDS; do \
		gpg --batch --keyserver "$PGP_KEYSERVER" --recv-keys "$key"; \
	done; \
	gpg --batch --verify "$OPENSSL_PATH.tar.gz.asc" "$OPENSSL_PATH.tar.gz"; \
	gpgconf --kill all; \
	rm -rf "$GNUPGHOME"; \
	echo "$OPENSSL_SOURCE_SHA256 *$OPENSSL_PATH.tar.gz" | sha256sum --check --strict -; \
	mkdir -p "$OPENSSL_PATH"; \
	tar --extract --file "$OPENSSL_PATH.tar.gz" --directory "$OPENSSL_PATH" --strip-components 1; \
	\
# Configure OpenSSL for compilation
	cd "$OPENSSL_PATH"; \
# without specifying "--libdir", Erlang will fail during "crypto:supports()" looking for a "pthread_atfork" function that doesn't exist (but only on arm32v7/armhf??)
# OpenSSL's "config" script uses a lot of "uname"-based target detection...
	dpkgArch="$(dpkg --print-architecture)"; dpkgArch="${dpkgArch##*-}"; \
# https://deb.debian.org/debian/dists/unstable/main/
	case "$dpkgArch" in \
# https://github.com/openssl/openssl/blob/openssl-3.1.1/Configurations/10-main.conf#L860 (look for "linux-" and "linux64-" keys)
		amd64) opensslMachine='linux-x86_64' ;; \
		arm64) opensslMachine='linux-aarch64' ;; \
# https://github.com/openssl/openssl/blob/openssl-3.1.1/Configurations/10-main.conf#L736-L766
# https://wiki.debian.org/ArchitectureSpecificsMemo#Architecture_baselines
# https://gcc.gnu.org/onlinedocs/gcc/ARM-Options.html
		armhf) opensslMachine='linux-armv4'; opensslExtraConfig='-march=armv7-a+fp' ;; \
		i386) opensslMachine='linux-x86' ;; \
		ppc64el) opensslMachine='linux-ppc64le' ;; \
		riscv64) opensslMachine='linux64-riscv64' ;; \
		s390x) opensslMachine='linux64-s390x' ;; \
		*) echo >&2 "error: unsupported arch: '$apkArch'"; exit 1 ;; \
	esac; \
	MACHINE="$opensslMachine" \
	RELEASE="4.x.y-z" \
	SYSTEM='Linux' \
	BUILD='???' \
	./Configure \
		"$opensslMachine" \
		enable-fips \
		--prefix="$OPENSSL_INSTALL_PATH_PREFIX" \
		--openssldir="$OPENSSL_CONFIG_DIR" \
		--libdir="$OPENSSL_INSTALL_PATH_PREFIX/lib" \
# add -rpath to avoid conflicts between our OpenSSL's "libssl.so" and the libssl package by making sure "$INSTALL_PATH_PREFIX/lib" is searched first (but only for Erlang/OpenSSL to avoid issues with other tools using libssl; https://github.com/docker-library/rabbitmq/issues/364)
		-Wl,-rpath="$OPENSSL_INSTALL_PATH_PREFIX/lib" \
		${opensslExtraConfig:-} \
	; \
# Compile, install OpenSSL, verify that the command-line works & development headers are present
	make -j "$(getconf _NPROCESSORS_ONLN)"; \
	make install_sw install_ssldirs install_fips; \
	ldconfig; \
# use Debian's CA certificates
	rmdir "$OPENSSL_CONFIG_DIR/certs" "$OPENSSL_CONFIG_DIR/private"; \
	ln -sf /etc/ssl/certs /etc/ssl/private "$OPENSSL_CONFIG_DIR"

# smoke test
RUN $OPENSSL_INSTALL_PATH_PREFIX/bin/openssl version

FROM openssl-builder as erlang-builder

ARG BUILDKIT_SBOM_SCAN_STAGE=true

RUN set -eux; \
	OTP_SOURCE_URL="https://github.com/erlang/otp/releases/download/OTP-$OTP_VERSION/otp_src_$OTP_VERSION.tar.gz"; \
	OTP_PATH="/usr/local/src/otp-$OTP_VERSION"; \
	\
# Download, verify & extract OTP_SOURCE
	mkdir -p "$OTP_PATH"; \
	wget --progress dot:giga --output-document "$OTP_PATH.tar.gz" "$OTP_SOURCE_URL"; \
	echo "$OTP_SOURCE_SHA256 *$OTP_PATH.tar.gz" | sha256sum --check --strict -; \
	tar --extract --file "$OTP_PATH.tar.gz" --directory "$OTP_PATH" --strip-components 1; \
	\
# Configure Erlang/OTP for compilation, disable unused features & applications
# https://erlang.org/doc/applications.html
# ERL_TOP is required for Erlang/OTP makefiles to find the absolute path for the installation
	cd "$OTP_PATH"; \
	export ERL_TOP="$OTP_PATH"; \
	CFLAGS="$(dpkg-buildflags --get CFLAGS)"; export CFLAGS; \
# add -rpath to avoid conflicts between our OpenSSL's "libssl.so" and the libssl package by making sure "$OPENSSL_INSTALL_PATH_PREFIX/lib" is searched first (but only for Erlang/OpenSSL to avoid issues with other tools using libssl; https://github.com/docker-library/rabbitmq/issues/364)
	export CFLAGS="$CFLAGS -Wl,-rpath=$OPENSSL_INSTALL_PATH_PREFIX/lib"; \
	hostArch="$(dpkg-architecture --query DEB_HOST_GNU_TYPE)"; \
	buildArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)"; \
	dpkgArch="$(dpkg --print-architecture)"; dpkgArch="${dpkgArch##*-}"; \
# JIT is only supported on amd64 + arm64; https://github.com/erlang/otp/blob/OTP-25.3.2.2/erts/configure#L24306-L24347
	jitFlag=; \
	case "$dpkgArch" in \
		amd64 | arm64) jitFlag='--enable-jit' ;; \
	esac; \
	./configure \
		--prefix="$ERLANG_INSTALL_PATH_PREFIX" \
		--host="$hostArch" \
		--build="$buildArch" \
		--disable-sctp \
		--disable-silent-rules \
		--enable-builtin-zlib \
		--enable-clock-gettime \
		--enable-hybrid-heap \
		--enable-kernel-poll \
		--enable-smp-support \
		--enable-threads \
		--with-microstate-accounting=extra \
		--with-ssl="$OPENSSL_INSTALL_PATH_PREFIX" \
		--without-common_test \
		--without-debugger \
		--without-dialyzer \
		--without-diameter \
		--without-edoc \
		--without-erl_docgen \
		--without-et \
		--without-eunit \
		--without-ftp \
		--without-jinterface \
		--without-megaco \
		--without-observer \
		--without-odbc \
		--without-reltool \
		--without-ssh \
		--without-tftp \
		--without-wx \
		$jitFlag \
	; \
	\
# Compile & install Erlang/OTP
	make -j "$(getconf _NPROCESSORS_ONLN)" GEN_OPT_FLGS="-O2 -fno-strict-aliasing"; \
	make install; \
	\
# Remove unnecessary files
	find "$ERLANG_INSTALL_PATH_PREFIX/lib/erlang" -type d -name examples -exec rm -rf '{}' +; \
	find "$ERLANG_INSTALL_PATH_PREFIX/lib/erlang" -type d -name src -exec rm -rf '{}' +; \
	find "$ERLANG_INSTALL_PATH_PREFIX/lib/erlang" -type d -name include -exec rm -rf '{}' +

# Check that Erlang/OTP crypto & ssl were compiled against OpenSSL correctly
ENV PATH $ERLANG_INSTALL_PATH_PREFIX/bin:$PATH
RUN find $ERLANG_INSTALL_PATH_PREFIX -type f -name 'crypto.so' -exec ldd {} \; | awk '/libcrypto\.so/ { if (!index($3,ENVIRON["OPENSSL_INSTALL_PATH_PREFIX"])) exit 1 }'
RUN erl -noshell -eval 'ok = crypto:start(), ok = io:format("~p~n~n~p~n~n", [crypto:supports(), ssl:versions()]), init:stop().'

FROM ubuntu:22.04

# OPENSSL/ERLANG_INSTALL_PATH_PREFIX are defined in a different stage, so define them again
ENV ERLANG_INSTALL_PATH_PREFIX /opt/erlang
ENV OPENSSL_INSTALL_PATH_PREFIX /opt/openssl
COPY --from=erlang-builder $ERLANG_INSTALL_PATH_PREFIX $ERLANG_INSTALL_PATH_PREFIX
RUN echo '{"spdxVersion":"SPDX-2.3","SPDXID":"SPDXRef-DOCUMENT","name":"erlang-sbom","packages":[{"name":"erlang","versionInfo":"26.2.2","SPDXID":"SPDXRef-Package--erlang","externalRefs":[{"referenceCategory":"PACKAGE-MANAGER","referenceType":"purl","referenceLocator":"pkg:generic/erlang@26.2.2?os_name=ubuntu&os_version=22.04"}],"licenseDeclared":"Apache-2.0"}]}' > $ERLANG_INSTALL_PATH_PREFIX/erlang.spdx.json

COPY --from=openssl-builder $OPENSSL_INSTALL_PATH_PREFIX $OPENSSL_INSTALL_PATH_PREFIX
RUN echo '{"spdxVersion":"SPDX-2.3","SPDXID":"SPDXRef-DOCUMENT","name":"openssl-sbom","packages":[{"name":"openssl","versionInfo":"3.1.5","SPDXID":"SPDXRef-Package--openssl","externalRefs":[{"referenceCategory":"PACKAGE-MANAGER","referenceType":"purl","referenceLocator":"pkg:generic/openssl@3.1.5?os_name=ubuntu&os_version=22.04"}],"licenseDeclared":"Apache-2.0"}]}' > $OPENSSL_INSTALL_PATH_PREFIX/openssl.spdx.json

ENV PATH $ERLANG_INSTALL_PATH_PREFIX/bin:$OPENSSL_INSTALL_PATH_PREFIX/bin:$PATH

ENV RABBITMQ_DATA_DIR /var/lib/rabbitmq

RUN set -eux; \
# Configure OpenSSL to use system certs
	ln -vsf /etc/ssl/certs /etc/ssl/private "$OPENSSL_INSTALL_PATH_PREFIX/etc/ssl"; \
	\
# Check that OpenSSL still works after copying from previous builder
	ldconfig; \
	sed -i.ORIG -e "/\.include.*fips/ s!.*!.include $OPENSSL_INSTALL_PATH_PREFIX/etc/ssl/fipsmodule.cnf!" \
		-e '/# fips =/s/.*/fips = fips_sect/' "$OPENSSL_INSTALL_PATH_PREFIX/etc/ssl/openssl.cnf"; \
	sed -i.ORIG -e '/^activate/s/^/#/' "$OPENSSL_INSTALL_PATH_PREFIX/etc/ssl/fipsmodule.cnf"; \
	[ "$(command -v openssl)" = "$OPENSSL_INSTALL_PATH_PREFIX/bin/openssl" ]; \
	openssl version; \
	openssl version -d; \
	\
# Check that Erlang/OTP crypto & ssl were compiled against OpenSSL correctly
	erl -noshell -eval 'ok = crypto:start(), ok = io:format("~p~n~n~p~n~n", [crypto:supports(), ssl:versions()]), init:stop().'; \
	\
# Create rabbitmq system user & group, fix permissions & allow root user to connect to the RabbitMQ Erlang VM
	groupadd --gid 999 --system rabbitmq; \
	useradd --uid 999 --system --home-dir "$RABBITMQ_DATA_DIR" --gid rabbitmq rabbitmq; \
	mkdir -p "$RABBITMQ_DATA_DIR" /etc/rabbitmq /etc/rabbitmq/conf.d /tmp/rabbitmq-ssl /var/log/rabbitmq; \
	chown -fR rabbitmq:rabbitmq "$RABBITMQ_DATA_DIR" /etc/rabbitmq /etc/rabbitmq/conf.d /tmp/rabbitmq-ssl /var/log/rabbitmq; \
	chmod 1777 "$RABBITMQ_DATA_DIR" /etc/rabbitmq /etc/rabbitmq/conf.d /tmp/rabbitmq-ssl /var/log/rabbitmq; \
	ln -sf "$RABBITMQ_DATA_DIR/.erlang.cookie" /root/.erlang.cookie

# Use the latest stable RabbitMQ release (https://www.rabbitmq.com/download.html)
ARG RABBITMQ_VERSION=4.0.0
ENV RABBITMQ_VERSION=${RABBITMQ_VERSION}
ENV RABBITMQ_HOME /opt/rabbitmq

# Add RabbitMQ to PATH
ENV PATH $RABBITMQ_HOME/sbin:$PATH

COPY package-generic-unix.tar.xz /usr/local/src/rabbitmq-$RABBITMQ_VERSION.tar.xz

# Install RabbitMQ
RUN set -eux; \
	export DEBIAN_FRONTEND=noninteractive; \
	apt-get update; \
	apt-get install --yes --no-install-recommends \
		ca-certificates \
# grab gosu for easy step-down from root
		gosu \
# Bring in tzdata so users could set the timezones through the environment
		tzdata \
	; \
# verify that the "gosu" binary works
	gosu nobody true; \
	\
	savedAptMark="$(apt-mark showmanual)"; \
	apt-get install --yes --no-install-recommends \
		gnupg \
		wget \
		xz-utils \
	; \
	rm -rf /var/lib/apt/lists/*; \
	\
	RABBITMQ_SOURCE_URL="https://github.com/rabbitmq/rabbitmq-server/releases/download/v$RABBITMQ_VERSION/rabbitmq-server-generic-unix-latest-toolchain-$RABBITMQ_VERSION.tar.xz"; \
	RABBITMQ_PATH="/usr/local/src/rabbitmq-$RABBITMQ_VERSION"; \
	\
	mkdir -p "$RABBITMQ_HOME"; \
	tar --extract --file "$RABBITMQ_PATH.tar.xz" --directory "$RABBITMQ_HOME" --strip-components 1; \
	rm -rf "$RABBITMQ_PATH"*; \
# Do not default SYS_PREFIX to RABBITMQ_HOME, leave it empty
	grep -qE '^SYS_PREFIX=\$\{RABBITMQ_HOME\}$' "$RABBITMQ_HOME/sbin/rabbitmq-defaults"; \
	sed -i 's/^SYS_PREFIX=.*$/SYS_PREFIX=/' "$RABBITMQ_HOME/sbin/rabbitmq-defaults"; \
	grep -qE '^SYS_PREFIX=$' "$RABBITMQ_HOME/sbin/rabbitmq-defaults"; \
	chown -R rabbitmq:rabbitmq "$RABBITMQ_HOME"; \
	\
	apt-mark auto '.*' > /dev/null; \
	apt-mark manual $savedAptMark; \
	apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false; \
	\
# verify assumption of no stale cookies
	[ ! -e "$RABBITMQ_DATA_DIR/.erlang.cookie" ]; \
# Ensure RabbitMQ was installed correctly by running a few commands that do not depend on a running server, as the rabbitmq user
# If they all succeed, it's safe to assume that things have been set up correctly
	gosu rabbitmq rabbitmqctl help; \
	gosu rabbitmq rabbitmqctl list_ciphers; \
	gosu rabbitmq rabbitmq-plugins list; \
# no stale cookies
	rm "$RABBITMQ_DATA_DIR/.erlang.cookie"; \
	\
	echo '{"spdxVersion":"SPDX-2.3","SPDXID":"SPDXRef-DOCUMENT","name":"rabbitmq-sbom","packages":[{"name":"rabbitmq","versionInfo":"4.0.0","SPDXID":"SPDXRef-Package--rabbitmq","externalRefs":[{"referenceCategory":"PACKAGE-MANAGER","referenceType":"purl","referenceLocator":"pkg:generic/rabbitmq@4.0.0?os_name=ubuntu&os_version=22.04"}],"licenseDeclared":"MPL-2.0 AND Apache-2.0"}]}' > $RABBITMQ_HOME/rabbitmq.spdx.json

# Enable Prometheus-style metrics by default (https://github.com/docker-library/rabbitmq/issues/419)
RUN gosu rabbitmq rabbitmq-plugins enable --offline rabbitmq_prometheus

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

COPY --chown=rabbitmq:rabbitmq 10-defaults.conf 20-management_agent.disable_metrics_collector.conf /etc/rabbitmq/conf.d/
COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]

EXPOSE 4369 5671 5672 15691 15692 25672
CMD ["rabbitmq-server"]


RUN set eux; \
	rabbitmq-plugins enable --offline rabbitmq_management; \
# make sure the metrics collector is re-enabled (disabled in the base image for Prometheus-style metrics by default)
	rm -f /etc/rabbitmq/conf.d/20-management_agent.disable_metrics_collector.conf; \
# grab "rabbitmqadmin" from inside the "rabbitmq_management-X.Y.Z" plugin folder
# see https://github.com/docker-library/rabbitmq/issues/207
	cp /plugins/rabbitmq_management-*/priv/www/cli/rabbitmqadmin /usr/local/bin/rabbitmqadmin; \
	[ -s /usr/local/bin/rabbitmqadmin ]; \
	chmod +x /usr/local/bin/rabbitmqadmin; \
	apt-get update; \
	apt-get install -y --no-install-recommends python3; \
	rm -rf /var/lib/apt/lists/*; \
	rabbitmqadmin --version

EXPOSE 15671 15672
