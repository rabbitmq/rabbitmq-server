#!/usr/bin/env bash

set -euxo pipefail

mv /opt/rabbitmq_server-* $RABBITMQ_HOME

groupadd --gid 999 --system rabbitmq
useradd --uid 999 --system --home-dir "$RABBITMQ_DATA_DIR" --gid rabbitmq rabbitmq
mkdir -p "$RABBITMQ_DATA_DIR" /etc/rabbitmq /etc/rabbitmq/conf.d /tmp/rabbitmq-ssl /var/log/rabbitmq
chown -fR rabbitmq:rabbitmq "$RABBITMQ_DATA_DIR" /etc/rabbitmq /etc/rabbitmq/conf.d /tmp/rabbitmq-ssl /var/log/rabbitmq
chmod 777 "$RABBITMQ_DATA_DIR" /etc/rabbitmq /etc/rabbitmq/conf.d /tmp/rabbitmq-ssl /var/log/rabbitmq
ln -sf "$RABBITMQ_DATA_DIR/.erlang.cookie" /root/.erlang.cookie

export PATH="$RABBITMQ_HOME/sbin:$PATH"

# Do not default SYS_PREFIX to RABBITMQ_HOME, leave it empty
grep -qE '^SYS_PREFIX=\$\{RABBITMQ_HOME\}$' "$RABBITMQ_HOME/sbin/rabbitmq-defaults"
sed -i 's/^SYS_PREFIX=.*$/SYS_PREFIX=/' "$RABBITMQ_HOME/sbin/rabbitmq-defaults"
grep -qE '^SYS_PREFIX=$' "$RABBITMQ_HOME/sbin/rabbitmq-defaults"
chown -R rabbitmq:rabbitmq "$RABBITMQ_HOME"

# verify assumption of no stale cookies
[ ! -e "$RABBITMQ_DATA_DIR/.erlang.cookie" ]
# Ensure RabbitMQ was installed correctly by running a few commands that do not depend on a running server, as the rabbitmq user
# If they all succeed, it's safe to assume that things have been set up correctly
gosu rabbitmq rabbitmqctl help
gosu rabbitmq rabbitmqctl list_ciphers
gosu rabbitmq rabbitmq-plugins list
# no stale cookies
rm "$RABBITMQ_DATA_DIR/.erlang.cookie"

# Added for backwards compatibility - users can simply COPY custom plugins to /plugins
ln -sf /opt/rabbitmq/plugins /plugins

# move default config and docker entrypoint into place
mv /opt/10-default-guest-user.conf /etc/rabbitmq/conf.d/
chown rabbitmq:rabbitmq /etc/rabbitmq/conf.d/10-default-guest-user.conf
mv /opt/docker-entrypoint.sh /usr/local/bin

# rabbitmq_management
rabbitmq-plugins enable --offline rabbitmq_management && \
    rabbitmq-plugins is_enabled rabbitmq_management --offline
# extract "rabbitmqadmin" from inside the "rabbitmq_management-X.Y.Z.ez" plugin zipfile
# see https://github.com/docker-library/rabbitmq/issues/207
# RabbitMQ 3.9 onwards uses uncompressed plugins by default, in which case extraction is
# unnecesary
cp /plugins/rabbitmq_management-*/priv/www/cli/rabbitmqadmin /usr/local/bin/rabbitmqadmin
[ -s /usr/local/bin/rabbitmqadmin ]
chmod +x /usr/local/bin/rabbitmqadmin
rabbitmqadmin --version

# rabbitmq_prometheus
rabbitmq-plugins enable --offline rabbitmq_prometheus && \
    rabbitmq-plugins is_enabled rabbitmq_prometheus --offline
