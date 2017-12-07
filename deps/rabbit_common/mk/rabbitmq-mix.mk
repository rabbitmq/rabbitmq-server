# This file is copied to rabbitmq_cli (and possibly other Elixir-based
# components) when the RabbitMQ source archive is created, to allow
# those Elixir applications to build even with no access to Hex.pm,
# using the bundled sources only.

HEX_OFFLINE=1

HOME=$(DEPS_DIR)

export HEX_OFFLINE HOME
