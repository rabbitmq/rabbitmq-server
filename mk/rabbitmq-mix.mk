# This file is copied to rabbitmq_cli (and possibly other Elixir-based
# components) when the RabbitMQ source archive is created, to allow
# those Elixir applications to build even with no access to Hex.pm,
# using the bundled sources only.

HEX_OFFLINE := 1

# mix(1) centralizes its data in `$MIX_HOME`. When unset, it defaults
# to something under `$XDG_DATA_HOME`/`$XDG_CONFIG_HOME` or `$HOME`
# depending on the Elixir version.
#
# We store those data for offline build in `$(DEPS_DIR)`.

override MIX_HOME := $(DEPS_DIR)/.mix

# In addition to `$MIX_HOME`, we still have to set `$HEX_HOME` which is used to
# find `~/.hex` where the Hex.pm cache and packages are stored.

override HEX_HOME := $(DEPS_DIR)/.hex

export HEX_OFFLINE MIX_HOME HEX_HOME
