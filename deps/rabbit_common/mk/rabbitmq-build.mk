# --------------------------------------------------------------------
# Compiler flags.
# --------------------------------------------------------------------

# NOTE: This plugin is loaded twice because Erlang.mk recurses. That's
# why ERL_LIBS may contain twice the path to Elixir libraries or
# ERLC_OPTS may contain duplicated flags.

TEST_ERLC_OPTS += +nowarn_export_all

ifneq ($(filter rabbitmq_cli,$(BUILD_DEPS) $(DEPS)),)
# Add the CLI ebin directory to the code path for the compiler: plugin
# CLI extensions may access behaviour modules defined in this directory.

RMQ_ERLC_OPTS += -pa $(DEPS_DIR)/rabbitmq_cli/ebin
endif

ifndef NON_DETERMINISTIC
RMQ_ERLC_OPTS += +deterministic
endif

# Push our compilation options to both the normal and test ERLC_OPTS.
ERLC_OPTS += $(RMQ_ERLC_OPTS)
TEST_ERLC_OPTS += $(RMQ_ERLC_OPTS)
