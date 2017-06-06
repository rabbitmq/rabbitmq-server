# --------------------------------------------------------------------
# Compiler flags.
# --------------------------------------------------------------------

# NOTE: This plugin is loaded twice because Erlang.mk recurses. That's
# why ERL_LIBS may contain twice the path to Elixir libraries or
# ERLC_OPTS may contain duplicated flags.

# Add Elixir libraries to ERL_LIBS for testsuites.
#
# We replace the leading drive letter ("C:/") with an MSYS2-like path
# ("/C/") for Windows. Otherwise, ERL_LIBS mixes `:` as a PATH separator
# and a drive letter marker. This causes the Erlang VM to crash with
# "Bad address".
#
# The space before `~r//` is apparently required. Otherwise, Elixir
# complains with "unexpected token "~"".

ELIXIR_LIB_DIR := $(shell elixir -e 'IO.puts(Regex.replace( ~r/^([a-zA-Z]):/, to_string(:code.lib_dir(:elixir)), "/\\1"))')
ifeq ($(ERL_LIBS),)
ERL_LIBS := $(ELIXIR_LIB_DIR)
else
ERL_LIBS := $(ERL_LIBS):$(ELIXIR_LIB_DIR)
endif

TEST_ERLC_OPTS += +nowarn_export_all

define compare_version
$(shell awk 'BEGIN {
	split("$(1)", v1, ".");
	version1 = v1[1] * 1000000 + v1[2] * 10000 + v1[3] * 100 + v1[4];

	split("$(2)", v2, ".");
	version2 = v2[1] * 1000000 + v2[2] * 10000 + v2[3] * 100 + v2[4];

	if (version1 $(3) version2) {
		print "true";
	} else {
		print "false";
	}
}')
endef

# Add the CLI ebin directory to the code path for the compiler: plugin
# CLI extensions may access behaviour modules defined in this directory.
RMQ_ERLC_OPTS += -pa $(DEPS_DIR)/rabbitmq_cli/_build/dev/lib/rabbitmqctl/ebin

# Add Lager parse_transform module and our default Lager extra sinks.
LAGER_EXTRA_SINKS += rabbit_log \
		     rabbit_log_channel \
		     rabbit_log_connection \
		     rabbit_log_mirroring \
		     rabbit_log_queue \
		     rabbit_log_federation \
		     rabbit_log_upgrade
lager_extra_sinks = $(subst $(space),$(comma),$(LAGER_EXTRA_SINKS))

RMQ_ERLC_OPTS += +'{parse_transform,lager_transform}' \
		 +'{lager_extra_sinks,[$(lager_extra_sinks)]}'

# Push our compilation options to both the normal and test ERLC_OPTS.
ERLC_OPTS += $(RMQ_ERLC_OPTS)
TEST_ERLC_OPTS += $(RMQ_ERLC_OPTS)
