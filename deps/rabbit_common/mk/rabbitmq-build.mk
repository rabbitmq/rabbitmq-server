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

# FIXME: We copy Erlang.mk default flags here: rabbitmq-build.mk is
# loaded as a plugin, so before those variables are defined. And because
# Erlang.mk uses '?=', the flags we set here override the default set.
#
# See: https://github.com/ninenines/erlang.mk/issues/502

WARNING_OPTS += +debug_info \
		+warn_export_vars \
		+warn_shadow_vars \
		+warn_obsolete_guard
ERLC_OPTS += -Werror $(WARNING_OPTS)
TEST_ERLC_OPTS += +nowarn_export_all $(WARNING_OPTS)

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

# --------------------------------------------------------------------
# Common Test flags.
# --------------------------------------------------------------------

# Enable the following common_test hooks on Travis and Concourse:
#
# cth_fail_fast
#   This hook will make sure the first failure puts an end to the
#   testsuites; ie. all remaining tests are skipped.
#
# cth_styledout
#   This hook will change the output of common_test to something more
#   consise and colored.

CTH_DEPS = cth_fail_fast cth_styledout
dep_cth_fail_fast = git https://github.com/rabbitmq/cth_fail_fast.git master
dep_cth_styledout = git https://github.com/rabbitmq/cth_styledout.git master
CTH_OPTS = -ct_hooks cth_fail_fast and cth_styledout

ifdef TRAVIS
TEST_DEPS += $(CTH_DEPS)
CT_OPTS += $(CTH_OPTS)
endif
ifdef CONCOURSE
TEST_DEPS += $(CTH_DEPS)
CT_OPTS += $(CTH_OPTS)
endif

# Disable most messages on Travis because it might exceed the limit
# set by Travis.
#
# On Concourse, we keep the default verbosity. With Erlang 19.2+, the
# cth_styledout hook will change the output to something concise and all
# messages are available in in HTML reports. With Erlang up-to 19.1,
# stdout will be flooded with messages, but we'll live with that.
#
# CAUTION: All arguments after -erl_args are passed to the emulator and
# common_test doesn't interpret them! Therefore, all common_test flags
# *MUST* appear before.

CT_QUIET_FLAGS = -verbosity 50 \
		 -erl_args \
		 -kernel error_logger silent

ifdef TRAVIS
CT_OPTS += $(CT_QUIET_FLAGS)
endif

# Enable JUnit-like report on Jenkins. Jenkins parses those reports so
# the results can be browsed from its UI. Furthermore, it displays a
# graph showing evolution of the results over time.

ifdef JENKINS_HOME
CT_OPTS += -ct_hooks cth_surefire
endif

# On CI, set $RABBITMQ_CT_SKIP_AS_ERROR so that any skipped
# testsuite/testgroup/testcase is considered an error.

ifdef TRAVIS
export RABBITMQ_CT_SKIP_AS_ERROR = true
endif
ifdef CONCOURSE
export RABBITMQ_CT_SKIP_AS_ERROR = true
endif
ifdef JENKINS_HOME
export RABBITMQ_CT_SKIP_AS_ERROR = true
endif
