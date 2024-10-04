# --------------------------------------------------------------------
# Hex.pm.
#
# This Erlang.mk plugin should only be included by
# applications that produce an Hex.pm release.
# --------------------------------------------------------------------

.PHONY: hex-publish hex-publish-docs

RABBIT_COMMON_HEXPM_VERSION = $(PROJECT_VERSION)
AMQP10_COMMON_HEXPM_VERSION = $(PROJECT_VERSION)
AMQP10_CLIENT_HEXPM_VERSION = $(PROJECT_VERSION)
AMQP_CLIENT_HEXPM_VERSION = $(PROJECT_VERSION)

rebar.config: dep_rabbit_common = hex $(RABBIT_COMMON_HEXPM_VERSION)
rebar.config: dep_amqp10_common = hex $(AMQP10_COMMON_HEXPM_VERSION)
rebar.config: dep_amqp10_client = hex $(AMQP10_CLIENT_HEXPM_VERSION)
rebar.config: dep_amqp_client = hex $(AMQP_CLIENT_HEXPM_VERSION)

define RABBITMQ_HEXPM_DEFAULT_FILES
	    "erlang.mk",
	    "git-revisions.txt",
	    "include",
	    "LICENSE*",
	    "Makefile",
	    "rabbitmq-components.mk",
	    "README",
	    "README.md",
	    "src"
endef

RMQ_COMPONENTS_PLAIN = $(DEPS_DIR)/../rabbitmq-components.mk
RMQ_COMPONENTS_HEXPM = $(DEPS_DIR)/rabbit_common/mk/rabbitmq-components.hexpm.mk

hex-publish: app rebar.config
	$(gen_verbose) echo "$(PROJECT_DESCRIPTION) $(PROJECT_VERSION)" \
		> git-revisions.txt
	$(verbose) mv \
		$(RMQ_COMPONENTS_PLAIN) \
		rabbitmq-components.mk.not-hexpm
	$(verbose) cp \
		$(RMQ_COMPONENTS_HEXPM) \
		$(RMQ_COMPONENTS_PLAIN)
	$(verbose) grep -E '^dep.* = hex' \
		rabbitmq-components.mk.not-hexpm \
		>> $(RMQ_COMPONENTS_PLAIN)
	$(verbose) touch -r \
		rabbitmq-components.mk.not-hexpm \
		$(RMQ_COMPONENTS_PLAIN)
	$(verbose) trap '\
		rm -f git-revisions.txt rebar.lock; \
		if test -f rabbitmq-components.mk.not-hexpm; then \
		  mv \
		    rabbitmq-components.mk.not-hexpm \
		    $(RMQ_COMPONENTS_PLAIN); \
		fi' EXIT INT; \
		$(MAKE) hex-release-publish

hex-publish-docs: app docs
	$(gen_verbose) trap 'rm -f rebar.lock' EXIT INT; \
		$(MAKE) hex-docs-publish
