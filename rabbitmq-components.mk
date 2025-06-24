ifeq ($(.DEFAULT_GOAL),)
# Define default goal to `all` because this file defines some targets
# before the inclusion of erlang.mk leading to the wrong target becoming
# the default.
.DEFAULT_GOAL = all
endif

# PROJECT_VERSION defaults to:
#   1. the version exported by environment;
#   2. the version stored in `git-revisions.txt`, if it exists;
#   3. a version based on git-describe(1), if it is a Git clone;
#   4. 0.0.0
#
# Note that in the case where git-describe(1) is used
# (e.g. during development), running "git gc" may help
# improve the performance.

PROJECT_VERSION := $(RABBITMQ_VERSION)

ifeq ($(PROJECT_VERSION),)
ifneq ($(wildcard git-revisions.txt),)
PROJECT_VERSION = $(shell \
	head -n1 git-revisions.txt | \
	awk '{print $$$(words $(PROJECT_DESCRIPTION) version);}')
else
PROJECT_VERSION = $(shell \
	(git describe --dirty --abbrev=7 --tags --always --first-parent \
		2>/dev/null || echo 0.0.0) | \
		sed -e 's/^v//' -e 's/_/./g' -e 's/-/+/' -e 's/-/./g')
endif
endif

# --------------------------------------------------------------------
# RabbitMQ components.
# --------------------------------------------------------------------

# Third-party dependencies version pinning.
#
# We do that in this file, which is included by all projects, to ensure
# all projects use the same versions. It avoids conflicts.

dep_accept = hex 0.3.5
dep_cowboy = hex 2.13.0
dep_cowlib = hex 2.14.0
dep_credentials_obfuscation = hex 3.5.0
dep_cuttlefish = hex 3.4.0
dep_gen_batch_server = hex 0.8.8
dep_jose = hex 1.11.10
dep_khepri = hex 0.17.1
dep_khepri_mnesia_migration = hex 0.8.0
dep_meck = hex 1.0.0
dep_osiris = git https://github.com/rabbitmq/osiris v1.8.8
dep_prometheus = hex 4.11.0
dep_ra = hex 2.16.11
dep_ranch = hex 2.2.0
dep_recon = hex 2.5.6
dep_redbug = hex 2.1.0
dep_systemd = hex 0.6.1
dep_thoas = hex 1.2.1
dep_observer_cli = hex 1.8.2
dep_seshat = git https://github.com/rabbitmq/seshat v0.6.1
dep_stdout_formatter = hex 0.2.4
dep_sysmon_handler = hex 1.3.0

# RabbitMQ applications found in the monorepo.
#
# Note that rabbitmq_server_release is not a real application
# but is the name used in the top-level Makefile.

RABBITMQ_BUILTIN = \
	amqp10_client \
	amqp10_common \
	amqp_client \
	oauth2_client \
	rabbit \
	rabbit_common \
	rabbitmq_amqp1_0 \
	rabbitmq_amqp_client \
	rabbitmq_auth_backend_cache \
	rabbitmq_auth_backend_http \
	rabbitmq_auth_backend_internal_loopback \
	rabbitmq_auth_backend_ldap \
	rabbitmq_auth_backend_oauth2 \
	rabbitmq_auth_mechanism_ssl \
	rabbitmq_aws \
	rabbitmq_cli \
	rabbitmq_codegen \
	rabbitmq_consistent_hash_exchange \
	rabbitmq_ct_client_helpers \
	rabbitmq_ct_helpers \
	rabbitmq_event_exchange \
	rabbitmq_federation \
	rabbitmq_federation_management \
	rabbitmq_federation_prometheus \
	rabbitmq_jms_topic_exchange \
	rabbitmq_management \
	rabbitmq_management_agent \
	rabbitmq_mqtt \
	rabbitmq_peer_discovery_aws \
	rabbitmq_peer_discovery_common \
	rabbitmq_peer_discovery_consul \
	rabbitmq_peer_discovery_etcd \
	rabbitmq_peer_discovery_k8s \
	rabbitmq_prelaunch \
	rabbitmq_prometheus \
	rabbitmq_random_exchange \
	rabbitmq_recent_history_exchange \
    rabbitmq_server_release \
	rabbitmq_sharding \
	rabbitmq_shovel \
	rabbitmq_shovel_management \
	rabbitmq_stomp \
	rabbitmq_stream \
	rabbitmq_stream_common \
	rabbitmq_stream_management \
	rabbitmq_top \
	rabbitmq_tracing \
	rabbitmq_trust_store \
	rabbitmq_web_dispatch \
	rabbitmq_web_mqtt \
	rabbitmq_web_mqtt_examples \
	rabbitmq_web_stomp \
	rabbitmq_web_stomp_examples \
	trust_store_http

# Applications outside of the monorepo maintained by Team RabbitMQ.

RABBITMQ_COMMUNITY = \
	rabbitmq_auth_backend_amqp \
	rabbitmq_boot_steps_visualiser \
	rabbitmq_delayed_message_exchange \
	rabbitmq_lvc_exchange \
	rabbitmq_management_exchange \
	rabbitmq_management_themes \
	rabbitmq_message_timestamp \
	rabbitmq_metronome \
	rabbitmq_routing_node_stamp \
	rabbitmq_rtopic_exchange

community_dep = git git@github.com:rabbitmq/$1.git $(if $2,$2,main)
dep_rabbitmq_auth_backend_amqp = $(call community_dep,rabbitmq-auth-backend-amqp)
dep_rabbitmq_boot_steps_visualiser = $(call community_dep,rabbitmq-boot-steps-visualiser,master)
dep_rabbitmq_delayed_message_exchange = $(call community_dep,rabbitmq-delayed-message-exchange)
dep_rabbitmq_lvc_exchange = $(call community_dep,rabbitmq-lvc-exchange)
dep_rabbitmq_management_exchange = $(call community_dep,rabbitmq-management-exchange)
dep_rabbitmq_management_themes = $(call community_dep,rabbitmq-management-themes,master)
dep_rabbitmq_message_timestamp = $(call community_dep,rabbitmq-message-timestamp)
dep_rabbitmq_metronome = $(call community_dep,rabbitmq-metronome,master)
dep_rabbitmq_routing_node_stamp = $(call community_dep,rabbitmq-routing-node-stamp)
dep_rabbitmq_rtopic_exchange = $(call community_dep,rabbitmq-rtopic-exchange)

# All RabbitMQ applications.

RABBITMQ_COMPONENTS = $(RABBITMQ_BUILTIN) $(RABBITMQ_COMMUNITY)

# Erlang.mk does not rebuild dependencies by default, once they were
# compiled once, except for those listed in the `$(FORCE_REBUILD)`
# variable.
#
# We want all RabbitMQ components to always be rebuilt: this eases
# the work on several components at the same time.

FORCE_REBUILD = $(RABBITMQ_COMPONENTS)

# We disable autopatching for community plugins as they sit in
# their own repository and we want to avoid polluting the git
# status with changes that should not be committed.
NO_AUTOPATCH += $(RABBITMQ_COMMUNITY)

# --------------------------------------------------------------------
# Component distribution.
# --------------------------------------------------------------------

list-dist-deps::
	@:

prepare-dist::
	@:

# --------------------------------------------------------------------
# RabbitMQ-specific settings.
# --------------------------------------------------------------------

# If the top-level project is a RabbitMQ component, we override
# $(DEPS_DIR) for this project to point to the top-level's one.
#
# We do the same for $(ERLANG_MK_TMP) as we want to keep the
# beam cache regardless of where we build. We also want to
# share Hex tarballs.

ifneq ($(PROJECT),rabbitmq_server_release)
DEPS_DIR ?= $(abspath ..)
ERLANG_MK_TMP ?= $(abspath ../../.erlang.mk)
DISABLE_DISTCLEAN = 1
endif

# We disable `make distclean` so $(DEPS_DIR) is not accidentally removed.

ifeq ($(DISABLE_DISTCLEAN),1)
ifneq ($(filter distclean distclean-deps,$(MAKECMDGOALS)),)
SKIP_DEPS = 1
endif
endif
