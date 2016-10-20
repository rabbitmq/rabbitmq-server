
UPGRADE_FROM = 3.6.5

# UPGRADE_FROM_SCRIPT specifies a script name to use.
# it should not be changed for every UPGRADE_FROM version
UPGRADE_FROM_SCRIPT = 3.5
UPGRADE_TO_SCRIPT = 3.6

RELEASE_FOR_UPGRADE = $(CURDIR)/rabbitmq_r_$(UPGRADE_FROM)
RELEASE_ARCHIVE = rabbitmq-server-generic-unix-$(UPGRADE_FROM).tar.xz
RELEASE_FOR_UPGRADE_URL = "http://www.rabbitmq.com/releases/rabbitmq-server/v$(UPGRADE_FROM)/$(RELEASE_ARCHIVE)"
RELEASE_UPGRADE_DATA = $(CURDIR)/rabbitmq_server_upgrade_data/rabbitmq-test-instances
UPGRADE_CONFIG_DIR = $(CURDIR)/config
UPGRADE_SCRIPTS_DIR = $(CURDIR)/scripts

QUEUES_COUNT_TRANSIENT ?= 1
QUEUES_COUNT_DURABLE ?= 1
MSGS_COUNT_NON_PERSISTENT ?= 10
MSGS_COUNT_PERSISTENT_INDEX ?= 10
MSGS_COUNT_PERSISTENT_STORE ?= 10
INDEX_MSG_SIZE ?= 50
STORE_MSG_SIZE ?= 150

# Clean up erlang.mk exported variables
ifneq (0,${MAKELEVEL})
export ERL_LIBS=
export RABBITMQ_SCRIPTS_DIR=
endif

all: verify-and-cleanup

$(RELEASE_ARCHIVE):
	wget $(RELEASE_FOR_UPGRADE_URL) -O $(RELEASE_ARCHIVE)

$(RELEASE_FOR_UPGRADE): $(RELEASE_ARCHIVE)
	mkdir $(RELEASE_FOR_UPGRADE)
	tar -xvf $(RELEASE_ARCHIVE)
	mv rabbitmq_server-*/* $(RELEASE_FOR_UPGRADE)

run-release-for-upgrade: $(RELEASE_FOR_UPGRADE)
	rm -rf $(RELEASE_UPGRADE_DATA)
	mkdir -p $(RELEASE_UPGRADE_DATA)
	cd $(RELEASE_FOR_UPGRADE) && \
	RABBITMQ_HOME=$(RELEASE_FOR_UPGRADE) \
	RABBITMQ_CONFIG_FILE=$(UPGRADE_CONFIG_DIR)/rabbitmq \
	RABBITMQ_NODENAME=rabbit_upgrade \
	RABBITMQ_ENABLED_PLUGINS_FILE=$(UPGRADE_CONFIG_DIR)/enabled_plugins \
	RABBITMQ_LOG_BASE=$(RELEASE_UPGRADE_DATA)/rabbit_upgrade/log \
	RABBITMQ_MNESIA_BASE=$(RELEASE_UPGRADE_DATA)/rabbit_upgrade/mnesia \
	RABBITMQ_MNESIA_DIR=$(RELEASE_UPGRADE_DATA)/rabbit_upgrade/mnesia/rabbit_upgrade \
	RABBITMQ_SCHEMA_DIR=$(RELEASE_UPGRADE_DATA)/rabbit_upgrade/schema \
	RABBITMQ_PLUGINS_EXPAND_DIR=$(RELEASE_UPGRADE_DATA)/rabbit_upgrade/plugins \
	RABBITMQ_PLUGINS_DIR=$(RELEASE_FOR_UPGRADE)/plugins \
	./sbin/rabbitmq-server -detached
	sleep 10

stop-release-for-upgrade:
	cd $(RELEASE_FOR_UPGRADE) && \
	RABBITMQ_HOME=$(RELEASE_FOR_UPGRADE) \
	./sbin/rabbitmqctl -n rabbit_upgrade stop

setup-release-for-upgrade: run-release-for-upgrade
	RABBITMQCTL="$(RELEASE_FOR_UPGRADE)/sbin/rabbitmqctl -n rabbit_upgrade" \
	UPGRADE_FROM_SCRIPT=$(UPGRADE_FROM_SCRIPT) \
	QUEUES_COUNT_TRANSIENT=$(QUEUES_COUNT_TRANSIENT) \
	QUEUES_COUNT_DURABLE=$(QUEUES_COUNT_DURABLE) \
	MSGS_COUNT_NON_PERSISTENT=$(MSGS_COUNT_NON_PERSISTENT) \
	MSGS_COUNT_PERSISTENT_INDEX=$(MSGS_COUNT_PERSISTENT_INDEX) \
	MSGS_COUNT_PERSISTENT_STORE=$(MSGS_COUNT_PERSISTENT_STORE) \
	INDEX_MSG_SIZE=$(INDEX_MSG_SIZE) \
	STORE_MSG_SIZE=$(STORE_MSG_SIZE) \
	bash $(UPGRADE_SCRIPTS_DIR)/upgrade-from.sh

prepare-release-for-upgrade: setup-release-for-upgrade stop-release-for-upgrade

run-broker-upgrade:
	$(MAKE) -C ../ run-background-broker \
	RABBITMQ_CONFIG_FILE=$(UPGRADE_CONFIG_DIR)/rabbitmq \
	RABBITMQ_NODENAME=rabbit_upgrade \
	TMPDIR=`dirname $(RELEASE_UPGRADE_DATA)` \
	PLUGINS=rabbitmq_management
	sleep 10

verify-upgrade: prepare-release-for-upgrade run-broker-upgrade
	RABBITMQCTL="$(RELEASE_FOR_UPGRADE)/sbin/rabbitmqctl -n rabbit_upgrade" \
	UPGRADE_TO_SCRIPT=$(UPGRADE_TO_SCRIPT) \
	QUEUES_COUNT_TRANSIENT=$(QUEUES_COUNT_TRANSIENT) \
	QUEUES_COUNT_DURABLE=$(QUEUES_COUNT_DURABLE) \
	MSGS_COUNT_NON_PERSISTENT=$(MSGS_COUNT_NON_PERSISTENT) \
	MSGS_COUNT_PERSISTENT_INDEX=$(MSGS_COUNT_PERSISTENT_INDEX) \
	MSGS_COUNT_PERSISTENT_STORE=$(MSGS_COUNT_PERSISTENT_STORE) \
	INDEX_MSG_SIZE=$(INDEX_MSG_SIZE) \
	STORE_MSG_SIZE=$(STORE_MSG_SIZE) \
	$(UPGRADE_SCRIPTS_DIR)/upgrade-to.sh

verify-and-stop: verify-upgrade
	-$(MAKE) stop-release-for-upgrade

verify-and-cleanup: verify-and-stop
	$(MAKE) clean

clean: 
	-$(MAKE) stop-release-for-upgrade
	rm -rf rabbitmq_server*
	rm -rf rabbitmq_r*
	rm -rf rabbitmqadmin

distclean: clean
	rm -rf rabbitmq-server-generic-unix-*