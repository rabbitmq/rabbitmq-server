VERSION=0.0.0
SOURCE_DIR=rabbitmq-server-$(VERSION)
TARGET_DIR=rabbitmq_server-$(VERSION)
TARGET_TARBALL=rabbitmq-server-mac-standalone-$(VERSION)
RLS_DIR=$(TARGET_DIR)/release/$(TARGET_DIR)

ERTS_VSN=$(shell erl -noshell -eval 'io:format("~s", [erlang:system_info(version)]), halt().')
ERTS_ROOT_DIR=$(shell erl -noshell -eval 'io:format("~s", [code:root_dir()]), halt().')

# used to generate the erlang release
RABBITMQ_HOME=$(TARGET_DIR)
RABBITMQ_EBIN_ROOT=$(RABBITMQ_HOME)/ebin
RABBITMQ_PLUGINS_DIR=$(RABBITMQ_HOME)/plugins
RABBITMQ_PLUGINS_EXPAND_DIR=$(RABBITMQ_PLUGINS_DIR)/expand

dist:
	tar -zxf ../../dist/$(SOURCE_DIR).tar.gz

	$(MAKE) -C $(SOURCE_DIR) \
		TARGET_DIR=`pwd`/$(TARGET_DIR) \
		SBIN_DIR=`pwd`/$(TARGET_DIR)/sbin \
		MAN_DIR=`pwd`/$(TARGET_DIR)/share/man \
			install

	./fix-rabbitmq-defaults.sh $(TARGET_DIR) $(ERTS_VSN) $(VERSION)

	mkdir -p $(TARGET_DIR)/etc/rabbitmq

	$(MAKE) generate_release

	mkdir -p $(RLS_DIR)
	tar -C $(RLS_DIR) -xzf $(RABBITMQ_HOME)/rabbit.tar.gz

# add minimal boot file
	cp $(ERTS_ROOT_DIR)/bin/start_clean.boot $(RLS_DIR)/releases/$(VERSION)
	cp $(ERTS_ROOT_DIR)/bin/start_sasl.boot $(RLS_DIR)/releases/$(VERSION)

# move rabbitmq files to top level folder
	mv $(RLS_DIR)/lib/rabbit-$(VERSION)/* $(RLS_DIR)

# remove empty lib/rabbit-$(VERSION) folder
	rm -rf $(RLS_DIR)/lib/rabbit-$(VERSION)

	tar -zcf $(TARGET_TARBALL).tar.gz -C $(TARGET_DIR)/release $(TARGET_DIR)
	rm -rf $(SOURCE_DIR) $(TARGET_DIR)

clean: clean_partial
	rm -f rabbitmq-server-mac-standalone-*.tar.gz

clean_partial:
	rm -rf $(SOURCE_DIR)
	rm -rf $(TARGET_DIR)

.PHONY : generate_release
generate_release:
	erlc -I $(TARGET_DIR)/include/ -o src -Wall -v +debug_info -Duse_specs -Duse_proper_qc -pa $(TARGET_DIR)/ebin/ src/rabbit_release.erl
	erl \
	    -pa "$(RABBITMQ_EBIN_ROOT)" \
	    -pa src \
	    -noinput \
	    -hidden \
	    -s rabbit_release \
	    -extra "$(RABBITMQ_PLUGINS_DIR)" "$(RABBITMQ_PLUGINS_EXPAND_DIR)" "$(RABBITMQ_HOME)"
	rm src/rabbit_release.beam
