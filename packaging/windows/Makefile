VERSION=0.0.0
SOURCE_DIR=rabbitmq-server-$(VERSION)
TARGET_DIR=rabbitmq_server-$(VERSION)
TARGET_ZIP=rabbitmq-server-windows-$(VERSION)
PLUGINS_DIST_DIR=

dist:
	tar -zxf ../../dist/$(SOURCE_DIR).tar.gz
	$(MAKE) -C $(SOURCE_DIR)

	mkdir $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmq-server.bat $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmq-service.bat $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmq-plugin.bat $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmqctl.bat $(SOURCE_DIR)/sbin
	rm -rf $(SOURCE_DIR)/scripts
	rm -rf $(SOURCE_DIR)/codegen* $(SOURCE_DIR)/Makefile $(SOURCE_DIR)/*mk
	rm -f $(SOURCE_DIR)/README
	rm -rf $(SOURCE_DIR)/docs
	rm -rf $(SOURCE_DIR)/src
	rm -rf $(SOURCE_DIR)/plugins-src
	rm -rf $(SOURCE_DIR)/dist

	mv $(SOURCE_DIR) $(TARGET_DIR)
	mkdir -p $(TARGET_DIR)
	mkdir -p $(TARGET_DIR)/plugins
	echo Put your .ez plugin files in this directory > $(TARGET_DIR)/plugins/README
	xmlto -o . xhtml-nochunks ../../docs/rabbitmq-service.xml
	elinks -dump -no-references -no-numbering rabbitmq-service.html \
		> $(TARGET_DIR)/readme-service.txt
	todos $(TARGET_DIR)/readme-service.txt
	rm -f $(TARGET_DIR)/plugins-src/rabbitmq-server
	zip -q -r $(TARGET_ZIP).zip $(TARGET_DIR)
	rm -rf $(TARGET_DIR) rabbitmq-service.html

clean: clean_partial
	rm -f rabbitmq-server-windows-*.zip

clean_partial:
	rm -rf $(SOURCE_DIR)
	rm -rf $(TARGET_DIR)
