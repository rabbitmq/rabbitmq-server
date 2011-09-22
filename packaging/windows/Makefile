VERSION=0.0.0
SOURCE_DIR=rabbitmq-server-$(VERSION)
TARGET_DIR=rabbitmq_server-$(VERSION)
TARGET_ZIP=rabbitmq-server-windows-$(VERSION)

dist:
	tar -zxf ../../dist/$(SOURCE_DIR).tar.gz
	$(MAKE) -C $(SOURCE_DIR)

	mkdir $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmq-server.bat $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmq-service.bat $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmq-plugins.bat $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmqctl.bat $(SOURCE_DIR)/sbin
	rm -rf $(SOURCE_DIR)/scripts
	rm -rf $(SOURCE_DIR)/codegen* $(SOURCE_DIR)/Makefile $(SOURCE_DIR)/*mk
	rm -f $(SOURCE_DIR)/README
	rm -rf $(SOURCE_DIR)/docs
	rm -rf $(SOURCE_DIR)/src
	rm -rf $(SOURCE_DIR)/dist

	mv $(SOURCE_DIR) $(TARGET_DIR)
	mkdir -p $(TARGET_DIR)
	mkdir -p $(TARGET_DIR)/plugins
	echo "This directory is unused.  See the plugins-dist directory."> $(TARGET_DIR)/plugins/UNUSED-README-UNUSED.txt
	mv $(TARGET_DIR)/plugins-src/README $(TARGET_DIR)/plugins-dist/README.txt
	xmlto -o . xhtml-nochunks ../../docs/rabbitmq-service.xml
	elinks -dump -no-references -no-numbering rabbitmq-service.html \
		> $(TARGET_DIR)/readme-service.txt
	todos $(TARGET_DIR)/readme-service.txt
	rm -rf $(TARGET_DIR)/plugins-src
	zip -q -r $(TARGET_ZIP).zip $(TARGET_DIR)
	rm -rf $(TARGET_DIR) rabbitmq-service.html

clean: clean_partial
	rm -f rabbitmq-server-windows-*.zip

clean_partial:
	rm -rf $(SOURCE_DIR)
	rm -rf $(TARGET_DIR)
