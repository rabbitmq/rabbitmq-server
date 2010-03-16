VERSION=0.0.0
SOURCE_DIR=rabbitmq-server-$(VERSION)
TARGET_DIR=rabbitmq_server-$(VERSION)
TARGET_ZIP=rabbitmq-server-windows-$(VERSION)

dist:
	$(MAKE) -C ../.. VERSION=$(VERSION) srcdist
	tar -zxvf ../../dist/$(SOURCE_DIR).tar.gz
	$(MAKE) -C $(SOURCE_DIR)

	mkdir $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmq-server.bat $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmq-service.bat $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmqctl.bat $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmq-multi.bat $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmq-activate-plugins.bat $(SOURCE_DIR)/sbin
	mv $(SOURCE_DIR)/scripts/rabbitmq-deactivate-plugins.bat $(SOURCE_DIR)/sbin
	rm -rf $(SOURCE_DIR)/scripts
	rm -rf $(SOURCE_DIR)/codegen* $(SOURCE_DIR)/Makefile
	rm -f $(SOURCE_DIR)/README
	rm -rf $(SOURCE_DIR)/docs

	mv $(SOURCE_DIR) $(TARGET_DIR)
	mkdir -p $(TARGET_DIR)
	xmlto -o . xhtml-nochunks ../../docs/rabbitmq-service.xml
	elinks -dump -no-references -no-numbering rabbitmq-service.html \
		> $(TARGET_DIR)/readme-service.txt
	todos $(TARGET_DIR)/readme-service.txt
	zip -r $(TARGET_ZIP).zip $(TARGET_DIR)
	rm -rf $(TARGET_DIR) rabbitmq-service.html

clean: clean_partial
	rm -f rabbitmq-server-windows-*.zip

clean_partial:
	rm -rf $(SOURCE_DIR)
	rm -rf $(TARGET_DIR)
