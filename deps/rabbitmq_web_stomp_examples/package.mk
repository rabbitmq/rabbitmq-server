RELEASABLE:=true
DEPS:=rabbitmq-server rabbitmq-web-stomp rabbitmq-mochiweb

define construct_app_commands
	cp -r $(PACKAGE_DIR)/priv $(APP_DIR)
endef
