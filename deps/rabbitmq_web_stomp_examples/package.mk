RELEASABLE:=true
DEPS:=rabbitmq-mochiweb rabbitmq-web-stomp rabbitmq-server

define construct_app_commands
	cp -r $(PACKAGE_DIR)/priv $(APP_DIR)
endef
