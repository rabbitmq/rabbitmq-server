UPSTREAM_GIT=https://github.com/eclipse/paho.mqtt.java
REVISION=00b5b2f99ae8410b7d96d106e080a092c5f92546

JC=javac

TEST_SRC=src
CHECKOUT_DIR=test_client
PAHO_JAR_NAME=org.eclipse.paho.client.mqttv3.jar
PAHO_JAR=$(CHECKOUT_DIR)/org.eclipse.paho.client.mqttv3/$(PAHO_JAR_NAME)
JUNIT_JAR=../lib/junit.jar
JAVA_AMQP_DIR=$(DEPS_DIR)/rabbitmq_java_client
JAVA_AMQP_CLASSES=$(JAVA_AMQP_DIR)/build/classes/

ALL_CLASSES:=$(foreach f,$(shell find src -name '*.class'),'$(f)')
CP:=$(PAHO_JAR):$(JUNIT_JAR):$(TEST_SRC):$(JAVA_AMQP_CLASSES)

HOSTNAME:=$(shell hostname)

define class_from_path
$(subst .class,,$(subst src.,,$(subst /,.,$(1))))
endef

.PHONY: test
test: build_java_amqp
	ant test -Dhostname=$(HOSTNAME) -Dcerts.dir=$(SSL_CERTS_DIR)

clean:
	ant clean
	rm -rf test_client


distclean: clean
	rm -rf $(CHECKOUT_DIR)

$(CHECKOUT_DIR):
	git clone $(UPSTREAM_GIT) $@
	(cd $@ && git checkout $(REVISION)) || rm -rf $@


.PHONY: build_java_amqp
build_java_amqp: $(CHECKOUT_DIR)
	$(MAKE) -C $(JAVA_AMQP_DIR) jar
