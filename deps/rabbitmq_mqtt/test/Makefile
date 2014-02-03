UPSTREAM_GIT=https://git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.java.git
REVISION=00b5b2f99ae8410b7d96d106e080a092c5f92546

JC=javac

TEST_SRC=src
CHECKOUT_DIR=test_client
PAHO_JAR_NAME=org.eclipse.paho.client.mqttv3.jar
PAHO_JAR=$(CHECKOUT_DIR)/org.eclipse.paho.client.mqttv3/$(PAHO_JAR_NAME)
JUNIT_JAR=../lib/junit.jar
JAVA_AMQP_DIR=../../rabbitmq-java-client/
JAVA_AMQP_CLASSES=$(JAVA_AMQP_DIR)build/classes/

TEST_SRCS:=$(shell find $(TEST_SRC) -name '*.java')
ALL_CLASSES:=$(foreach f,$(shell find src -name '*.class'),'$(f)')
TEST_CLASSES:=$(TEST_SRCS:.java=.class)
CP:=$(PAHO_JAR):$(JUNIT_JAR):$(TEST_SRC):$(JAVA_AMQP_CLASSES)

define class_from_path
$(subst .class,,$(subst src.,,$(subst /,.,$(1))))
endef

.PHONY: test
test: $(TEST_CLASSES) build_java_amqp
	$(foreach test,$(TEST_CLASSES),CLASSPATH=$(CP) java  junit.textui.TestRunner -text $(call class_from_path,$(test)))

clean:
	rm -rf $(PAHO_JAR) $(ALL_CLASSES)

distclean: clean
	rm -rf $(CHECKOUT_DIR)

$(CHECKOUT_DIR):
	git clone $(UPSTREAM_GIT) $@
	(cd $@ && git checkout $(REVISION)) || rm -rf $@

$(PAHO_JAR): $(CHECKOUT_DIR)
	ant -buildfile $</org.eclipse.paho.client.mqttv3/build.xml \
	-Dship.folder=. -Dmqttv3-client-jar=$(PAHO_JAR_NAME) full

%.class: %.java $(PAHO_JAR) $(JUNIT_JAR)
	$(JC) -cp $(CP) $<

.PHONY: build_java_amqp
build_java_amqp:
	make -C $(JAVA_AMQP_DIR)
