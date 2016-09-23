HOSTNAME := $(shell hostname)
MVN_FLAGS += -Ddeps.dir="$(abspath $(DEPS_DIR))" \
			 -Dhostname=$(HOSTNAME) \
			 -Dcerts.dir=$(SSL_CERTS_DIR) \
			 -Dmqtt.ssl.port=$(MQTT_SSL_PORT) \
			 -Dmqtt.port=$(MQTT_PORT) \
			 -Damqp.port=$(AMQP_PORT)

.PHONY: deps tests clean distclean

deps:
	mkdir -p lib
	mvn dependency:copy-dependencies -DoutputDirectory=lib

tests:
	mvn $(MVN_FLAGS) test

clean:
	mvn clean

distclean: clean
	rm -f lib/*.jar
