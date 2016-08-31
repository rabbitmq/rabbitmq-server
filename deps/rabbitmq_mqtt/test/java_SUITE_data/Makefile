HOSTNAME := $(shell hostname)

.PHONY: deps tests clean distclean

deps:
	mkdir -p lib
	mvn dependency:copy-dependencies -DoutputDirectory=lib

tests: deps
	ant test \
		-Dhostname=$(HOSTNAME) \
		-Dcerts.dir=$(SSL_CERTS_DIR) \
		-Dmqtt.ssl.port=$(MQTT_SSL_PORT) \
		-Dmqtt.port=$(MQTT_PORT) \
		-Damqp.port=$(AMQP_PORT)

clean:
	ant clean

distclean: clean
	rm -f lib/*.jar
