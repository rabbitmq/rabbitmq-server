export PATH :=$(CURDIR):$(PATH)
HOSTNAME := $(shell hostname)
MVN_FLAGS += -Dhostname=$(HOSTNAME) \
			 -Dnode1.stream.port=$(NODE1_STREAM_PORT) \
			 -Dnode1.stream.port.tls=$(NODE1_STREAM_PORT_TLS) \
			 -Dnode1.name=$(NODE1_NAME) \
			 -Dnode2.name=$(NODE2_NAME) \
			 -Dnode2.stream.port=$(NODE2_STREAM_PORT) \
			 -Dnode2.stream.port.tls=$(NODE2_STREAM_PORT_TLS) \
			 -Drabbitmqctl.bin=$(RABBITMQCTL)

.PHONY: tests clean

tests:
	# Note: to run a single test
	# @mvnw -q $(MVN_FLAGS) -Dtest=StreamTest#metadataOnClusterShouldReturnLeaderAndReplicas test
	@mvnw -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -B $(MVN_FLAGS) test

clean:
	@mvnw clean
