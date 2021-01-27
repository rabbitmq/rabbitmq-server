export PATH :=$(CURDIR):$(PATH)
HOSTNAME := $(shell hostname)
MVN_FLAGS += -Dstream.port=$(STREAM_PORT) \
             -Dprometheus.port=$(PROMETHEUS_PORT)

.PHONY: tests clean

tests:
	# Note: to run a single test
	# @mvnw -q $(MVN_FLAGS) -Dtest=StreamTest#metadataOnClusterShouldReturnLeaderAndReplicas test
	@mvnw -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -B $(MVN_FLAGS) test

clean:
	@mvnw clean
