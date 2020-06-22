export PATH :=$(CURDIR):$(PATH)
HOSTNAME := $(shell hostname)
MVN_FLAGS += -Dhostname=$(HOSTNAME) \
			 -Dnode1.stream.port=$(NODE1_STREAM_PORT) \
			 -Dnode1.name=$(NODE1_NAME) \
			 -Drabbitmqctl.bin=$(RABBITMQCTL)

.PHONY: tests clean

tests:
	# Note: to run a single test
	# @mvnw -q $(MVN_FLAGS) -Dtest=StreamTest#metadataOnClusterShouldReturnLeaderAndReplicas test
	@mvnw $(MVN_FLAGS) test

clean:
	@mvnw clean
