export PATH :=$(CURDIR):$(PATH)
HOSTNAME := $(shell hostname)
MVN_FLAGS += -Dhostname=$(HOSTNAME) \
			 -Dstream.port=$(STREAM_PORT)

.PHONY: tests clean

tests:
	# Note: to run a single test
	# @mvnw -q $(MVN_FLAGS) -Dtest=StreamTest#metadataOnClusterShouldReturnLeaderAndReplicas test
	@mvnw $(MVN_FLAGS) test

clean:
	@mvnw clean
