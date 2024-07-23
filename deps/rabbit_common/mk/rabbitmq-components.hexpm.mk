ifeq ($(.DEFAULT_GOAL),)
# Define default goal to `all` because this file defines some targets
# before the inclusion of erlang.mk leading to the wrong target becoming
# the default.
.DEFAULT_GOAL = all
endif

# PROJECT_VERSION defaults to:
#   1. the version exported by environment;
#   2. the version stored in `git-revisions.txt`, if it exists;
#   3. a version based on git-describe(1), if it is a Git clone;
#   4. 0.0.0
#
# Note that in the case where git-describe(1) is used
# (e.g. during development), running "git gc" may help
# improve the performance.

PROJECT_VERSION := $(RABBITMQ_VERSION)

ifeq ($(PROJECT_VERSION),)
ifneq ($(wildcard git-revisions.txt),)
PROJECT_VERSION = $(shell \
	head -n1 git-revisions.txt | \
	awk '{print $$$(words $(PROJECT_DESCRIPTION) version);}')
else
PROJECT_VERSION = $(shell \
	(git describe --dirty --abbrev=7 --tags --always --first-parent \
		2>/dev/null || echo 0.0.0) | \
		sed -e 's/^v//' -e 's/_/./g' -e 's/-/+/' -e 's/-/./g')
endif
endif


# --------------------------------------------------------------------
# RabbitMQ components.
# --------------------------------------------------------------------

dep_amqp10_client                     = hex $(PROJECT_VERSION)
dep_amqp10_common                     = hex $(PROJECT_VERSION)
dep_amqp_client                       = hex $(PROJECT_VERSION)
dep_rabbit_common                     = hex $(PROJECT_VERSION)

# Third-party dependencies version pinning.
