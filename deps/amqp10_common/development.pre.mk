# Variables and recipes in development.*.mk are meant to be used from
# any Git clone. They are excluded from the files published to Hex.pm.
# Generated files are published to Hex.pm however so people using this
# source won't have to depend on Python and rabbitmq-codegen.

BUILD_DEPS += rabbitmq_codegen

EXTRA_SOURCES += include/amqp10_framing.hrl				\
		 src/amqp10_framing0.erl

.DEFAULT_GOAL = all
$(PROJECT).d:: $(EXTRA_SOURCES)
