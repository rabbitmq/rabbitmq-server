ifeq ($(filter rabbitmq-build.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-build.mk
endif

ifeq ($(filter rabbitmq-hexpm.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-hexpm.mk
endif

ifeq ($(filter rabbitmq-dist.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-dist.mk
endif

ifeq ($(filter rabbitmq-run.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-run.mk
endif

ifeq ($(filter rabbitmq-test.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-test.mk
endif

ifeq ($(filter rabbitmq-tools.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-tools.mk
endif
