ifeq ($(filter rabbitmq-dist.mk,$(notdir $(MAKEFILE_LIST))),)
$(info Loading rabbitmq-dist.mk $(MAKEFILE_LIST))
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-dist.mk
endif

ifeq ($(filter rabbitmq-run.mk,$(notdir $(MAKEFILE_LIST))),)
$(info Loading rabbitmq-run.mk $(MAKEFILE_LIST))
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-run.mk
endif
