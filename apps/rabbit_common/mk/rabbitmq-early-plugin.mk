ifeq ($(filter rabbitmq-early-test.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-early-test.mk
endif
