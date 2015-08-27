.PHONY: run-broker

ifeq ($(filter rabbitmq-dist.mk,$(notdir $(MAKEFILE_LIST))),)
$(info Loading rabbitmq-dist.mk)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-dist.mk
endif

run-broker:: dist
	@echo TODO -- $(MAKE) $@
