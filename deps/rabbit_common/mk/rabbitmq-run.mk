.DEFAULT_GOAL: all

.PHONY: run-broker

ifeq ($(filter rabbitmq-dist.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-dist.mk
endif

run-broker:: dist
	@echo TODO -- $(MAKE) $@
