PROJECT = rabbit_common

DEPS = rabbitmq_codegen
TEST_DEPS = mochiweb

# For RabbitMQ repositories, we want to checkout branches which match
# the parent porject. For instance, if the parent project is on a
# release tag, dependencies must be on the same release tag. If the
# parent project is on a topic branch, dependencies must be on the same
# topic branch or fallback to `stable` or `master` whichever was the
# base of the topic branch.

ifeq ($(origin current_rmq_ref),undefined)
current_rmq_ref := $(shell git symbolic-ref -q --short HEAD || git describe --tags --exact-match)
export current_rmq_ref
endif
ifeq ($(origin base_rmq_ref),undefined)
base_rmq_ref := $(shell git merge-base --is-ancestor $$(git merge-base master HEAD) stable && echo stable || echo master)
export base_rmq_ref
endif

dep_rabbitmq_codegen = git https://github.com/rabbitmq/rabbitmq-codegen.git $(current_rmq_ref) $(base_rmq_ref)

.DEFAULT_GOAL = all

EXTRA_SOURCES += include/rabbit_framing.hrl				\
		 src/rabbit_framing_amqp_0_8.erl			\
		 src/rabbit_framing_amqp_0_9_1.erl

$(PROJECT).d:: $(EXTRA_SOURCES)

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include erlang.mk
include mk/rabbitmq-dist.mk

# --------------------------------------------------------------------
# Framing sources generation.
# --------------------------------------------------------------------

CODEGEN       = $(CURDIR)/codegen.py
CODEGEN_DIR  ?= $(DEPS_DIR)/rabbitmq_codegen
CODEGEN_AMQP  = $(CODEGEN_DIR)/amqp_codegen.py

AMQP_SPEC_JSON_FILES_0_8   = $(CODEGEN_DIR)/amqp-rabbitmq-0.8.json
AMQP_SPEC_JSON_FILES_0_9_1 = $(CODEGEN_DIR)/amqp-rabbitmq-0.9.1.json	\
			     $(CODEGEN_DIR)/credit_extension.json

include/rabbit_framing.hrl:: $(CODEGEN) $(CODEGEN_AMQP) \
    $(AMQP_SPEC_JSON_FILES_0_9_1) $(AMQP_SPEC_JSON_FILES_0_8)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	 $(CODEGEN) --ignore-conflicts header \
	 $(AMQP_SPEC_JSON_FILES_0_9_1) $(AMQP_SPEC_JSON_FILES_0_8) $@

src/rabbit_framing_amqp_0_9_1.erl:: $(CODEGEN) $(CODEGEN_AMQP) \
    $(AMQP_SPEC_JSON_FILES_0_9_1)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	 $(CODEGEN) body $(AMQP_SPEC_JSON_FILES_0_9_1) $@

src/rabbit_framing_amqp_0_8.erl:: $(CODEGEN) $(CODEGEN_AMQP) \
    $(AMQP_SPEC_JSON_FILES_0_8)
	$(gen_verbose) env PYTHONPATH=$(CODEGEN_DIR) \
	 $(CODEGEN) body $(AMQP_SPEC_JSON_FILES_0_8) $@

clean:: clean-extra-sources

clean-extra-sources:
	$(gen_verbose) rm -f $(EXTRA_SOURCES)
