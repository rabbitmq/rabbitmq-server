.PHONY: tests-with-broker standalone-tests

ifeq ($(filter rabbitmq-run.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-run.mk
endif

test_verbose_0 = @echo " TEST  " $@;
test_verbose_2 = set -x;
test_verbose = $(test_verbose_$(V))

TEST_BEAM_DIRS = $(CURDIR)/test \
		 $(DEPS_DIR)/rabbitmq_test/ebin

pre-standalone-tests:: virgin-test-tmpdir

tests:: tests-with-broker standalone-tests

tests-with-broker: pre-standalone-tests test-dist
	$(verbose) rm -f $(TEST_TMPDIR)/.passed
	$(verbose) $(MAKE) start-background-node \
		RABBITMQ_SERVER_START_ARGS='$(patsubst %, -pa %,$(TEST_BEAM_DIRS))' \
		$(WITH_BROKER_TEST_MAKEVARS) \
		LOG_TO_STDIO=yes
	$(verbose) $(MAKE) start-rabbit-on-node
	-$(exec_verbose) echo > $(TEST_TMPDIR)/test-output && \
	if $(foreach SCRIPT,$(WITH_BROKER_SETUP_SCRIPTS), \
	     MAKE='$(MAKE)' \
	     DEPS_DIR='$(DEPS_DIR)' \
	     NODE_TMPDIR='$(NODE_TMPDIR)' \
	     RABBITMQCTL='$(RABBITMQCTL)' \
	     RABBITMQ_NODENAME='$(RABBITMQ_NODENAME)' \
	     $(WITH_BROKER_TEST_ENVVARS) \
	     $(SCRIPT) &&) \
	   $(foreach CMD,$(WITH_BROKER_TEST_COMMANDS), \
	     echo >> $(TEST_TMPDIR)/test-output && \
	     echo "$(CMD)." \
               | tee -a $(TEST_TMPDIR)/test-output \
               | $(ERL_CALL) $(ERL_CALL_OPTS) \
               | tee -a $(TEST_TMPDIR)/test-output \
               | egrep '{ok, (ok|passed)}' >/dev/null &&) \
	   $(foreach SCRIPT,$(WITH_BROKER_TEST_SCRIPTS), \
	     MAKE='$(MAKE)' \
	     DEPS_DIR='$(DEPS_DIR)' \
	     NODE_TMPDIR='$(NODE_TMPDIR)' \
	     RABBITMQCTL='$(RABBITMQCTL)' \
	     RABBITMQ_NODENAME='$(RABBITMQ_NODENAME)' \
	     $(WITH_BROKER_TEST_ENVVARS) \
	     $(SCRIPT) &&) : ; \
        then \
	  touch $(TEST_TMPDIR)/.passed ; \
	fi
	$(verbose) if test -f $(TEST_TMPDIR)/.passed; then \
	  printf "\nPASSED\n" ; \
	else \
	  cat $(TEST_TMPDIR)/test-output ; \
	  printf "\n\nFAILED\n" ; \
	fi
	$(verbose) sleep 1
	$(verbose) echo 'rabbit_misc:report_cover(), init:stop().' | $(ERL_CALL) $(ERL_CALL_OPTS) >/dev/null
	$(verbose) sleep 1
	$(verbose) test -f $(TEST_TMPDIR)/.passed

standalone-tests: pre-standalone-tests test-dist
	$(exec_verbose) $(if $(STANDALONE_TEST_COMMANDS), \
	  $(foreach CMD,$(STANDALONE_TEST_COMMANDS), \
	    MAKE='$(MAKE)' \
	    DEPS_DIR='$(DEPS_DIR)' \
	    TEST_TMPDIR='$(TEST_TMPDIR)' \
	    RABBITMQCTL='$(RABBITMQCTL)' \
	    ERL_LIBS='$(CURDIR)/$(DIST_DIR):$(DIST_ERL_LIBS)' \
	    $(ERL) $(ERL_OPTS) $(patsubst %,-pa %,$(TEST_BEAM_DIRS)) \
	    -sname standalone_test \
	    -eval "init:stop(case $(CMD) of ok -> 0; passed -> 0; _Else -> 1 end)" && \
	  ) \
	:)
	$(verbose) $(if $(STANDALONE_TEST_SCRIPTS), \
	$(foreach SCRIPT,$(STANDALONE_TEST_SCRIPTS), \
	    MAKE='$(MAKE)' \
	    DEPS_DIR='$(DEPS_DIR)' \
	    TEST_TMPDIR='$(TEST_TMPDIR)' \
	    RABBITMQCTL='$(RABBITMQCTL)' \
	    $(SCRIPT) &&) :)

# Add an alias for the old `make test` target.
.PHONY: test
test: tests
