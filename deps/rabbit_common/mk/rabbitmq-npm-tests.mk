# Any plugin with a `package.json` at its root runs its JavaScript tests
# as part of `tests`. A node too old for them, and an empty test list,
# both look like a clean run, so CI sets `REQUIRE_NPM_TESTS` to fail.
ifneq ($(wildcard $(CURDIR)/package.json),)
NODE_MIN_MAJOR = 21
NPM_TEST_FILES = $(shell find $(CURDIR)/test -name '*.test.js' 2>/dev/null)

.PHONY: npm-test
npm-test:
	@if command -v node > /dev/null 2>&1 \
	    && node -e 'process.exit(+process.versions.node.split(".")[0] >= $(NODE_MIN_MAJOR) ? 0 : 1)' \
	    && [ -n "$(NPM_TEST_FILES)" ]; then \
		cd $(CURDIR) && node --test $(NPM_TEST_FILES); \
	elif [ -n "$(REQUIRE_NPM_TESTS)" ]; then \
		echo "Cannot run the JavaScript tests for $(PROJECT)"; \
		exit 1; \
	else \
		echo "Skipped the JavaScript tests for $(PROJECT)"; \
	fi

tests:: npm-test
endif
