# Any plugin with a package.json at its root runs its JavaScript tests as
# part of `tests`. Skipped if npm is not installed.
ifneq ($(wildcard $(CURDIR)/package.json),)
.PHONY: npm-test
npm-test:
	@if command -v npm > /dev/null 2>&1; then \
		cd $(CURDIR) && \
		{ [ package-lock.json -nt package.json ] || npm install; } && \
		npm test -- 'test/**/*.test.js'; \
	else \
		echo "Skipped JavaScript tests for $(PROJECT): npm is not installed locally"; \
	fi

tests:: npm-test
endif
