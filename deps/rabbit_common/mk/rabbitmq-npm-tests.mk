# JavaScript/Node tests: any plugin that ships a package.json at its root
# opts in automatically, with no changes needed to the plugin's own Makefile.
# Skipped silently if npm isn't installed, since not every developer works
# with the JavaScript side.
ifneq ($(wildcard $(CURDIR)/package.json),)
.PHONY: npm-test
npm-test:
	@if command -v npm > /dev/null 2>&1; then \
		cd $(CURDIR) && npm install && npm test; \
	else \
		echo "Skipped JavaScript tests for $(PROJECT): npm is not installed locally"; \
	fi

tests:: npm-test
endif
