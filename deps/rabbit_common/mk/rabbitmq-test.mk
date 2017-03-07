.PHONY: ct-slow ct-fast

ct-slow ct-fast:
	$(MAKE) ct CT_SUITES='$(CT_SUITES)'
