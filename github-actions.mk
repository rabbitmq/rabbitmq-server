YTT ?= ytt

../../.github/workflows/test-$(PROJECT).yaml: $(wildcard test/*_SUITE.erl) ../../.github/workflows/test-plugin.template.yaml
	$(gen_verbose) $(YTT) \
		--file ../../.github/workflows/test-plugin.template.yaml \
		--data-value-yaml plugin=$(PROJECT) \
		--data-value-yaml suites=[$(subst $(space),$(comma),$(foreach s,$(CT_SUITES),"$s"))] \
		| sed 's/^true:/on:/' > $@
