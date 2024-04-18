ifeq ($(CT_SUITES),)
define suites_yaml
---
$(PROJECT):
  suites: []
endef
else
define suites_yaml
---
$(PROJECT):
  suites:
  - $(subst $(space),$(newline)  - ,$(CT_SUITES))
endef
endif

../../.github/workflows/data/$(PROJECT).yaml: $(wildcard test/*_SUITE.erl)
	$(file > $@,$(suites_yaml))
