all: package-generic-unix
	@:

# --------------------------------------------------------------------
# Packaging.
# --------------------------------------------------------------------

.PHONY: package-generic-unix \
	docker-image \
	docker-image-push

PACKAGES_DIR ?= ../PACKAGES
SOURCE_DIST_FILE ?= $(wildcard $(PACKAGES_DIR)/rabbitmq-server-*.tar.xz)

ifneq ($(filter-out clean,$(MAKECMDGOALS)),)
ifeq ($(SOURCE_DIST_FILE),)
$(error Cannot find source archive; please specify SOURCE_DIST_FILE)
endif
ifneq ($(words $(SOURCE_DIST_FILE)),1)
$(error Multiple source archives found; please specify SOURCE_DIST_FILE)
endif
ifeq ($(filter %.tar.xz %.txz,$(SOURCE_DIST_FILE)),)
$(error The source archive must a tar.xz archive)
endif
ifeq ($(wildcard $(SOURCE_DIST_FILE)),)
$(error The source archive must exist)
endif
endif

ifndef NO_CLEAN
DO_CLEAN := clean
endif

VARS = SOURCE_DIST_FILE="$(abspath $(SOURCE_DIST_FILE))" \
       PACKAGES_DIR="$(abspath $(PACKAGES_DIR))" \
       SIGNING_KEY="$(SIGNING_KEY)"

package-generic-unix: $(SOURCE_DIST_FILE)
	$(gen_verbose) $(MAKE) -C generic-unix $(VARS) all $(DO_CLEAN)

docker-image:
	$(gen_verbose) $(MAKE) -C docker-image $(VARS) all $(DO_CLEAN)

docker-image-push:
	$(gen_verbose) $(MAKE) -C docker-image $(VARS) push $(DO_CLEAN)

.PHONY: clean

clean:
	for subdir in generic-unix docker-image; do \
		$(MAKE) -C "$$subdir" clean; \
	done
