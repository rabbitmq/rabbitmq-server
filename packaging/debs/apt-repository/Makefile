PACKAGES_DIR ?= ../../../PACKAGES
REPO_DIR ?= debian

SIGNING_KEY ?= default

ifeq "$(UNOFFICIAL_RELEASE)" ""
HOME_ARG = HOME=$(GNUPG_PATH)
endif

all: debian_apt_repository

clean:
	rm -rf $(REPO_DIR)

CAN_HAS_REPREPRO=$(shell [ -f /usr/bin/reprepro ] && echo true)
ifeq ($(CAN_HAS_REPREPRO), true)
debian_apt_repository: clean
	mkdir -p $(REPO_DIR)/conf
	cp -a distributions $(REPO_DIR)/conf
ifeq "$(UNOFFICIAL_RELEASE)" ""
	echo SignWith: $(SIGNING_KEY) >> $(REPO_DIR)/conf/distributions
endif
	for FILE in $(PACKAGES_DIR)/*.changes ; do \
		$(HOME_ARG) reprepro --ignore=wrongdistribution \
			-Vb $(REPO_DIR) include kitten $${FILE} ; \
	done
	reprepro -Vb $(REPO_DIR) createsymlinks
else
debian_apt_repository:
	@echo Not building APT repository as reprepro could not be found
endif
