SIGNING_USER_EMAIL=info@rabbitmq.com

ifeq "$(UNOFFICIAL_RELEASE)" ""
HOME_ARG=HOME=$(GNUPG_PATH)
endif

all: debian_apt_repository

clean:
	rm -rf debian

CAN_HAS_REPREPRO=$(shell [ -f /usr/bin/reprepro ] && echo true)
ifeq ($(CAN_HAS_REPREPRO), true)
debian_apt_repository: clean
	mkdir -p debian/conf
	cp -a distributions debian/conf
ifeq "$(UNOFFICIAL_RELEASE)" ""
	echo SignWith: $(SIGNING_USER_EMAIL) >> debian/conf/distributions
endif
	for FILE in ../Debian/*.changes ; do \
		$(HOME_ARG) reprepro --ignore=wrongdistribution \
			-Vb debian include kitten $${FILE} ; \
	done
	reprepro -Vb debian createsymlinks
else
debian_apt_repository:
	@echo Not building APT repository as reprepro could not be found
endif
