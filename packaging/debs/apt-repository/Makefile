
ifeq "$(UNOFFICIAL_RELEASE)" ""
HOME_ARG=HOME=$(GNUPG_PATH)
endif

all: debian_apt_repository

clean:
	rm -rf debian

CAN_HAS_APTLY=$(shell [ -f /usr/bin/aptly ] && echo true)
ifeq ($(CAN_HAS_APTLY), true)
debian_apt_repository: clean
	mkdir -p debian/conf
	cp -a distributions debian/conf
	
	./aptly_wrapper.sh -o fetch
	for FILE in ../Debian/*.deb ; do \
		./aptly_wrapper.sh -o add -f $${FILE} ; \
	done
	for FILE in ../Debian/*.dsc ; do \
		./aptly_wrapper.sh -o add -f $${FILE} ; \
	done
ifeq ($(UNOFFICIAL_RELEASE), true)
	@echo UNOFFICIAL_RELEASE set to true
	sed -i '/gpgDisableSign/ s/false/true/' aptly.conf
	./aptly_wrapper.sh -o publish 
else
	sed -i '/gpgDisableSign/ s/true/false/' aptly.conf
	./aptly_wrapper.sh -o publish -h $(HOME_ARG) -g $(SIGNING_KEY)
endif
	./aptly_wrapper.sh -o push
else
debian_apt_repository:
	@echo Not building APT repository as aptly could not be found
endif
