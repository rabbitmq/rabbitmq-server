
ifeq "$(UNOFFICIAL_RELEASE)" ""
HOME_ARG=HOME=$(GNUPG_PATH)
endif

all: debian_apt_repository

CAN_HAS_APTLY=$(shell [ -f /usr/bin/aptly ] && echo true)
ifeq ($(CAN_HAS_APTLY), true)
debian_apt_repository: 
	
	./aptly_wrapper.sh -o fetch -d $(DEPLOY_HOST) -p $(DEPLOY_PATH)
	for FILE in ../Debian/*.d?? ; do \
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
	./aptly_wrapper.sh -o push -d $(DEPLOY_HOST) -p $(DEPLOY_PATH)
else
debian_apt_repository:
	@echo Not building APT repository as aptly could not be found
endif
