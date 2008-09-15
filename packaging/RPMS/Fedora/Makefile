# Read README.txt for instructions

VERSION=0.0.0
SOURCE_TARBALL_DIR=../../../dist
PACKAGE_VERSION_NAME=rabbitmq-server-$(VERSION)
TARBALL=$(SOURCE_TARBALL_DIR)/$(PACKAGE_VERSION_NAME).tar.gz
TOP_DIR=$(shell pwd)
RPM_VERSION=$(shell echo $(VERSION) | tr - _)
DEFINES=--define '_topdir $(TOP_DIR)' --define '_tmppath $(TOP_DIR)/tmp' --define 'main_version $(VERSION)' --define 'rpm_version $(RPM_VERSION)'
RPM_SOURCE_DIR=$(PACKAGE_VERSION_NAME)/rpm

rpms:   clean server

#Create proper environment for making rpms
prepare:
	mkdir -p BUILD SOURCES SPECS SRPMS RPMS tmp
	cp $(TOP_DIR)/$(TARBALL) SOURCES
	cp rabbitmq-server.spec SPECS

	rm -rf tmp/$(PACKAGE_VERSION_NAME)*
	tar -zxf $(TOP_DIR)/$(TARBALL) -C tmp
	cp -r tmp/$(PACKAGE_VERSION_NAME) tmp/$(PACKAGE_VERSION_NAME)-orig

	mkdir tmp/$(RPM_SOURCE_DIR)
	cp init.d tmp/$(RPM_SOURCE_DIR)
	cp rabbitmqctl_wrapper tmp/$(RPM_SOURCE_DIR)
	cp rabbitmq-server.logrotate tmp/$(RPM_SOURCE_DIR)
	- (cd tmp; diff -aurN $(PACKAGE_VERSION_NAME)-orig $(PACKAGE_VERSION_NAME) \
		> ../SOURCES/rpm.patch )

server: prepare
	rpmbuild -ba SPECS/rabbitmq-server.spec $(DEFINES) --target noarch

clean:	
	rm -rf SOURCES SPECS RPMS SRPMS BUILD tmp
