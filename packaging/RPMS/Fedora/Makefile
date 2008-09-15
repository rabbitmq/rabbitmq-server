# Read README.txt for instructions

VERSION=0.0.0
SOURCE_TARBALL_DIR=../../../dist
TARBALL=$(SOURCE_TARBALL_DIR)/rabbitmq-server-$(VERSION).tar.gz
TOP_DIR=$(shell pwd)
RPM_VERSION=$(shell echo $(VERSION) | tr - _)
DEFINES=--define '_topdir $(TOP_DIR)' --define '_tmppath $(TOP_DIR)/tmp' --define 'main_version $(VERSION)' --define 'rpm_version $(RPM_VERSION)' --define 'debian 1'

rpms:   clean server

#Create proper environment for making rpms
prepare:
	mkdir -p $(TOP_DIR)/BUILD
	mkdir -p $(TOP_DIR)/SOURCES
	mkdir -p $(TOP_DIR)/SPECS
	mkdir -p $(TOP_DIR)/SRPMS
	mkdir -p $(TOP_DIR)/RPMS
	mkdir -p $(TOP_DIR)/tmp
	cp $(TOP_DIR)/$(TARBALL) $(TOP_DIR)/SOURCES
	cp $(TOP_DIR)/rabbitmq-server.spec $(TOP_DIR)/SPECS
	cp $(TOP_DIR)/init.d $(TOP_DIR)/BUILD
	cp $(TOP_DIR)/rabbitmqctl_wrapper $(TOP_DIR)/BUILD

server: prepare
	rpmbuild -ba $(TOP_DIR)/SPECS/rabbitmq-server.spec $(DEFINES) --target noarch

clean:	
	rm -rf $(TOP_DIR)/SOURCES/
	rm -rf $(TOP_DIR)/SPECS/
	rm -rf $(TOP_DIR)/RPMS/
	rm -rf $(TOP_DIR)/SRPMS/
	rm -rf $(TOP_DIR)/BUILD/
	rm -rf $(TOP_DIR)/tmp/
