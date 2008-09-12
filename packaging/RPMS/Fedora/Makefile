# Read README.txt for instructions

VERSION=0.0.0
SOURCE_TARBALL_DIR=../../../dist
TARBALL=$(SOURCE_TARBALL_DIR)/rabbitmq-server-$(VERSION).tar.gz
TOP_DIR=$(shell pwd)
RPM_VERSION=$(shell echo $(VERSION) | tr - _)
DEFINES=--define '_topdir $(TOP_DIR)' --define '_tmppath $(TOP_DIR)/tmp' --define 'main_version $(VERSION)' --define 'rpm_version $(RPM_VERSION)'
RPM_SOURCE_DIR=rabbitmq-server-$(VERSION)/rpm

rpms:   clean server

#Create proper environment for making rpms
prepare:
	mkdir -p BUILD
	mkdir -p SOURCES
	mkdir -p SPECS
	mkdir -p SRPMS
	mkdir -p RPMS
	mkdir -p tmp
	cp $(TOP_DIR)/$(TARBALL) SOURCES
	cp rabbitmq-server.spec SPECS
	
	mkdir -p tmp/$(RPM_SOURCE_DIR)
	cp init.d tmp/$(RPM_SOURCE_DIR)
	cp rabbitmqctl_wrapper tmp/$(RPM_SOURCE_DIR)
	cp rabbitmq-server.logrotate tmp/$(RPM_SOURCE_DIR)
	
	gunzip SOURCES/rabbitmq-server-$(VERSION).tar.gz
	tar -C tmp -rf  $(TOP_DIR)/SOURCES/rabbitmq-server-$(VERSION).tar \
		$(RPM_SOURCE_DIR)/
	gzip SOURCES/rabbitmq-server-$(VERSION).tar
	rm -rf tmp/rabbitmq-server*

server: prepare
	rpmbuild -ba SPECS/rabbitmq-server.spec $(DEFINES) --target noarch

clean:	
	rm -rf SOURCES
	rm -rf SPECS
	rm -rf RPMS
	rm -rf SRPMS
	rm -rf BUILD
	rm -rf tmp
