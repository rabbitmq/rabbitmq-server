# Read README.txt for instructions

VERSION=0.0.0
SOURCE_TARBALL_DIR=../../../dist
TARBALL=$(SOURCE_TARBALL_DIR)/rabbitmq-server-$(VERSION).tar.gz
TOP_DIR=$(shell pwd)
DEFINES=--define '_topdir $(TOP_DIR)' --define '_tmppath $(TOP_DIR)/tmp' --define 'debian 1'

rpms:   clean server

#Create proper environment for making rpms
prepare:
	mkdir -p BUILD SOURCES SPECS SRPMS RPMS tmp
	cp $(TOP_DIR)/$(TARBALL) SOURCES
	cp rabbitmq-server.spec SPECS
	sed -i 's/%%VERSION%%/$(VERSION)/' SPECS/rabbitmq-server.spec

	cp init.d SOURCES/rabbitmq-server.init
	cp rabbitmqctl_wrapper SOURCES/rabbitmq-server.wrapper
	cp rabbitmq-server.logrotate SOURCES/rabbitmq-server.logrotate

server: prepare
	rpmbuild -ba SPECS/rabbitmq-server.spec $(DEFINES) --target noarch

clean:	
	rm -rf SOURCES SPECS RPMS SRPMS BUILD tmp
