VERSION=0.0.0
SOURCE_TARBALL_DIR=../../../dist
TARBALL=$(SOURCE_TARBALL_DIR)/rabbitmq-server-$(VERSION).tar.gz
TOP_DIR=$(shell pwd)
#Under debian we do not want to check build dependencies, since that
#only checks build-dependencies using rpms, not debs
DEFINES=--define '_topdir $(TOP_DIR)' --define '_tmppath $(TOP_DIR)/tmp' --define 'debian 1'

rpms:   clean server

prepare:
	mkdir -p BUILD SOURCES SPECS SRPMS RPMS tmp
	cp $(TOP_DIR)/$(TARBALL) SOURCES
	cp rabbitmq-server.spec SPECS
	sed -i 's/%%VERSION%%/$(VERSION)/' SPECS/rabbitmq-server.spec

	cp init.d SOURCES/rabbitmq-server.init
	cp rabbitmqctl_wrapper SOURCES/rabbitmq-server.wrapper
	cp rabbitmq-server.logrotate SOURCES/rabbitmq-server.logrotate

server: prepare
	rpmbuild -ba SPECS/rabbitmq-server.spec $(DEFINES) --target i386
	rpmbuild -ba SPECS/rabbitmq-server.spec $(DEFINES) --define '_arch x86_64' \
		--define '_defaultdocdir /usr/share/doc' --target x86_64

clean:	
	rm -rf SOURCES SPECS RPMS SRPMS BUILD tmp
