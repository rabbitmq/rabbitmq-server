TARBALL_DIR=../../../dist
TARBALL=$(notdir $(wildcard $(TARBALL_DIR)/rabbitmq-server-[0-9.]*.tar.gz))
COMMON_DIR=../../common
VERSION=$(shell echo $(TARBALL) | sed -e 's:rabbitmq-server-\(.*\)\.tar\.gz:\1:g')

TOP_DIR=$(shell pwd)
#Under debian we do not want to check build dependencies, since that
#only checks build-dependencies using rpms, not debs
DEFINES=--define '_topdir $(TOP_DIR)' --define '_tmppath $(TOP_DIR)/tmp' --define '_sysconfdir /etc' --define '_localstatedir /var'

ifndef RPM_OS
RPM_OS=fedora
endif

ifeq "$(RPM_OS)" "suse"
FUNCTION_LIBRARY=
REQUIRES=/sbin/chkconfig /sbin/service
OS_DEFINES=--define '_initrddir /etc/init.d' --define 'dist .suse'
SPEC_DEFINES=--define 'group_tag Productivity/Networking/Other'
START_PROG=startproc
else
FUNCTION_LIBRARY=\# Source function library.\n. /etc/init.d/functions
REQUIRES=chkconfig initscripts
OS_DEFINES=--define '_initrddir /etc/rc.d/init.d'
SPEC_DEFINES=--define 'group_tag Development/Libraries'
START_PROG=daemon
endif

rpms:   clean server

prepare:
	mkdir -p BUILD SOURCES SPECS SRPMS RPMS tmp
	cp $(TARBALL_DIR)/$(TARBALL) SOURCES
	cp rabbitmq-server.spec SPECS
	sed -i 's|%%VERSION%%|$(VERSION)|;s|%%REQUIRES%%|$(REQUIRES)|' \
		SPECS/rabbitmq-server.spec

	cp ${COMMON_DIR}/* SOURCES/
	cp rabbitmq-server.init SOURCES/rabbitmq-server.init
	sed -i \
	    -e 's|^START_PROG=.*$$|START_PROG="$(START_PROG)"|' \
	    -e 's|^@FUNCTION_LIBRARY@|$(FUNCTION_LIBRARY)|' \
	    SOURCES/rabbitmq-server.init
ifeq "$(RPM_OS)" "fedora"
# Fedora says that only vital services should have Default-Start
	sed -i -e '/^# Default-Start:/d;/^# Default-Stop:/d' \
	    SOURCES/rabbitmq-server.init
endif
	sed -i -e 's|@SU_RABBITMQ_SH_C@|su rabbitmq -s /bin/sh -c|' \
	       -e 's|@STDOUT_STDERR_REDIRECTION@||' \
	    SOURCES/rabbitmq-script-wrapper
	cp rabbitmq-server.logrotate SOURCES/rabbitmq-server.logrotate

server: prepare
	rpmbuild -ba --nodeps SPECS/rabbitmq-server.spec $(DEFINES) $(OS_DEFINES) $(SPEC_DEFINES)

clean:
	rm -rf SOURCES SPECS RPMS SRPMS BUILD tmp
