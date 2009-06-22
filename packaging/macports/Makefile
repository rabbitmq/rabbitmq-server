TARBALL_DIR=../../dist
TARBALL=$(shell (cd $(TARBALL_DIR); echo rabbitmq-server-[0-9]*.tar.gz))
VERSION=$(shell echo $(TARBALL) | sed -e 's:rabbitmq-server-\(.*\)\.tar\.gz:\1:g')

MD5=$(shell openssl md5 $(TARBALL_DIR)/$(TARBALL) | sed -e 's/MD5\(.*\)= //')
SHA1=$(shell openssl sha1 $(TARBALL_DIR)/$(TARBALL) | sed -e 's/SHA1\(.*\)= //')
RMD160=$(shell openssl rmd160 $(TARBALL_DIR)/$(TARBALL) | sed -e 's/RIPEMD160\(.*\)= //')

all:
	@echo 'Please choose a target from the Makefile.'

package: clean
	sed -e 's/%MD5%/$(MD5)/' -e 's/%SHA1%/$(SHA1)/' -e 's/%RMD160%/$(RMD160)/' -e 's/%VERSION%/$(VERSION)/' < net/rabbitmq-server/Portfile.in > net/rabbitmq-server/Portfile
	tar cvf rabbitmq-server-$(VERSION)-macports.tar.gz --exclude=Portfile.in net

clean:
	rm -f net/rabbitmq-server/Portfile
	rm -f *-macports.tar.gz
