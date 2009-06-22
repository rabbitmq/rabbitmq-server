TARBALL_DIR=../../dist
TARBALL=$(shell (cd $(TARBALL_DIR); echo rabbitmq-server-[0-9]*.tar.gz))
VERSION=$(shell echo $(TARBALL) | sed -e 's:rabbitmq-server-\(.*\)\.tar\.gz:\1:g')
UNPACKED_DIR=rabbitmq-server-$(VERSION)
PACKAGENAME=rabbitmq-server
SIGNING_KEY_ID=056E8E56

ifneq "$(UNOFFICIAL_RELEASE)" ""
  SIGNING=-us -uc
else
  SIGNING=-k$(SIGNING_KEY_ID)
endif

all:
	@echo 'Please choose a target from the Makefile.'

MD5=$(shell md5sum $(TARBALL_DIR)/$(TARBALL) | sed -e 's/\([0-9a-f]*\).*/\1/')
SHA1=$(shell openssl sha1 ../../dist/rabbitmq-server-0.0.0.tar.gz | sed -e 's/SHA1\(.*\)= //')
RMD160=$(shell openssl rmd160 ../../dist/rabbitmq-server-0.0.0.tar.gz | sed -e 's/RIPEMD160\(.*\)= //')
package: clean
	echo $(MD5) $(SHA1) $(RMD160)
	sed -e 's/%MD5%/$(MD5)/' -e 's/%SHA1%/$(SHA1)/' -e 's/%RMD160%/$(RMD160)/' -e 's/%VERSION%/$(VERSION)/' < net/rabbitmq-server/Portfile.in > net/rabbitmq-server/Portfile
	#UNOFFICIAL_RELEASE=$(UNOFFICIAL_RELEASE) VERSION=$(VERSION) ./check-changelog.sh rabbitmq-server $(UNPACKED_DIR)

clean:
