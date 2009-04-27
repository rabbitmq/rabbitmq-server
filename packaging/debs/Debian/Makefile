TARBALL_DIR=../../../dist
TARBALL=$(shell (cd $(TARBALL_DIR); echo rabbitmq-server-[0-9]*.tar.gz))
COMMON_DIR=../../common
DEBIAN_ORIG_TARBALL=$(shell echo $(TARBALL) |  sed -e 's:\(.*\)-\(.*\)\(\.tar\.gz\):\1_\2\.orig\3:g')
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

package: clean
	cp $(TARBALL_DIR)/$(TARBALL) $(DEBIAN_ORIG_TARBALL)
	tar -zxvf $(DEBIAN_ORIG_TARBALL)
	cp -r debian $(UNPACKED_DIR)
	cp $(COMMON_DIR)/* $(UNPACKED_DIR)/debian/
	chmod a+x $(UNPACKED_DIR)/debian/rules
	UNOFFICIAL_RELEASE=$(UNOFFICIAL_RELEASE) VERSION=$(VERSION) ./check-changelog.sh rabbitmq-server $(UNPACKED_DIR)
	cd $(UNPACKED_DIR); GNUPGHOME=$(GNUPG_PATH)/.gnupg dpkg-buildpackage -rfakeroot $(SIGNING)
	rm -rf $(UNPACKED_DIR)

clean:
	rm -rf $(UNPACKED_DIR)
	rm -f $(PACKAGENAME)_*.tar.gz
	rm -f $(PACKAGENAME)_*.diff.gz
	rm -f $(PACKAGENAME)_*.dsc
	rm -f $(PACKAGENAME)_*_*.changes
	rm -f $(PACKAGENAME)_*_*.deb
