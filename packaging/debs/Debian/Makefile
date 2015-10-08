TARBALL_DIR=../../..
TARBALL=$(notdir $(wildcard $(TARBALL_DIR)/rabbitmq-server-[0-9.]*.tar.xz))
VERSION=$(patsubst rabbitmq-server-%.tar.xz,%,$(TARBALL))

DEBIAN_ORIG_TARBALL=$(patsubst rabbitmq-server-%.tar.xz,rabbitmq-server_%.orig.tar.xz,$(TARBALL))
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
	cp -a $(TARBALL_DIR)/$(TARBALL) $(DEBIAN_ORIG_TARBALL)
	tar -Jxf $(DEBIAN_ORIG_TARBALL)
	cp -a debian $(UNPACKED_DIR)
	rsync -a \
		--exclude '.sw?' --exclude '.*.sw?' \
		--exclude '.git*' \
		--delete --delete-excluded \
		debian/ $(UNPACKED_DIR)/debian/
	UNOFFICIAL_RELEASE=$(UNOFFICIAL_RELEASE) VERSION=$(VERSION) ./check-changelog.sh rabbitmq-server $(UNPACKED_DIR)
	cd $(UNPACKED_DIR); GNUPGHOME=$(GNUPG_PATH)/.gnupg dpkg-buildpackage -sa $(SIGNING)
	rm -rf $(UNPACKED_DIR)

clean:
	rm -rf $(UNPACKED_DIR)
	rm -f $(PACKAGENAME)_*.tar.gz
	rm -f $(PACKAGENAME)_*.diff.gz
	rm -f $(PACKAGENAME)_*.dsc
	rm -f $(PACKAGENAME)_*_*.changes
	rm -f $(PACKAGENAME)_*_*.deb
