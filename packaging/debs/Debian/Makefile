SOURCE_DIST_FILE ?= $(wildcard ../../../rabbitmq-server-*.tar.xz)
ifeq ($(SOURCE_DIST_FILE),)
$(error Cannot find source archive; please specify SOURCE_DIST_FILE)
endif
ifneq ($(words $(SOURCE_DIST_FILE)),1)
$(error Multile source archives found; please specify SOURCE_DIST_FILE)
endif

VERSION ?= $(patsubst rabbitmq-server-%.tar.xz,%,$(notdir $(SOURCE_DIST_FILE)))
ifeq ($(VERSION),)
$(error Cannot determine version; please specify VERSION)
endif

DEBIAN_ORIG_TARBALL = rabbitmq-server_$(VERSION).orig.tar.xz
UNPACKED_DIR = rabbitmq-server-$(VERSION)
PACKAGENAME = rabbitmq-server
SIGNING_KEY_ID = 056E8E56

ifneq "$(UNOFFICIAL_RELEASE)" ""
  SIGNING=-us -uc
else
  SIGNING=-k$(SIGNING_KEY_ID)
endif

unexport DEPS_DIR
unexport ERL_LIBS

all: package
	@:

package: clean
	cp -a $(SOURCE_DIST_FILE) $(DEBIAN_ORIG_TARBALL)
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

	if test "$(PACKAGES_DIR)"; then \
		mkdir -p "$(PACKAGES_DIR)"; \
		mv $(PACKAGENAME)_$(VERSION)* "$(PACKAGES_DIR)"; \
	fi

clean:
	rm -rf $(UNPACKED_DIR)
	rm -f $(PACKAGENAME)_*.tar.gz
	rm -f $(PACKAGENAME)_*.diff.gz
	rm -f $(PACKAGENAME)_*.dsc
	rm -f $(PACKAGENAME)_*_*.changes
	rm -f $(PACKAGENAME)_*_*.deb
