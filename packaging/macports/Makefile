TARBALL_DIR=../../dist
TARBALL=$(notdir $(wildcard $(TARBALL_DIR)/rabbitmq-server-[0-9.]*.tar.gz))
COMMON_DIR=../common
VERSION=$(shell echo $(TARBALL) | sed -e 's:rabbitmq-server-\(.*\)\.tar\.gz:\1:g')

# The URL at which things really get deployed
REAL_WEB_URL=http://www.rabbitmq.com/

DEST=macports/net/rabbitmq-server

dirs:
	mkdir -p $(DEST)/files

$(DEST)/Portfile: Portfile.in
	for algo in md5 sha1 rmd160 ; do \
          checksum=$$(openssl $$algo $(TARBALL_DIR)/$(TARBALL) | awk '{print $$NF}') ; \
	  echo "s|@$$algo@|$$checksum|g" ; \
	done >checksums.sed
	sed -e "s|@VERSION@|$(VERSION)|g;s|@BASE_URL@|$(REAL_WEB_URL)|g" \
	    -f checksums.sed <$^ >$@

macports: dirs $(DEST)/Portfile
	for f in rabbitmq-asroot-script-wrapper rabbitmq-script-wrapper ; do \
	  cp $(COMMON_DIR)/$$f $(DEST)/files ; \
	done
	sed -i -e 's|@SU_RABBITMQ_SH_C@|sudo -u rabbitmq -H /bin/sh -c|' \
	    $(DEST)/files/rabbitmq-script-wrapper
	cp patch-org.macports.rabbitmq-server.plist.diff $(DEST)/files

clean:
	rm -rf $(DEST) checksums.sed
