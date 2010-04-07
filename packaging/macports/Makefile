TARBALL_DIR=../../dist
TARBALL=$(notdir $(wildcard $(TARBALL_DIR)/rabbitmq-server-[0-9.]*.tar.gz))
COMMON_DIR=../common
VERSION=$(shell echo $(TARBALL) | sed -e 's:rabbitmq-server-\(.*\)\.tar\.gz:\1:g')

# The URL at which things really get deployed
REAL_WEB_URL=http://www.rabbitmq.com/

# The user@host for an OSX machine with macports installed, which is
# used to generate the macports index files.  That step will be
# skipped if this variable is not set.  If you do set it, you might
# also want to set SSH_OPTS, which allows adding ssh options, e.g. to
# specify a key that will get into the OSX machine without a
# passphrase.
MACPORTS_USERHOST=

MACPORTS_DIR=macports
DEST=$(MACPORTS_DIR)/net/rabbitmq-server

all: macports

dirs:
	mkdir -p $(DEST)/files

$(DEST)/Portfile: Portfile.in
	for algo in md5 sha1 rmd160 ; do \
          checksum=$$(openssl $$algo $(TARBALL_DIR)/$(TARBALL) | awk '{print $$NF}') ; \
	  echo "s|@$$algo@|$$checksum|g" ; \
	done >checksums.sed
	sed -e "s|@VERSION@|$(VERSION)|g;s|@BASE_URL@|$(REAL_WEB_URL)|g" \
	    -f checksums.sed <$^ >$@
	rm checksums.sed

macports: dirs $(DEST)/Portfile
	for f in rabbitmq-asroot-script-wrapper rabbitmq-script-wrapper ; do \
	  cp $(COMMON_DIR)/$$f $(DEST)/files ; \
	done
	sed -i -e 's|@SU_RABBITMQ_SH_C@|SHELL=/bin/sh su -m rabbitmq -c|' \
	    $(DEST)/files/rabbitmq-script-wrapper
	cp patch-org.macports.rabbitmq-server.plist.diff $(DEST)/files
	if [ -n "$(MACPORTS_USERHOST)" ] ; then \
	  tar cf - -C $(MACPORTS_DIR) . | ssh $(SSH_OPTS) lshift@macrabbit ' \
	    d="/tmp/mkportindex.$$$$" ; \
	    mkdir $$d \
	      && cd $$d \
	      && tar xf - \
	      && /opt/local/bin/portindex -a -o . >/dev/null \
	      && tar cf - . \
	      && cd \
	      && rm -rf $$d' \
	    | tar xf - -C $(MACPORTS_DIR) ; \
	fi

clean:
	rm -rf $(DEST) checksums.sed
