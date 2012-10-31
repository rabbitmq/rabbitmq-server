TARBALL_SRC_DIR=../../dist
TARBALL_BIN_DIR=../../packaging/generic-unix/
TARBALL_SRC=$(wildcard $(TARBALL_SRC_DIR)/rabbitmq-server-[0-9.]*.tar.gz)
TARBALL_BIN=$(wildcard $(TARBALL_BIN_DIR)/rabbitmq-server-generic-unix-[0-9.]*.tar.gz)
COMMON_DIR=../common
VERSION=$(shell echo $(TARBALL_SRC) | sed -e 's:rabbitmq-server-\(.*\)\.tar\.gz:\1:g')

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
	./make-checksums.sh $(TARBALL_SRC) $(TARBALL_BIN) > checksums.sed
	sed -e "s|@VERSION@|$(VERSION)|g;s|@BASE_URL@|$(REAL_WEB_URL)|g" \
	    -f checksums.sed <$^ >$@
	rm checksums.sed

# The purpose of the intricate substitution below is to set up similar
# environment vars to the ones that su will on Linux.  On OS X, we
# have to use the -m option to su in order to be able to set the shell
# (which for the rabbitmq user would otherwise be /dev/null).  But the
# -m option means that *all* environment vars get preserved.  Erlang
# needs vars such as HOME to be set.  So we have to set them
# explicitly.
macports: dirs $(DEST)/Portfile
	sed -e 's|@SU_RABBITMQ_SH_C@|SHELL=/bin/sh HOME=/var/lib/rabbitmq USER=rabbitmq LOGNAME=rabbitmq PATH="$$(eval `PATH=@MACPORTS_PREFIX@/bin /usr/libexec/path_helper -s`; echo $$PATH)" su -m rabbitmq -c|' \
		$(COMMON_DIR)/rabbitmq-script-wrapper >$(DEST)/files/rabbitmq-script-wrapper
	cp patch-org.macports.rabbitmq-server.plist.diff $(DEST)/files
	if [ -n "$(MACPORTS_USERHOST)" ] ; then \
	  tar cf - -C $(MACPORTS_DIR) . | ssh $(SSH_OPTS) $(MACPORTS_USERHOST) ' \
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
	rm -rf $(MACPORTS_DIR) checksums.sed
