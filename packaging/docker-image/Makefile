GENERIC_UNIX_ARCHIVE ?= $(wildcard $(PACKAGES_DIR)/rabbitmq-server-generic-unix-*.tar.xz)

ifneq ($(filter-out clean,$(MAKECMDGOALS)),)
ifeq ($(GENERIC_UNIX_ARCHIVE),)
$(error Cannot find generic-unix archive; please specify GENERIC_UNIX_ARCHIVE)
endif
ifneq ($(words $(SOURCE_DIST_FILE)),1)
$(error Multiple generic-unix archives found; please specify GENERIC_UNIX_ARCHIVE)
endif

VERSION ?= $(patsubst rabbitmq-server-generic-unix-%.tar.xz,%,$(notdir $(GENERIC_UNIX_ARCHIVE)))
ifeq ($(VERSION),)
$(error Cannot determine version; please specify VERSION)
endif
endif

OTP_VERSION ?= 23.2.4
OTP_SHA256 ?= e72aa084907e0f34f932cf00caa33aba93147b0a7c9c35569d6bd1c402f532de

all: dist

dist:
	xzcat $(GENERIC_UNIX_ARCHIVE) | tar xvf -
	echo docker build --pull \
	  --build-arg PGP_KEYSERVER=pgpkeys.uk \
	  --build-arg OTP_VERSION=$(OTP_VERSION) \
	  --build-arg OTP_SHA256=$(OTP_SHA256) \
	  --build-arg RABBITMQ_VERSION=$(VERSION) \
	  --build-arg RABBITMQ_BUILD=rabbitmq_server-$(VERSION) \
	  --tag pivotalrabbitmq/rabbitmq:$(VERSION) \
	  . \
	&& docker push pivotalrabbitmq/rabbitmq:$(VERSION)

clean:
	rm -rf rabbitmq_server-*
