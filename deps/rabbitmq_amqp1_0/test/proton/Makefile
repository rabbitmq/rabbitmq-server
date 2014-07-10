PROTON_VER=0.7
PROTON_DIR=qpid-proton-$(PROTON_VER)
PROTON_TARBALL=$(PROTON_DIR).tar.gz
PROTON_URL=http://www.mirrorservice.org/sites/ftp.apache.org/qpid/proton/$(PROTON_VER)/$(PROTON_TARBALL)

.PHONY: test

test: build/lib
	ant test

build/lib: $(PROTON_TARBALL)
	mkdir -p build/tmp
	tar xvz -C build/tmp -f $(PROTON_TARBALL)
	cd build/tmp/$(PROTON_DIR)/proton-j && mvn package
	mkdir -p build/lib
	cp build/tmp/$(PROTON_DIR)/proton-j/target/proton-j-$(PROTON_VER).jar build/lib
	cp ../lib-java/*.jar build/lib

clean:
	rm -rf build $(PROTON_TARBALL)

$(PROTON_TARBALL):
	wget $(PROTON_URL)
