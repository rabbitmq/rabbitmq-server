# Check before changing this - do other versions build on platforms we care
# about? Neither 0.2 nor 0.5 build on a Mac for example.
PROTON_VER=0.4
PROTON_DIR=qpid-proton-$(PROTON_VER)
PROTON_TARBALL=$(PROTON_DIR).tar.gz
PROTON_URL=http://apache.mirror.anlx.net/qpid/proton/$(PROTON_VER)/$(PROTON_TARBALL)

deps: proton

proton: $(PROTON_TARBALL)
	rm -rf tmp
	mkdir tmp
	tar xvzf $(PROTON_TARBALL) -C tmp
	cd tmp/$(PROTON_DIR) && mkdir build && cd build && cmake -DCMAKE_INSTALL_PREFIX=/not-going-to-install .. && make all
	cp tmp/$(PROTON_DIR)/build/proton-c/proton .

$(PROTON_TARBALL):
	wget $(PROTON_URL)

clean:
	rm -rf $(PROTON_DIR) $(PROTON_TARBALL) proton tmp
