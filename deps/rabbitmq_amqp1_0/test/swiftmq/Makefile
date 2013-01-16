CLIENT_DIR=swiftmq_9_2_5_client
CLIENT_PKG=$(CLIENT_DIR).zip

.PHONY: test

test: build/lib
	ant test

build/lib: $(CLIENT_PKG)
	mkdir -p build/tmp
	unzip -d build/tmp $(CLIENT_PKG)
	mkdir -p build/lib
	mv build/tmp/$(CLIENT_DIR)/jars/*.jar build/lib
	rm -rf build/tmp
	cp lib/*.jar build/lib

$(CLIENT_PKG):
	@echo
	@echo You need $(CLIENT_PKG) to run these tests. Unfortunately we can\'t
	@echo redistribute it. Obtain it from the SwiftMQ website and place it
	@echo in this directory.
	@echo
	@false

clean:
	rm -rf build
