OPENSSL=openssl

OBJECTS= server/key.pem \
		 server/cert.pem \
		 ca/cacerts.pem \
		 client/key.pem \
		 client/cert.pem

all: $(OBJECTS)
	@echo "New Certs and Keys have been generated"
	@echo " Please remember to use ca/cacerts.pem "
	@echo " server/cert.pem and server/key.pem in "
	@echo " your rabbitmq-server ssl configuration"

server/key.pem: 
	mkdir -p server
	$(OPENSSL) genrsa -out $@ 1024

server/cert.pem: server/key.pem
	$(OPENSSL) req -new -x509 -key $< -nodes -sha1 -days 365 -batch -out $@

ca/cacerts.pem: server/cert.pem
	mkdir -p ca
	cp $< $@

client/key.pem:
	mkdir -p client
	$(OPENSSL) genrsa -out $@ 1024

client/cert.pem: client/key.pem
	$(OPENSSL) req -new -x509 -key $< -nodes -sha1 -days 365 -batch -out $@

clean:
	rm -rf server
	rm -rf ca
	rm -rf client
