OPENSSL=openssl
VALID_DAYS=1

ifndef DIR
DIR := .
endif

OBJECTS= $(DIR)/server/key.pem \
	 $(DIR)/server/cert.pem \
	 $(DIR)/ca/cacerts.pem \
	 $(DIR)/ca/cacerts.cer \
	 $(DIR)/client/key.pem \
	 $(DIR)/client/cert.pem

all: $(OBJECTS)
	@echo "New Certs and Keys have been generated"
	@echo " Please remember to use ca/cacerts.pem "
	@echo " server/cert.pem and server/key.pem in "
	@echo " your rabbitmq-server ssl configuration"
	@echo
	@echo " Also, you should import ca/cacerts.cer"
	@echo " for your mono Trust store"

$(DIR)/server/key.pem: 
	mkdir -p $(DIR)/server
	$(OPENSSL) genrsa -out $@ 1024

$(DIR)/server/cert.pem: $(DIR)/server/key.pem
	$(OPENSSL) req -new -x509 -key $< -nodes -sha1 -days $(VALID_DAYS) -batch -out $@

$(DIR)/ca/cacerts.pem: $(DIR)/server/cert.pem
	mkdir -p $(DIR)/ca
	cp $< $@

$(DIR)/ca/cacerts.cer: $(DIR)/ca/cacerts.pem
	$(OPENSSL) x509 -in $< -out $@ -outform DER

$(DIR)/client/key.pem:
	mkdir -p $(DIR)/client
	$(OPENSSL) genrsa -out $@ 1024

$(DIR)/client/cert.pem: $(DIR)/client/key.pem
	$(OPENSSL) req -new -x509 -key $< -nodes -sha1 -days $(VALID_DAYS) -batch -out $@

clean:
	rm -rf $(DIR)/server
	rm -rf $(DIR)/ca
	rm -rf $(DIR)/client
