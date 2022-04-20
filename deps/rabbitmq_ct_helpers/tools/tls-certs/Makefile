ifndef DIR
$(error DIR must be specified)
endif

PASSWORD ?= changeme
HOSTNAME := $(shell if [ "$$(uname)" = Darwin ]; then hostname -s; else hostname; fi)

# Verbosity.

V ?= 0

verbose_0 = @
verbose_2 = set -x;
verbose = $(verbose_$(V))

gen_verbose_0 = @echo " GEN   " $@;
gen_verbose_2 = set -x;
gen_verbose = $(gen_verbose_$(V))

openssl_output_0 = 2>/dev/null
openssl_output = $(openssl_output_$(V))

.PRECIOUS: %/testca/cacert.pem
.PHONY: all testca server client clean

all: server client
	@:

testca: $(DIR)/testca/cacert.pem

server: TARGET = server
server: $(DIR)/server/cert.pem
	@:

client: TARGET = client
client: $(DIR)/client/cert.pem
	@:

$(DIR)/testca/cacert.pem:
	$(gen_verbose) mkdir -p $(dir $@)
	$(verbose) { ( cd $(dir $@) && \
	    mkdir -p certs private && \
	    chmod 700 private && \
	    echo 01 > serial && \
	    :> index.txt && \
	    sed -e 's/@HOSTNAME@/$(HOSTNAME)/g' $(CURDIR)/openssl.cnf.in > $(CURDIR)/openssl.cnf && \
	    openssl req -x509 -config $(CURDIR)/openssl.cnf -newkey rsa:2048 -days 365 \
	      -out cacert.pem -outform PEM -subj /CN=MyTestCA/L=$$$$/ -nodes && \
	    openssl x509 -in cacert.pem -out cacert.cer -outform DER ) $(openssl_output) \
	  || (rm -rf $(dir $@) && false); }

$(DIR)/%/cert.pem: $(DIR)/testca/cacert.pem
	$(gen_verbose) mkdir -p $(DIR)/$(TARGET)
	$(verbose) { ( cd $(DIR)/$(TARGET) && \
	    openssl genrsa -out key.pem 2048 && \
	    openssl req -new -key key.pem -out req.pem -outform PEM \
	      -subj /C=UK/ST=England/CN=$(HOSTNAME)/O=$(TARGET)/L=$$$$/ -nodes && \
	    cd ../testca && \
	    sed -e 's/@HOSTNAME@/$(HOSTNAME)/g' $(CURDIR)/openssl.cnf.in > $(CURDIR)/openssl.cnf && \
	    openssl ca -config $(CURDIR)/openssl.cnf -in ../$(TARGET)/req.pem -out \
	      ../$(TARGET)/cert.pem -notext -batch -extensions \
	      $(TARGET)_ca_extensions && \
	    cd ../$(TARGET) && \
	    openssl pkcs12 -export -out keycert.p12 -in cert.pem -inkey key.pem \
	      -passout pass:$(PASSWORD) ) $(openssl_output) || (rm -rf $(DIR)/$(TARGET) && false); }

clean:
	rm -rf $(DIR)/testca
	rm -rf $(DIR)/server
	rm -rf $(DIR)/client
