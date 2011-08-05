VERSION=0.0.0
ZIP=../windows/rabbitmq-server-windows-$(VERSION)

dist: rabbitmq-$(VERSION).nsi rabbitmq_server-$(VERSION)
	makensis -V2 rabbitmq-$(VERSION).nsi

rabbitmq-$(VERSION).nsi: rabbitmq_nsi.in
	sed \
	    -e 's|%%VERSION%%|$(VERSION)|' \
	    $< > $@

rabbitmq_server-$(VERSION):
	unzip -q $(ZIP)

clean:
	rm -rf rabbitmq-*.nsi rabbitmq_server-* rabbitmq-server-*.exe
