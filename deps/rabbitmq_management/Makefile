include ../umbrella.mk

dev: ebin/rabbitmq_management.app

ebin/rabbitmq_management.app: ebin/rabbitmq_management.app.in
	escript ../generate_app $< $@ ./src

