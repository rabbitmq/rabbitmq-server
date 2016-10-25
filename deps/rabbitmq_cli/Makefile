all:
	mix deps.get
	mix deps.compile
	mix escript.build
	rm -f escript/rabbitmq-plugins
	ln -s rabbitmqctl escript/rabbitmq-plugins
	rm -f escript/rabbitmq-diagnostics
	ln -s rabbitmqctl escript/rabbitmq-diagnostics
tests: all
	mix test --trace
clean:
	mix clean