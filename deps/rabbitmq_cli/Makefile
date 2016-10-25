all:
	mix deps.get
	mix deps.compile
	mix escript.build
tests: all
	mix test --trace
plugins: all
	rm -f rabbitmq-plugins
	ln -s rabbitmqctl rabbitmq-plugins
diagnostics: all
	rm -f rabbitmq-diagnostics
	ln -s rabbitmqctl rabbitmq-diagnostics
clean:
	mix clean