all:
	mix deps.get
	mix deps.compile
	mix escript.build
tests: all
	mix test
plugins: all
	ln -s rabbitmqctl rabbitmq-plugins

