all:
	mix deps.get
	mix deps.compile
	mix escript.build
tests: all
	mix test
list: all
	cp rabbitmqctl rabbitmqctl_list

