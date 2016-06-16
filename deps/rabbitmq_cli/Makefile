all:
	mix deps.get
	mix deps.compile
	mix escript.build
	MIX_EXS=mix_list.exs mix escript.build
tests: all
	mix test
list:
	MIX_EXS=mix_list.exs mix escript.build
