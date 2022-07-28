-module(trust_store_http_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
	Specs = [],
	Flags = #{
		strategy  => one_for_one,
		intensity => 1,
		period    => 5
	},
	{ok, {Flags, Specs}}.
