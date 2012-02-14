-module(rabbit_ws_sup).

-behaviour(supervisor).

-export([start_link/0, start_child/1]).
-export([init/1]).

%% --------------------------------------------------------------------------

-spec start_link() -> ignore | {'ok', pid()} | {'error', any()}.
start_link() ->
     supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one, 10, 10},
          [{undefined, {rabbit_ws, start_link, []},
            transient, 5000, worker, [rabbit_ws]}]}}.

start_child(Params) ->
   supervisor:start_child(?MODULE, [Params]).

