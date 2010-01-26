-module(rabbit_shovel_sup).
-behaviour(supervisor3).

-export([start_link/0, init/1]).

start_link() ->
  supervisor3:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 1, 1},
          [{rabbit_shovel_worker,
            {rabbit_shovel_worker, start_link, []},
            {permanent, 5},
            16#ffffffff,
            worker,
            [rabbit_shovel_worker]}
          ]}}.
