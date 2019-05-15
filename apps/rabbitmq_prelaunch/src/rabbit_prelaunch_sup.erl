-module(rabbit_prelaunch_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% `rabbit_prelaunch` does not start a process, it only configures
    %% the node.
    Prelaunch = #{id => prelaunch,
                  start => {rabbit_prelaunch, run_prelaunch_first_phase, []},
                  restart => transient},
    Procs = [Prelaunch],
    {ok, {{one_for_one, 1, 5}, Procs}}.
