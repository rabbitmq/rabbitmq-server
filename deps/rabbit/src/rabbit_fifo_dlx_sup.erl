-module(rabbit_fifo_dlx_sup).

-behaviour(supervisor).

-rabbit_boot_step({?MODULE,
                   [{description, "supervisor of quorum queue dead-letter workers"},
                    {mfa,         {rabbit_sup, start_supervisor_child, [?MODULE]}},
                    {requires,    kernel_ready},
                    {enables,     core_initialized}]}).

%% supervisor callback
-export([init/1]).
%% client API
-export([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 1,
                 period => 5},
    Worker = rabbit_fifo_dlx_worker,
    ChildSpec = #{id => Worker,
                  start => {Worker, start_link, []},
                  type => worker,
                  modules => [Worker]},
    {ok, {SupFlags, [ChildSpec]}}.
