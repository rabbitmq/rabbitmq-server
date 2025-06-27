-module(rabbit_cli_backend_sup).

-behaviour(supervisor).

-export([start_link/0,
         start_backend/3,
         which_backends/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, none).

start_backend(Context, Caller, GroupLeader) ->
    supervisor:start_child(?MODULE, [Context, Caller, GroupLeader]).

which_backends() ->
    Children = supervisor:which_children(?MODULE),
    [Child || {_ChildId, Child, _Type, _Modules} <- Children].

init(_Args) ->
    SupFlags = #{strategy => simple_one_for_one},
    BackendChild = #{id => rabbit_cli_backend,
                     start => {rabbit_cli_backend, start_link, []},
                     restart => temporary},
    {ok, {SupFlags, [BackendChild]}}.
