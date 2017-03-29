-module(amqp10_client_connections_sup).

-behaviour(supervisor).

%% Private API.
-export([start_link/0,
         stop_child/1]).

%% Supervisor callbacks.
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     temporary, infinity, Type, [Mod]}).

%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

stop_child(Pid) ->
    supervisor:terminate_child({local, ?MODULE}, Pid).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% -------------------------------------------------------------------
%% Supervisor callbacks.
%% -------------------------------------------------------------------

init([]) ->
    Template = ?CHILD(connection_sup, amqp10_client_connection_sup,
                      supervisor, []),
    {ok, {{simple_one_for_one, 0, 1}, [Template]}}.
