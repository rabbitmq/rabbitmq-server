-module(amqp10_client_sessions_sup).

-behaviour(supervisor).

%% Private API.
-export([start_link/0]).

%% Supervisor callbacks.
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     transient, 5000, Type, [Mod]}).

%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | ignore | {error, any()}.

start_link() ->
    supervisor:start_link(?MODULE, []).

%% -------------------------------------------------------------------
%% Supervisor callbacks.
%% -------------------------------------------------------------------

init([]) ->
    Template = ?CHILD(session, amqp10_client_session, worker, []),
    {ok, {{simple_one_for_one, 0, 1}, [Template]}}.
