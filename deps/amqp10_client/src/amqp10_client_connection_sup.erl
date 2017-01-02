-module(amqp10_client_connection_sup).

-behaviour(supervisor).

%% Private API.
-export([start_link/2]).

%% Supervisor callbacks.
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     transient, 5000, Type, [Mod]}).

%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

-spec start_link(any(), any()) -> {ok, pid()} | ignore | {error, any()}.

start_link(Addr, Port) ->
    supervisor:start_link(?MODULE, [Addr, Port]).

%% -------------------------------------------------------------------
%% Supervisor callbacks.
%% -------------------------------------------------------------------

init(Args) ->
    ReaderSpec = ?CHILD(reader, amqp10_client_frame_reader,
                        worker, [self() | Args]),
    ConnectionSpec = ?CHILD(connection, amqp10_client_connection,
                            worker, [self()]),
    SessionsSupSpec = ?CHILD(sessions, amqp10_client_sessions_sup,
                             supervisor, []),
    {ok, {{one_for_all, 0, 1}, [ReaderSpec,
                                ConnectionSpec,
                                SessionsSupSpec]}}.
