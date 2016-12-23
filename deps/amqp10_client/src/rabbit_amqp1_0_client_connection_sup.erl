-module(rabbit_amqp1_0_client_connection_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/2]).
-export([start_reader/3]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     transient, 5000, Type, [Mod]}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
-spec start_link(any(), any()) -> {ok, pid()} | ignore | {error, any()}.
start_link(Addr, Port) ->
    supervisor:start_link(?MODULE, [Addr, Port]).

start_reader(SupPid, ConnPid, Endpoint) ->
    % ReaderName = list_to_atom(atom_to_list(ConnName) ++ "_frame_reader"),
    % SupName = list_to_atom(atom_to_list(ConnName) ++ "_sup"),
    Spec = ?CHILD(reader, rabbit_amqp1_0_client_frame_reader,
                  worker, [ConnPid, Endpoint]),
    supervisor:start_child(SupPid, Spec).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init(Args) ->
    ConnSpec = [?CHILD(connection, rabbit_amqp1_0_client_connection, worker,
                       [self() | Args])],
    % {ok, {{one_for_one, 5, 10}, []}}.
    {ok, {{one_for_all, 5, 10}, ConnSpec}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
