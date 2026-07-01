%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_amqp_sole_conn).

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("include/rabbit_khepri.hrl").
-include_lib("amqp10_common/include/amqp10_sole_conn.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-define(CLOSE_EXISTING_TIMEOUT, 30_000).
-define(ALIVENESS_RPC_TIMEOUT, 1_000).

-export([init/0,
         acquire/4,
         refuse_connection_error/0,
         close_existing_connection_error/0]).

%% for testing
-export([conn/1,
         try_put/3,
         conn_path/2,
         close_connection/1]).

-type vhost() :: binary().
-type container_id() :: binary().

-record(conn, {pid :: pid()}).

init() ->
    _ = rabbit_khepri:adv_put(kill_connection_sproc_path(),
                              fun kill_connection_sproc/1),

    EventFilter = khepri_evf:tree(kill_connection_sproc_trigger_pattern(),
                                  #{on_actions => [update]}),

    ok = khepri:register_trigger(
           rabbit_khepri:get_store_id(),
           amqp10_sole_conn_kill_connection,
           EventFilter,
           kill_connection_sproc_path()).

-spec acquire(none | enforcement_policy(), vhost(), container_id(), pid()) ->
    ok | {error, refuse_connection | close_existing}.
acquire(none, _, _, _) ->
    ok;
acquire(refuse_connection = Plcy, VHost, ContainerId, ConnPid) ->
    Path = conn_path(VHost, ContainerId),

    Opts = default_options(ConnPid),
    Payload = #conn{pid = ConnPid},
    case rabbit_khepri:adv_create(Path, Payload, Opts) of
        {ok, _} ->
            %% no node yet, accept
            %% node should clean itself when the connection is closed
            ok;
        {error, {khepri, mismatching_node, #{node_props := #{data := ExistingConn}}}} ->
            case check_conn(ExistingConn) of
                true ->
                    {error, refuse_connection};
                _ ->
                    case try_put(Path, ExistingConn, Payload) of
                        ok ->
                            ok;
                        _ ->
                            {error, refuse_connection}
                    end
            end;
        {error, Reason} ->
            ?LOG_INFO("Unexpected Khepri error for connection '~ts' "
                      "in vhost ~ts (policy ~ts): ~p. Refusing connection.",
                      [ContainerId, VHost, Plcy, Reason]),
            {error, refuse_connection}
    end;
acquire(close_existing = Plcy, VHost, ContainerId, ConnPid) ->
    Path = conn_path(VHost, ContainerId),
    Opts = default_options(ConnPid),
    Payload = #conn{pid = ConnPid},
    case rabbit_khepri:adv_put(Path, Payload, Opts) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            ?LOG_INFO("Unexpected Khepri error for connection '~ts' "
                      "in vhost ~ts (policy ~ts): ~p. Refusing connection.",
                      [ContainerId, VHost, Plcy, Reason]),
            {error, refuse_connection}
    end.

refuse_connection_error() ->
    %% the error field of close MUST have an error with the condition field
    %% of error being invalid-field and the info field of error having
    %% the symbol key invalid-field taking the symbol value container-id.
    %% [sole conn 3.2.1]
    amqp_error(
      ?V_1_0_AMQP_ERROR_INVALID_FIELD,
      <<"The container-id is already bound to an "
        "active exclusive connection.">>,
      {?V_1_0_AMQP_ERROR_INVALID_FIELD, {symbol, <<"container-id">>}}).

close_existing_connection_error() ->
    %% "The existing connection MUST be closed with the error field of
    %% close having the condition field of error being resource-locked.
    %% Further the info field of error MUST contain the symbol key
    %% sole-connection-enforcement taking the boolean value true"
    %% [sole conn 3.2.1]
    amqp_error(?V_1_0_AMQP_ERROR_RESOURCE_LOCKED,
               <<"Connection closed because another "
                 "connection with the same container-id "
                 "was established (sole connection "
                 "enforcement).">>,
               {?SOLE_CONN_ENFORCEMENT, {boolean, true}}).

%% --------------------------------------------------------------
%% Internals
%% --------------------------------------------------------------
 
default_options(Pid) ->
    #{keep_while => Pid}.

check_conn(#conn{pid = Pid}) ->
    Node = node(Pid),
    case Node =:= node() of
        true ->
            is_process_alive(Pid);
        false ->
            try erpc:call(Node, erlang, is_process_alive, [Pid],
                          ?ALIVENESS_RPC_TIMEOUT) of
                Result ->
                    Result
            catch
                error:{erpc, _Reason} ->
                    %% If the RPC times out, the node is down, or unreachable,
                    %% we assume the process is dead to allow the new connection.
                    false
            end
    end.

try_put(Path,
        #conn{pid = ExistingPid} = ExistingConn,
        #conn{pid = NewPid} = NewConn) ->
    Opts = default_options(NewPid),
    case khepri:compare_and_swap(store_id(), Path, ExistingConn, NewConn,
                                 Opts) of
        ok ->
            ok;
        {error, Error} ->
            ?LOG_WARNING("Unexpected Khepri error for connection '~p', "
                         "old conn ~p, new conn ~p. Error is ~p.",
                         [Path, ExistingPid, NewPid, Error]),
            error
    end.

close_connection(#conn{pid = Pid}) ->
    Error = close_existing_connection_error(),
    rabbit_networking:close_connection(Pid, Error, ?CLOSE_EXISTING_TIMEOUT).

store_id() ->
    rabbit_khepri:get_store_id().


kill_connection_sproc(#khepri_trigger{type = tree,
                                      event = #{change := update,
                                                old_node_props := #{data := #conn{pid = Pid}}}}) ->
    % Pid ! close_sole_conn_enforcement,
    exit(Pid, sole_conn_enforcement),
    ok;
kill_connection_sproc(Props) ->
    ?LOG_WARNING("Unexpected event for sole_conn stored procedure, "
                 "connection will not be instructed to close. Event: ~p",
                 Props),
    ok.

amqp_error(Cond, Desc, Info) ->
    #'v1_0.error'{
       condition = Cond,
       description = {utf8, Desc},
       info = {map, [Info]}}.

%% for testing
conn(Pid) ->
    #conn{pid = Pid}.

%% --------------------------------------------------------------
%% Khepri paths
%% --------------------------------------------------------------

conn_path(VHost, ContainerId)
  when ?IS_KHEPRI_PATH_CONDITION(VHost) andalso
       ?IS_KHEPRI_PATH_CONDITION(ContainerId) ->
    ?RABBITMQ_KHEPRI_VHOST_PATH(VHost, [amqp10_sole_conn, ContainerId]).

kill_connection_sproc_path() ->
    ?RABBITMQ_KHEPRI_ROOT_PATH([amqp10_sole_conn, kill_connection]).

kill_connection_sproc_trigger_pattern() ->
    ?RABBITMQ_KHEPRI_VHOST_PATH(?KHEPRI_WILDCARD_STAR_STAR,
                                [amqp10_sole_conn,
                                 ?KHEPRI_WILDCARD_STAR_STAR]).
