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

-export([acquire/4, release/2]).

%% for testing
-export([conn/1, try_put/3, conn_path/2]).

-type vhost() :: binary().
-type container_id() :: binary().

-record(conn, {pid :: pid()}).

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
            end,
            {error, refuse_connection};
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
        {ok, #{data := ExistingConn}} ->
            close_connection(ExistingConn),
            ok;
        {ok, _} ->
            ok;
        {error, Reason} ->
            ?LOG_INFO("Unexpected Khepri error for connection '~ts' "
                      "in vhost ~ts (policy ~ts): ~p. Refusing connection.",
                      [ContainerId, VHost, Plcy, Reason]),
            {error, refuse_connection}
    end.

-spec release(vhost(), container_id()) -> ok.
release(VHost, ContainerId) ->
    Path = conn_path(VHost, ContainerId),
    case rabbit_khepri:adv_delete(Path) of
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.

%% --------------------------------------------------------------
%% Internals
%% --------------------------------------------------------------

default_options(Pid) ->
    #{keep_while => Pid}.

check_conn(#conn{pid = Pid}) ->
    %% TODO make aliveness check more robust
    %% it should work local and non-local PIDs
    is_process_alive(Pid).

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
    %% TODO close connection with appropriate error/condition
    %% see soleconn spec 3.2.1
    %% should be async or with low timeout (timeout related to the connection?)
    rabbit_networking:close_connection(Pid, "sole conn").

store_id() ->
    rabbit_khepri:get_store_id().

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
