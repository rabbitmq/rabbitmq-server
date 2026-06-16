%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_amqp_sole_conn).

-include_lib("khepri/include/khepri.hrl").
-include("include/rabbit_khepri.hrl").
-include_lib("amqp10_common/include/amqp10_sole_conn.hrl").

-export([acquire/4, release/2]).

-type vhost() :: binary().
-type container_id() :: binary().

-spec acquire(none | enforcement_policy(), vhost(), container_id(), pid()) ->
    ok | {error, refuse_connection}.
acquire(none, _, _, _) ->
    ok;
acquire(_, VHost, ContainerId, ConnectionPid) ->
    Path = khepri_sole_conn_path(VHost, ContainerId),

    Opts = #{keep_while => ConnectionPid},
    case rabbit_khepri:adv_create(Path, ConnectionPid, Opts) of
        {ok, _} ->
            ok;
        {error, {khepri, mismatching_node, #{node_props := #{data := _ExistingPid}}}} ->
            %% The container ID is already claimed by another connection
            {error, refuse_connection};
        {error, Reason} ->
            {error, Reason}
    end.

-spec release(vhost(), container_id()) -> ok.
release(VHost, ContainerId) ->
    Path = khepri_sole_conn_path(VHost, ContainerId),
    case rabbit_khepri:adv_delete(Path) of
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.

%% --------------------------------------------------------------
%% Khepri paths
%% --------------------------------------------------------------

khepri_sole_conn_path(VHost, ContainerId)
  when ?IS_KHEPRI_PATH_CONDITION(VHost) andalso
       ?IS_KHEPRI_PATH_CONDITION(ContainerId) ->
    ?RABBITMQ_KHEPRI_VHOST_PATH(VHost, [amqp10_sole_conn, ContainerId]).
