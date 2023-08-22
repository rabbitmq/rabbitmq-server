%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_mgmt_wm_quorum_queue_replicas_grow).

-export([init/2, is_authorized/2, allowed_methods/2,
  content_types_accepted/2, resource_exists/2, accept_content/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(TIMEOUT, 30_000).

init(Req, _State) ->
  {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
  {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
  {[<<"POST">>, <<"OPTIONS">>], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
  {[{'*', accept_content}], ReqData, Context}.

resource_exists(ReqData, Context) ->
  case rabbit_mgmt_util:id(node, ReqData) of
    none -> {false, ReqData, Context};
    Node ->
      NodeExists = lists:member(rabbit_data_coercion:to_atom(Node), rabbit_nodes:list_running()),
      {NodeExists, ReqData, Context}
  end.

accept_content(ReqData, Context) ->
  NewReplicaNode = rabbit_mgmt_util:id(node, ReqData),
  rabbit_mgmt_util:with_decode(
    [vhost_pattern, queue_pattern, strategy], ReqData, Context,
    fun([VHPattern, QPattern, Strategy], _Body, _ReqData) ->
      rabbit_quorum_queue:grow(
        rabbit_data_coercion:to_atom(NewReplicaNode),
        VHPattern,
        QPattern,
        rabbit_data_coercion:to_atom(Strategy))
    end),
  {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    case rabbit_mgmt_features:is_qq_replica_operations_disabled() of
        true ->
            rabbit_mgmt_util:method_not_allowed(<<"Broker settings disallow quorum queue replica operations.">>, ReqData, Context);
        false ->
            rabbit_mgmt_util:is_authorized_admin(ReqData, Context)
    end.
