%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_mgmt_wm_quorum_queue_replicas_add_member).

-export([init/2, resource_exists/2, is_authorized/2, allowed_methods/2,
         content_types_accepted/2, accept_content/2]).
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

resource_exists(ReqData, Context) ->
  {case rabbit_mgmt_wm_queue:queue(ReqData) of
     not_found -> false;
     _         -> true
   end, ReqData, Context}.

content_types_accepted(ReqData, Context) ->
  {[{'*', accept_content}], ReqData, Context}.

accept_content(ReqData, Context) ->
  VHost = rabbit_mgmt_util:vhost(ReqData),
  QName = rabbit_mgmt_util:id(queue, ReqData),
  Res = rabbit_mgmt_util:with_decode(
    [node], ReqData, Context,
    fun([NewReplicaNode], _Body, _ReqData) ->
      rabbit_amqqueue:with(
        rabbit_misc:r(VHost, queue, QName),
        fun(_Q) ->
          rabbit_quorum_queue:add_member(VHost, QName, rabbit_data_coercion:to_atom(NewReplicaNode), ?TIMEOUT)
        end)
    end),
  case Res of
    ok ->
      {true, ReqData, Context};
    {ok, _} ->
      {true, ReqData, Context};
    {error, Reason} ->
      rabbit_mgmt_util:service_unavailable(Reason, ReqData, Context)
  end.


is_authorized(ReqData, Context) ->
    case rabbit_mgmt_features:is_qq_replica_operations_disabled() of
        true ->
            rabbit_mgmt_util:method_not_allowed(<<"Broker settings disallow quorum queue replica operations.">>, ReqData, Context);
        false ->
            rabbit_mgmt_util:is_authorized_admin(ReqData, Context)
    end.
