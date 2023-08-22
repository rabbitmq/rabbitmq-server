%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_mgmt_wm_quorum_queue_replicas_shrink).

-export([init/2, is_authorized/2, allowed_methods/2,
  content_types_accepted/2, delete_resource/2, delete_completed/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

init(Req, _State) ->
  {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
  {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
  {[<<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
  {[{'*', accept_content}], ReqData, Context}.

delete_resource(ReqData, Context) ->
  NodeToRemove = rabbit_mgmt_util:id(node, ReqData),
  _ = rabbit_quorum_queue:shrink_all(rabbit_data_coercion:to_atom(NodeToRemove)),
  {true, ReqData, Context}.

delete_completed(ReqData, Context) ->
  %% return 202 Accepted since this is an inherently asynchronous operation
  {false, ReqData, Context}.

is_authorized(ReqData, Context) ->
    case rabbit_mgmt_features:is_qq_replica_operations_disabled() of
        true ->
            rabbit_mgmt_util:method_not_allowed(<<"Broker settings disallow quorum queue replica operations.">>, ReqData, Context);
        false ->
            rabbit_mgmt_util:is_authorized_admin(ReqData, Context)
    end.
