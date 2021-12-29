%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_vhost_restart).

-export([init/2, resource_exists/2, is_authorized/2,
         allowed_methods/2, content_types_accepted/2, accept_content/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"POST">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    VHost = id(ReqData),
    {rabbit_vhost:exists(VHost), ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{'*', accept_content}], ReqData, Context}.

accept_content(ReqData, Context) ->
    VHost = id(ReqData),
    NodeB = rabbit_mgmt_util:id(node, ReqData),
    Node  = binary_to_atom(NodeB, utf8),
    case rabbit_vhost_sup_sup:start_vhost(VHost, Node) of
        {ok, _} ->
            {true, ReqData, Context};
        {error, {already_started, _}} ->
            {true, ReqData, Context};
        {error, Err} ->
            Message = io_lib:format("Request to node ~s failed with ~p",
                                    [Node, Err]),
            rabbit_mgmt_util:bad_request(list_to_binary(Message), ReqData, Context)
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

id(ReqData) ->
    rabbit_mgmt_util:id(vhost, ReqData).
