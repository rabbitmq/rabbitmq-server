%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Plugin.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2012 GoPivotal, Inc.  All rights reserved.
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
