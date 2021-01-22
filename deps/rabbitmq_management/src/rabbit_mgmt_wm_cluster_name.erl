%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_cluster_name).

-export([init/2, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{'*', accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"PUT">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {true, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(
      [{name, rabbit_nodes:cluster_name()}], ReqData, Context).

accept_content(ReqData0, Context = #context{user = #user{username = Username}}) ->
    rabbit_mgmt_util:with_decode(
      [name], ReqData0, Context, fun([Name], _, ReqData) ->
                                        rabbit_nodes:set_cluster_name(
                                          as_binary(Name), Username),
                                        {true, ReqData, Context}
                                end).

is_authorized(ReqData, Context) ->
    case cowboy_req:method(ReqData) of
        <<"PUT">> -> rabbit_mgmt_util:is_authorized_admin(ReqData, Context);
        _         -> rabbit_mgmt_util:is_authorized(ReqData, Context)
    end.

as_binary(Val) when is_binary(Val) ->
    Val;
as_binary(Val) when is_list(Val) ->
    list_to_binary(Val).
