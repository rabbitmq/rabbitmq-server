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
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
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
