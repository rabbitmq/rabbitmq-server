%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_wm_all_configuration).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

rw_state() ->
    [{users,       [name, password, administrator]},
     {vhosts,      [name]},
     {permissions, [user, vhost, confiure, write, read, scope]},
     {queues,      [name, vhost, durable, auto_delete, arguments]},
     {exchanges,   [name, vhost, type, durable, auto_delete, arguments]},
     {bindings,    [exchange, vhost, queue, routing_key, arguments]}].

filter(Items) ->
    [filter_items(N, V, proplists:get_value(N, rw_state())) || {N, V} <- Items].

filter_items(Name, List, Allowed) ->
    {Name, [filter_item(I, Allowed) || I <- List]}.

filter_item(Item, Allowed) ->
    [{K, Fact} || {K, Fact} <- Item, lists:member(K, Allowed)].

to_json(ReqData, Context) ->
    All =
        [{users,       rabbit_mgmt_wm_users:users()},
         {vhosts,      [[{name, N}]
                        || N <- rabbit_access_control:list_vhosts()]},
         {permissions, rabbit_mgmt_wm_permissions:perms()},
         {queues,      [rabbit_mgmt_format:queue(Q)
                        || Q <- rabbit_mgmt_wm_queues:queues(ReqData)]},
         {exchanges,   [rabbit_mgmt_format:exchange(X)
                        || X <- rabbit_mgmt_wm_exchanges:exchanges(ReqData)]},
         {bindings,    [rabbit_mgmt_format:binding(B)
                        || B <- rabbit_mgmt_wm_bindings:bindings(ReqData)]}],
    ReqData1 =
        case wrq:get_qs_value("mode", ReqData) of
            "download" -> wrq:set_resp_header(
                            "Content-disposition",
                            "attachment; filename=rabbit.json", ReqData);
            _          -> ReqData
        end,
    rabbit_mgmt_util:reply(filter(All), ReqData1, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).
