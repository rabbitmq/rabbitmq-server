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
-export([content_types_accepted/2, allowed_methods/2, accept_content/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(MAGIC_USER, <<"!!magic_user!!">>).
-define(MAGIC_PASSWORD, ?MAGIC_USER).
-define(FRAMING, rabbit_framing_amqp_0_9_1).

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'PUT'], ReqData, Context}.

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

accept_content(ReqData, Context) ->
    rabbit_mgmt_util:with_decode(
      [users, vhosts, permissions, queues, exchanges, bindings],
      ReqData, Context,
      fun([Users, VHosts, Permissions, Queues, Exchanges, Bindings]) ->
              rabbit_access_control:add_user(?MAGIC_USER, ?MAGIC_PASSWORD),
              for_all(Users,       fun add_user/1),
              for_all(VHosts,      fun add_vhost/1),
              for_all(Permissions, fun add_permission/1),
              for_all(Queues,      fun add_queue/1),
              for_all(Exchanges,   fun add_exchange/1),
              for_all(Bindings,    fun add_binding/1),
              rabbit_access_control:delete_user(?MAGIC_USER),
              {true, ReqData, Context}
      end).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

rw_state() ->
    [{users,       [name, password, administrator]},
     {vhosts,      [name]},
     {permissions, [user, vhost, configure, write, read, scope]},
     {queues,      [name, vhost, durable, auto_delete, arguments]},
     {exchanges,   [name, vhost, type, durable, auto_delete, arguments]},
     {bindings,    [exchange, vhost, queue, routing_key, arguments]}].

filter(Items) ->
    [filter_items(N, V, proplists:get_value(N, rw_state())) || {N, V} <- Items].

filter_items(Name, List, Allowed) ->
    {Name, [filter_item(I, Allowed) || I <- List]}.

filter_item(Item, Allowed) ->
    [{K, Fact} || {K, Fact} <- Item, lists:member(K, Allowed)].

%%--------------------------------------------------------------------

for_all(List, Fun) ->
    [Fun(atomise_names(I)) || {struct, I} <- List].

atomise_names(I) ->
    [{list_to_atom(binary_to_list(K)), V} || {K, V} <- I].

%%--------------------------------------------------------------------

add_user(User) ->
    rabbit_mgmt_wm_user:put_user(pget(name,          User),
                                 pget(password,      User),
                                 pget(administrator, User)).

add_vhost(VHost) ->
    VHostName = pget(name, VHost),
    rabbit_mgmt_wm_vhost:put_vhost(VHostName),
    rabbit_access_control:set_permissions(
      <<"client">>, ?MAGIC_USER, VHostName, <<".*">>, <<".*">>, <<".*">>).

add_permission(Permission) ->
    rabbit_access_control:set_permissions(pget(scope,     Permission),
                                          pget(user,      Permission),
                                          pget(vhost,     Permission),
                                          pget(configure, Permission),
                                          pget(write,     Permission),
                                          pget(read,      Permission)).

add_queue(Queue) ->
    amqp_request('queue.declare', map_name(queue, Queue)).

add_exchange(Exchange) ->
    amqp_request('exchange.declare', map_name(exchange, Exchange)).

add_binding(Binding) ->
    amqp_request('queue.bind', Binding).

pget(Key, List) ->
    proplists:get_value(Key, List).

amqp_request(MethodName, Props) ->
    Params = #amqp_params{username = ?MAGIC_USER,
                          password = ?MAGIC_PASSWORD,
                          virtual_host = pget(vhost, Props)},
    {ok, Conn} = amqp_connection:start(direct, Params),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:call(Ch, props_to_method(MethodName, Props)),
    amqp_channel:close(Ch),
    amqp_connection:close(Conn).

props_to_method(Method, Props) ->
    FieldNames = ?FRAMING:method_fieldnames(Method),
    {Res, _Idx} = lists:foldl(
                    fun (K, {R, Idx}) ->
                            NewR = case proplists:get_value(K, Props) of
                                       undefined -> R;
                                       V         -> setelement(Idx, R, V)
                                   end,
                            {NewR, Idx + 1}
                    end, {?FRAMING:method_record(Method), 2},
                    FieldNames),
    Res.

map_name(K, Props) ->
    [{K, pget(name, Props)}|Props].
