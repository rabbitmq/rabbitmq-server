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
-export([content_types_accepted/2, allowed_methods/2, accept_json/2]).
-export([post_is_create/2, create_path/2, accept_multipart/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(FRAMING, rabbit_framing_amqp_0_9_1).

%%--------------------------------------------------------------------

init(_Config) ->
    MagicUsername = rabbit_guid:binstring_guid("magic_user_"),
    MagicPassword = rabbit_guid:binstring_guid("magic_password_"),
    {ok, #context{extra = {MagicUsername, MagicPassword}}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_json},
     {"multipart/form-data", accept_multipart}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'POST'], ReqData, Context}.

post_is_create(ReqData, Context) ->
    {true, ReqData, Context}.

create_path(ReqData, Context) ->
    {"dummy", ReqData, Context}.

to_json(ReqData, Context) ->
    Queues = [rabbit_mgmt_format:queue(Q)
              || Q <- rabbit_mgmt_wm_queues:queues(ReqData), export_queue(Q)],
    Exclusives = [Q#amqqueue.name
                  || Q <- rabbit_mgmt_wm_queues:queues(ReqData),
                     not export_queue(Q)],
    {ok, Vsn} = application:get_key(rabbit, vsn),
    rabbit_mgmt_util:reply(
      [{rabbit_version, Vsn}] ++
      filter(
        [{users,       rabbit_mgmt_wm_users:users()},
         {vhosts,      [[{name, N}]
                        || N <- rabbit_access_control:list_vhosts()]},
         {permissions, rabbit_mgmt_wm_permissions:perms()},
         {queues,      Queues},
         {exchanges,   [rabbit_mgmt_format:exchange(X)
                        || X <- rabbit_mgmt_wm_exchanges:exchanges(ReqData),
                           export_exchange(X)]},
         {bindings,    [rabbit_mgmt_format:binding(B)
                        || B <- rabbit_mgmt_wm_bindings:bindings(ReqData),
                           export_binding(B, Exclusives)]}]),
      case wrq:get_qs_value("mode", ReqData) of
          "download" -> wrq:set_resp_header(
                          "Content-disposition",
                          "attachment; filename=rabbit.json", ReqData);
          _          -> ReqData
      end,
      Context).

accept_json(ReqData, Context) ->
    accept(wrq:req_body(ReqData), ReqData, Context).

accept_multipart(ReqData, Context) ->
    Parts = webmachine_multipart:get_all_parts(
              wrq:req_body(ReqData),
              webmachine_multipart:find_boundary(ReqData)),
    Redirect = get_part("redirect", Parts),
    Json = get_part("file", Parts),
    Resp = {Res, _, _} = accept(Json, ReqData, Context),
    case Res of
        true ->
            ReqData1 =
                case Redirect of
                    unknown -> ReqData;
                    _       -> rabbit_mgmt_util:redirect(Redirect, ReqData)
                end,
            {true, ReqData1, Context};
        _ ->
            Resp
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

accept(Body, ReqData,
       Context = #context{extra = {Username, Password}}) ->
    rabbit_mgmt_util:with_decode(
      [users, vhosts, permissions, queues, exchanges, bindings],
      Body, ReqData, Context,
      fun([Users, VHosts, Permissions, Queues, Exchanges, Bindings]) ->
              rabbit_access_control:add_user(Username, Password),
              [allow_user(Username, V)
               || V <- rabbit_access_control:list_vhosts()],
              Res =
                  rabbit_mgmt_util:with_amqp_error_handling(
                    ReqData, Context,
                    fun() ->
                            for_all(Users,       fun add_user/2,       Context),
                            for_all(VHosts,      fun add_vhost/2,      Context),
                            for_all(Permissions, fun add_permission/2, Context),
                            for_all(Queues,      fun add_queue/2,      Context),
                            for_all(Exchanges,   fun add_exchange/2,   Context),
                            for_all(Bindings,    fun add_binding/2,    Context),
                            {true, ReqData, Context}
                    end),
              rabbit_access_control:delete_user(Username),
              Res
      end).

get_part(Name, Parts) ->
    Filtered = [Value || {N, _Meta, Value} <- Parts, N == Name],
    case Filtered of
        []  -> unknown;
        [F] -> F
    end.

export_queue(#amqqueue{ exclusive_owner = none }) ->
    true;
export_queue(_) ->
    false.

export_binding(#binding { exchange_name = #resource{ name = Exchange },
                          queue_name = Queue }, ExclusiveQueues) ->
    not lists:member(Queue, ExclusiveQueues) andalso
        export_exchange_name(Exchange).

export_exchange(Exchange) ->
    R = proplists:get_value(name, Exchange),
    Name = R#resource.name,
    export_exchange_name(Name).

export_exchange_name(Name) ->
    not lists:prefix("amq.", binary_to_list(Name)) andalso
        Name =/= <<>>.

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

for_all(List, Fun, Context) ->
    [Fun([{atomise_name(K), clean_value(V)} || {K, V} <- I], Context) ||
        {struct, I} <- List].

atomise_name(N) ->
    list_to_atom(binary_to_list(N)).

clean_value({struct, L}) -> L;
clean_value(A)           -> A.

allow_user(Username, VHost) ->
    rabbit_access_control:set_permissions(
      <<"client">>, Username, VHost, <<".*">>, <<".*">>, <<".*">>).

%%--------------------------------------------------------------------

add_user(User, _Context) ->
    rabbit_mgmt_wm_user:put_user(pget(name,          User),
                                 pget(password,      User),
                                 pget(administrator, User)).

add_vhost(VHost, #context{ extra = {Username, _} }) ->
    VHostName = pget(name, VHost),
    rabbit_mgmt_wm_vhost:put_vhost(VHostName),
    allow_user(Username, VHostName).

add_permission(Permission, _Context) ->
    rabbit_access_control:set_permissions(pget(scope,     Permission),
                                          pget(user,      Permission),
                                          pget(vhost,     Permission),
                                          pget(configure, Permission),
                                          pget(write,     Permission),
                                          pget(read,      Permission)).

add_queue(Queue, Context) ->
    amqp_request('queue.declare', map_name(queue, Queue), Context).

add_exchange(Exchange, Context) ->
    amqp_request('exchange.declare', map_name(exchange, Exchange), Context).

add_binding(Binding, Context) ->
    amqp_request('queue.bind', Binding, Context).

pget(Key, List) ->
    proplists:get_value(Key, List).

amqp_request(MethodName, Props, #context{ extra = {Username, Password} }) ->
    Params = #amqp_params{username = Username,
                          password = Password,
                          virtual_host = pget(vhost, Props)},
    {ok, Conn} = amqp_connection:start(direct, Params),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:call(Ch, props_to_method(MethodName, Props)),
    amqp_channel:close(Ch),
    amqp_connection:close(Conn).

props_to_method(Method, Props) ->
    Props1 = add_args_types(Props),
    FieldNames = ?FRAMING:method_fieldnames(Method),
    {Res, _Idx} = lists:foldl(
                    fun (K, {R, Idx}) ->
                            NewR = case proplists:get_value(K, Props1) of
                                       undefined -> R;
                                       V         -> setelement(Idx, R, V)
                                   end,
                            {NewR, Idx + 1}
                    end, {?FRAMING:method_record(Method), 2},
                    FieldNames),
    Res.

map_name(K, Props) ->
    [{K, pget(name, Props)} | Props].

add_args_types(Props) ->
    Args = proplists:get_value(arguments, Props),
    [{arguments, rabbit_mgmt_util:args(Args)}|
     proplists:delete(arguments, Props)].
