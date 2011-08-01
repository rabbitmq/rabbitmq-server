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
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_all_configuration).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).
-export([content_types_accepted/2, allowed_methods/2, accept_json/2]).
-export([post_is_create/2, create_path/2, accept_multipart/2]).

-import(rabbit_misc, [pget/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------
init(_Config) -> {ok, #context{}}.

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
    Xs = [X || X <- rabbit_mgmt_wm_exchanges:exchanges(ReqData),
               export_exchange(X)],
    Qs = [Q || Q <- rabbit_mgmt_wm_queues:queues(ReqData),
               export_queue(Q)],
    QNames = [{pget(name, Q), pget(vhost, Q)} || Q <- Qs],
    Bs = [B || B <- rabbit_mgmt_wm_bindings:bindings(ReqData),
               export_binding(B, QNames)],
    {ok, Vsn} = application:get_key(rabbit, vsn),
    rabbit_mgmt_util:reply(
      [{rabbit_version, list_to_binary(Vsn)}] ++
      filter(
        [{users,       rabbit_mgmt_wm_users:users()},
         {vhosts,      rabbit_mgmt_wm_vhosts:vhosts()},
         {permissions, rabbit_mgmt_wm_permissions:permissions()},
         {queues,      Qs},
         {exchanges,   Xs},
         {bindings,    Bs}]),
      case wrq:get_qs_value("download", ReqData) of
          undefined -> ReqData;
          Filename  -> wrq:set_resp_header(
                         "Content-Disposition",
                         "attachment; filename=" ++
                             mochiweb_util:unquote(Filename), ReqData)
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

accept(Body, ReqData, Context) ->
    rabbit_mgmt_util:with_decode(
      [users, vhosts, permissions, queues, exchanges, bindings],
      Body, ReqData, Context,
      fun([Users, VHosts, Permissions, Queues, Exchanges, Bindings], _) ->
              try
                  for_all(Users,       fun add_user/1),
                  for_all(VHosts,      fun add_vhost/1),
                  for_all(Permissions, fun add_permission/1),
                  for_all(Queues,      fun add_queue/1),
                  for_all(Exchanges,   fun add_exchange/1),
                  for_all(Bindings,    fun add_binding/1),
                  {true, ReqData, Context}
              catch
                  exit:E ->
                      rabbit_mgmt_util:bad_request(E, ReqData, Context)
              end
      end).

get_part(Name, Parts) ->
    Filtered = [Value || {N, _Meta, Value} <- Parts, N == Name],
    case Filtered of
        []  -> unknown;
        [F] -> F
    end.

export_queue(Queue) ->
    pget(owner_pid, Queue) == none.

export_binding(Binding, Qs) ->
    Src      = pget(source, Binding),
    Dest     = pget(destination, Binding),
    DestType = pget(destination_type, Binding),
    VHost    = pget(vhost, Binding),
    Src =/= <<"">>
        andalso
          ( (DestType =:= queue andalso lists:member({Dest, VHost}, Qs))
            orelse (DestType =:= exchange andalso Dest =/= <<"">>) ).

export_exchange(Exchange) ->
    export_name(pget(name, Exchange)).

export_name(<<>>)                 -> false;
export_name(<<"amq.", _/binary>>) -> false;
export_name(_Name)                -> true.

%%--------------------------------------------------------------------

rw_state() ->
    [{users,       [name, password_hash, tags]},
     {vhosts,      [name]},
     {permissions, [user, vhost, configure, write, read]},
     {queues,      [name, vhost, durable, auto_delete, arguments]},
     {exchanges,   [name, vhost, type, durable, auto_delete, internal,
                    arguments]},
     {bindings,    [source, vhost, destination, destination_type, routing_key,
                    arguments]}].

filter(Items) ->
    [filter_items(N, V, proplists:get_value(N, rw_state())) || {N, V} <- Items].

filter_items(Name, List, Allowed) ->
    {Name, [filter_item(I, Allowed) || I <- List]}.

filter_item(Item, Allowed) ->
    [{K, Fact} || {K, Fact} <- Item, lists:member(K, Allowed)].

%%--------------------------------------------------------------------

for_all(List, Fun) ->
    [Fun([{atomise_name(K), clean_value(V)} || {K, V} <- I]) ||
        {struct, I} <- List].

atomise_name(N) ->
    list_to_atom(binary_to_list(N)).

clean_value({struct, L}) -> L;
clean_value(A)           -> A.

%%--------------------------------------------------------------------

add_user(User) ->
    rabbit_mgmt_wm_user:put_user(User).

add_vhost(VHost) ->
    VHostName = pget(name, VHost),
    rabbit_mgmt_wm_vhost:put_vhost(VHostName).

add_permission(Permission) ->
    rabbit_auth_backend_internal:set_permissions(pget(user,      Permission),
                                                 pget(vhost,     Permission),
                                                 pget(configure, Permission),
                                                 pget(write,     Permission),
                                                 pget(read,      Permission)).

add_queue(Queue) ->
    rabbit_amqqueue:declare(r(queue,                              Queue),
                            pget(durable,                         Queue),
                            pget(auto_delete,                     Queue),
                            rabbit_mgmt_util:args(pget(arguments, Queue)),
                            none).

add_exchange(Exchange) ->
    Internal = case pget(internal, Exchange) of
                   undefined -> false; %% =< 2.2.0
                   I         -> I
               end,
    rabbit_exchange:declare(r(exchange,                           Exchange),
                            rabbit_exchange:check_type(pget(type, Exchange)),
                            pget(durable,                         Exchange),
                            pget(auto_delete,                     Exchange),
                            Internal,
                            rabbit_mgmt_util:args(pget(arguments, Exchange))).

add_binding(Binding) ->
    DestType = list_to_atom(binary_to_list(pget(destination_type, Binding))),
    rabbit_binding:add(
      #binding{source       = r(exchange, source,                   Binding),
               destination  = r(DestType, destination,              Binding),
               key          = pget(routing_key,                     Binding),
               args         = rabbit_mgmt_util:args(pget(arguments, Binding))}).

r(Type, Props) ->
    r(Type, name, Props).

r(Type, Name, Props) ->
    rabbit_misc:r(pget(vhost, Props), Type, pget(Name, Props)).
