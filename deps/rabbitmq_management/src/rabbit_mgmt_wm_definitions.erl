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
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_wm_definitions).

-export([init/3, rest_init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([content_types_accepted/2, allowed_methods/2, accept_json/2]).
-export([accept_multipart/2]).
-export([variances/2]).

-export([apply_defs/4]).

-import(rabbit_misc, [pget/2, pget/3]).

-include("rabbit_mgmt.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init(_, _, _) -> {upgrade, protocol, cowboy_rest}.

rest_init(Req, _Config) ->
    {ok, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{<<"application/json">>, accept_json},
     {{<<"multipart">>, <<"form-data">>, '*'}, accept_multipart}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"POST">>, <<"OPTIONS">>], ReqData, Context}.

to_json(ReqData, Context) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        none ->
            all_definitions(ReqData, Context);
        not_found ->
            rabbit_mgmt_util:bad_request(rabbit_data_coercion:to_binary("vhost_not_found"),
                                         ReqData, Context);
        _VHost ->
            vhost_definitions(ReqData, Context)
    end.

all_definitions(ReqData, Context) ->
    Xs = [X || X <- rabbit_mgmt_wm_exchanges:basic(ReqData),
               export_exchange(X)],
    Qs = [Q || Q <- rabbit_mgmt_wm_queues:basic(ReqData),
               export_queue(Q)],
    QNames = [{pget(name, Q), pget(vhost, Q)} || Q <- Qs],
    Bs = [B || B <- rabbit_mgmt_wm_bindings:basic(ReqData),
               export_binding(B, QNames)],
    {ok, Vsn} = application:get_key(rabbit, vsn),
    rabbit_mgmt_util:reply(
      [{rabbit_version, rabbit_data_coercion:to_binary(Vsn)}] ++
      filter(
        [{users,             rabbit_mgmt_wm_users:users()},
         {vhosts,            rabbit_mgmt_wm_vhosts:basic()},
         {permissions,       rabbit_mgmt_wm_permissions:permissions()},
         {topic_permissions, rabbit_mgmt_wm_topic_permissions:topic_permissions()},
         {parameters,        rabbit_mgmt_wm_parameters:basic(ReqData)},
         {global_parameters, rabbit_mgmt_wm_global_parameters:basic()},
         {policies,          rabbit_mgmt_wm_policies:basic(ReqData)},
         {queues,            Qs},
         {exchanges,         Xs},
         {bindings,          Bs}]),
      case cowboy_req:qs_val(<<"download">>, ReqData) of
          {undefined, _} -> ReqData;
          {Filename, _}  -> rabbit_mgmt_util:set_resp_header(
                         <<"Content-Disposition">>,
                         "attachment; filename=" ++
                             binary_to_list(Filename), ReqData)
      end,
      Context).

accept_json(ReqData0, Context) ->
    {ok, Body, ReqData} = cowboy_req:body(ReqData0),
    accept(Body, ReqData, Context).

vhost_definitions(ReqData, Context) ->
    %% rabbit_mgmt_wm_<>:basic/1 filters by VHost if it is available
    Xs = [strip_vhost(X) || X <- rabbit_mgmt_wm_exchanges:basic(ReqData),
               export_exchange(X)],
    VQs = [Q || Q <- rabbit_mgmt_wm_queues:basic(ReqData), export_queue(Q)],
    Qs = [strip_vhost(Q) || Q <- VQs],
    QNames = [{pget(name, Q), pget(vhost, Q)} || Q <- VQs],
    Bs = [strip_vhost(B) || B <- rabbit_mgmt_wm_bindings:basic(ReqData),
                            export_binding(B, QNames)],
    {ok, Vsn} = application:get_key(rabbit, vsn),
    rabbit_mgmt_util:reply(
      [{rabbit_version, rabbit_data_coercion:to_binary(Vsn)}] ++
          filter(
            [{policies,    rabbit_mgmt_wm_policies:basic(ReqData)},
             {queues,      Qs},
             {exchanges,   Xs},
             {bindings,    Bs}]),
      case cowboy_req:qs_val(<<"download">>, ReqData) of
          {undefined, _} -> ReqData;
          {Filename, _}  -> rabbit_mgmt_util:set_resp_header(
                         "Content-Disposition",
                         "attachment; filename=" ++
                             binary_to_list(Filename), ReqData)
      end,
      Context).

accept_multipart(ReqData0, Context) ->
    {Parts, ReqData} = get_all_parts(ReqData0),
    Redirect = get_part(<<"redirect">>, Parts),
    Json = get_part(<<"file">>, Parts),
    Resp = {Res, _, _} = accept(Json, ReqData, Context),
    case {Res, Redirect} of
        {true, unknown} -> {true, ReqData, Context};
        {true, _}       -> {{true, Redirect}, ReqData, Context};
        _               -> Resp
    end.

is_authorized(ReqData, Context) ->
    case cowboy_req:qs_val(<<"auth">>, ReqData) of
        {undefined, _} -> rabbit_mgmt_util:is_authorized_admin(ReqData, Context);
        {Auth, _}      -> is_authorized_qs(ReqData, Context, Auth)
    end.

%% Support for the web UI - it can't add a normal "authorization"
%% header for a file download.
is_authorized_qs(ReqData, Context, Auth) ->
    case rabbit_web_dispatch_util:parse_auth_header("Basic " ++ Auth) of
        [Username, Password] -> rabbit_mgmt_util:is_authorized_admin(
                                  ReqData, Context, Username, Password);
        _                    -> {?AUTH_REALM, ReqData, Context}
    end.

%%--------------------------------------------------------------------

accept(Body, ReqData, Context = #context{user = #user{username = Username}}) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        none ->
            apply_defs(Body, Username, fun() -> {true, ReqData, Context} end,
                       fun(E) -> rabbit_mgmt_util:bad_request(E, ReqData, Context) end);
        not_found ->
            rabbit_mgmt_util:bad_request(rabbit_data_coercion:to_binary("vhost_not_found"),
                                         ReqData, Context);
        VHost ->
            apply_defs(Body, Username, fun() -> {true, ReqData, Context} end,
                       fun(E) -> rabbit_mgmt_util:bad_request(E, ReqData, Context) end,
                       VHost)
    end.

apply_defs(Body, Username, SuccessFun, ErrorFun) ->
    case rabbit_mgmt_util:decode([], Body) of
        {error, E} ->
            ErrorFun(E);
        {ok, _, All} ->
            Version = maps:get(rabbit_version, All, undefined),
            try
                for_all(users,              Username, All,
                        fun(User, _Username) ->
                                rabbit_mgmt_wm_user:put_user(
                                  User,
                                  Version,
                                  Username)
                        end),
                for_all(vhosts,             Username, All, fun add_vhost/2),
                for_all(permissions,        Username, All, fun add_permission/2),
                for_all(topic_permissions,  Username, All, fun add_topic_permission/2),
                for_all(parameters,         Username, All, fun add_parameter/2),
                for_all(global_parameters,  Username, All, fun add_global_parameter/2),
                for_all(policies,           Username, All, fun add_policy/2),
                for_all(queues,             Username, All, fun add_queue/2),
                for_all(exchanges,          Username, All, fun add_exchange/2),
                for_all(bindings,           Username, All, fun add_binding/2),
                SuccessFun()
            catch {error, E} -> ErrorFun(format(E));
                  exit:E     -> ErrorFun(format(E))
            end
    end.

apply_defs(Body, Username, SuccessFun, ErrorFun, VHost) ->
    case rabbit_mgmt_util:decode([], Body) of
        {error, E} ->
            ErrorFun(E);
        {ok, _, All} ->
            try
                for_all(policies,    Username, All, VHost, fun add_policy/3),
                for_all(queues,      Username, All, VHost, fun add_queue/3),
                for_all(exchanges,   Username, All, VHost, fun add_exchange/3),
                for_all(bindings,    Username, All, VHost, fun add_binding/3),
                SuccessFun()
            catch {error, E} -> ErrorFun(format(E));
                  exit:E     -> ErrorFun(format(E))
            end
    end.

format(#amqp_error{name = Name, explanation = Explanation}) ->
    rabbit_data_coercion:to_binary(rabbit_misc:format("~s: ~s", [Name, Explanation]));
format(E) ->
    rabbit_data_coercion:to_binary(rabbit_misc:format("~p", [E])).

get_all_parts(ReqData) ->
    get_all_parts(ReqData, []).

get_all_parts(ReqData0, Acc) ->
    case cowboy_req:part(ReqData0) of
        {done, ReqData} ->
            {Acc, ReqData};
        {ok, Headers, ReqData1} ->
            Name = case cow_multipart:form_data(Headers) of
                {data, N} -> N;
                {file, N, _, _, _} -> N
            end,
            {ok, Body, ReqData} = cowboy_req:part_body(ReqData1),
            get_all_parts(ReqData, [{Name, Body}|Acc])
    end.

get_part(Name, Parts) ->
    case lists:keyfind(Name, 1, Parts) of
        false -> unknown;
        {_, Value} -> Value
    end.

export_queue(Queue) ->
    pget(owner_pid, Queue) == none.

export_binding(Binding, Qs) ->
    Src      = pget(source,           Binding),
    Dest     = pget(destination,      Binding),
    DestType = pget(destination_type, Binding),
    VHost    = pget(vhost,            Binding),
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
    [{users,              [name, password_hash, hashing_algorithm, tags]},
     {vhosts,             [name]},
     {permissions,        [user, vhost, configure, write, read]},
     {topic_permissions,  [user, vhost, exchange, write, read]},
     {parameters,         [vhost, component, name, value]},
     {global_parameters,  [name, value]},
     {policies,           [vhost, name, pattern, definition, priority, 'apply-to']},
     {queues,             [name, vhost, durable, auto_delete, arguments]},
     {exchanges,          [name, vhost, type, durable, auto_delete, internal,
                           arguments]},
     {bindings,           [source, vhost, destination, destination_type, routing_key,
                           arguments]}].

filter(Items) ->
    [filter_items(N, V, proplists:get_value(N, rw_state())) || {N, V} <- Items].

filter_items(Name, List, Allowed) ->
    {Name, [filter_item(I, Allowed) || I <- List]}.

filter_item(Item, Allowed) ->
    [{K, Fact} || {K, Fact} <- Item, lists:member(K, Allowed)].

strip_vhost(Item) ->
    lists:keydelete(vhost, 1, Item).

%%--------------------------------------------------------------------

for_all(Name, Username, All, Fun) ->
    case maps:get(Name, All, undefined) of
        undefined -> ok;
        List      -> [Fun(maps:from_list([{atomise_name(K), V} || {K, V} <- maps:to_list(M)]),
                          Username) ||
                         M <- List, is_map(M)]
    end.

for_all(Name, Username, All, VHost, Fun) ->
    case maps:get(Name, All, undefined) of
        undefined -> ok;
        List      -> [Fun(VHost, maps:from_list([{atomise_name(K), V} || {K, V} <- maps:to_list(M)]),
                          Username) ||
                         M <- List, is_map(M)]
    end.

atomise_name(N) -> rabbit_data_coercion:to_atom(N).

%%--------------------------------------------------------------------

add_parameter(Param, Username) ->
    VHost = maps:get(vhost,     Param, undefined),
    Comp  = maps:get(component, Param, undefined),
    Key   = maps:get(name,      Param, undefined),
    Term  = maps:get(value,     Param, undefined),
    case rabbit_runtime_parameters:set(VHost, Comp, Key, Term, Username) of
        ok                -> ok;
        {error_string, E} -> S = rabbit_misc:format(" (~s/~s/~s)", [VHost, Comp, Key]),
                             exit(rabbit_data_coercion:to_binary(rabbit_mgmt_format:escape_html_tags(E ++ S)))
    end.

add_global_parameter(Param, Username) ->
    Key   = maps:get(name,      Param, undefined),
    Term  = maps:get(value,     Param, undefined),
    rabbit_runtime_parameters:set_global(Key, Term, Username).

add_policy(Param, Username) ->
    VHost = maps:get(vhost, Param, undefined),
    add_policy(VHost, Param, Username).

add_policy(VHost, Param, Username) ->
    Key   = maps:get(name,  Param, undefined),
    case rabbit_policy:set(
           VHost, Key, maps:get(pattern, Param, undefined),
           case maps:get(definition, Param, undefined) of
               undefined -> undefined;
               Def -> maps:to_list(Def)
           end,
           maps:get(priority, Param, undefined),
           maps:get('apply-to', Param, <<"all">>),
           Username) of
        ok                -> ok;
        {error_string, E} -> S = rabbit_misc:format(" (~s/~s)", [VHost, Key]),
                             exit(rabbit_data_coercion:to_binary(rabbit_mgmt_format:escape_html_tags(E ++ S)))
    end.

add_vhost(VHost, Username) ->
    VHostName = maps:get(name, VHost, undefined),
    VHostTrace = maps:get(tracing, VHost, undefined),
    rabbit_mgmt_wm_vhost:put_vhost(VHostName, VHostTrace, Username).

add_permission(Permission, Username) ->
    rabbit_auth_backend_internal:set_permissions(maps:get(user,      Permission, undefined),
                                                 maps:get(vhost,     Permission, undefined),
                                                 maps:get(configure, Permission, undefined),
                                                 maps:get(write,     Permission, undefined),
                                                 maps:get(read,      Permission, undefined),
                                                 Username).

add_topic_permission(TopicPermission, Username) ->
    rabbit_auth_backend_internal:set_topic_permissions(
        maps:get(user,      TopicPermission, undefined),
        maps:get(vhost,     TopicPermission, undefined),
        maps:get(exchange,  TopicPermission, undefined),
        maps:get(write,     TopicPermission, undefined),
        maps:get(read,      TopicPermission, undefined),
      Username).

add_queue(Queue, Username) ->
    add_queue_int(Queue, r(queue, Queue), Username).

add_queue(VHost, Queue, Username) ->
    add_queue_int(Queue, rv(VHost, queue, Queue), Username).

add_queue_int(Queue, Name, Username) ->
    rabbit_amqqueue:declare(Name,
                            maps:get(durable,                         Queue, undefined),
                            maps:get(auto_delete,                     Queue, undefined),
                            rabbit_mgmt_util:args(maps:get(arguments, Queue, undefined)),
                            none,
                            Username).

add_exchange(Exchange, Username) ->
    add_exchange_int(Exchange, r(exchange, Exchange), Username).

add_exchange(VHost, Exchange, Username) ->
    add_exchange_int(Exchange, rv(VHost, exchange, Exchange), Username).

add_exchange_int(Exchange, Name, Username) ->
    Internal = case maps:get(internal, Exchange, undefined) of
                   undefined -> false; %% =< 2.2.0
                   I         -> I
               end,
    rabbit_exchange:declare(Name,
                            rabbit_exchange:check_type(maps:get(type, Exchange, undefined)),
                            maps:get(durable,                         Exchange, undefined),
                            maps:get(auto_delete,                     Exchange, undefined),
                            Internal,
                            rabbit_mgmt_util:args(maps:get(arguments, Exchange, undefined)),
                            Username).

add_binding(Binding, Username) ->
    DestType = dest_type(Binding),
    add_binding_int(Binding, r(exchange, source, Binding),
                    r(DestType, destination, Binding), Username).

add_binding(VHost, Binding, Username) ->
    DestType = dest_type(Binding),
    add_binding_int(Binding, rv(VHost, exchange, source, Binding),
                    rv(VHost, DestType, destination, Binding), Username).

add_binding_int(Binding, Source, Destination, Username) ->
    rabbit_binding:add(
      #binding{source       = Source,
               destination  = Destination,
               key          = maps:get(routing_key, Binding, undefined),
               args         = rabbit_mgmt_util:args(maps:get(arguments, Binding, undefined))},
      Username).

dest_type(Binding) ->
    list_to_atom(binary_to_list(maps:get(destination_type, Binding, undefined))).

r(Type, Props) -> r(Type, name, Props).

r(Type, Name, Props) ->
    rabbit_misc:r(maps:get(vhost, Props, undefined), Type, maps:get(Name, Props, undefined)).

rv(VHost, Type, Props) -> rv(VHost, Type, name, Props).

rv(VHost, Type, Name, Props) ->
    rabbit_misc:r(VHost, Type, maps:get(Name, Props, undefined)).
