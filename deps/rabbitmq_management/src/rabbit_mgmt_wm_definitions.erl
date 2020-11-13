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

-module(rabbit_mgmt_wm_definitions).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([content_types_accepted/2, allowed_methods/2, accept_json/2]).
-export([accept_multipart/2]).
-export([variances/2]).

-export([apply_defs/4, apply_defs/5]).

-import(rabbit_misc, [pget/2]).

-include("rabbit_mgmt.hrl").
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
   {[{{<<"application">>, <<"json">>, '*'}, accept_json},
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
        VHost ->
            vhost_definitions(ReqData, VHost, Context)
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
        [{users,             rabbit_mgmt_wm_users:users(all)},
         {vhosts,            rabbit_mgmt_wm_vhosts:basic()},
         {permissions,       rabbit_mgmt_wm_permissions:permissions()},
         {topic_permissions, rabbit_mgmt_wm_topic_permissions:topic_permissions()},
         {parameters,        rabbit_mgmt_wm_parameters:basic(ReqData)},
         {global_parameters, rabbit_mgmt_wm_global_parameters:basic()},
         {policies,          rabbit_mgmt_wm_policies:basic(ReqData)},
         {queues,            Qs},
         {exchanges,         Xs},
         {bindings,          Bs}]),
      case rabbit_mgmt_util:qs_val(<<"download">>, ReqData) of
          undefined -> ReqData;
          Filename  -> rabbit_mgmt_util:set_resp_header(
                         <<"Content-Disposition">>,
                         "attachment; filename=" ++
                             binary_to_list(Filename), ReqData)
      end,
      Context).

accept_json(ReqData0, Context) ->
    {ok, Body, ReqData} = rabbit_mgmt_util:read_complete_body(ReqData0),
    accept(Body, ReqData, Context).

vhost_definitions(ReqData, VHost, Context) ->
    %% rabbit_mgmt_wm_<>:basic/1 filters by VHost if it is available
    Xs = [strip_vhost(X) || X <- rabbit_mgmt_wm_exchanges:basic(ReqData),
               export_exchange(X)],
    VQs = [Q || Q <- rabbit_mgmt_wm_queues:basic(ReqData), export_queue(Q)],
    Qs = [strip_vhost(Q) || Q <- VQs],
    QNames = [{pget(name, Q), pget(vhost, Q)} || Q <- VQs],
    Bs = [strip_vhost(B) || B <- rabbit_mgmt_wm_bindings:basic(ReqData),
                            export_binding(B, QNames)],
    {ok, Vsn} = application:get_key(rabbit, vsn),
    Parameters = [rabbit_mgmt_format:parameter(
                    rabbit_mgmt_wm_parameters:fix_shovel_publish_properties(P))
                  || P <- rabbit_runtime_parameters:list(VHost)],
    rabbit_mgmt_util:reply(
      [{rabbit_version, rabbit_data_coercion:to_binary(Vsn)}] ++
          filter(
            [{parameters,  Parameters},
             {policies,    rabbit_mgmt_wm_policies:basic(ReqData)},
             {queues,      Qs},
             {exchanges,   Xs},
             {bindings,    Bs}]),
      case rabbit_mgmt_util:qs_val(<<"download">>, ReqData) of
          undefined -> ReqData;
          Filename  -> rabbit_mgmt_util:set_resp_header(
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
    case rabbit_mgmt_util:qs_val(<<"auth">>, ReqData) of
        undefined -> rabbit_mgmt_util:is_authorized_admin(ReqData, Context);
        Auth      -> is_authorized_qs(ReqData, Context, Auth)
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
    %% At this point the request was fully received.
    %% There is no point in the idle_timeout anymore.
    disable_idle_timeout(ReqData),
    SuccessFun =
        fun() ->
            {true, ReqData, Context}
        end,
    ErrorFun =
        fun(E) ->
            rabbit_log:error("Encountered an error when importing definitions: ~p", [E]),
            rabbit_mgmt_util:bad_request(E, ReqData, Context)
        end,
    case rabbit_mgmt_util:vhost(ReqData) of
        none ->
            apply_defs(Body, Username, SuccessFun, ErrorFun);
        not_found ->
            rabbit_mgmt_util:bad_request(rabbit_data_coercion:to_binary("vhost_not_found"),
                                         ReqData, Context);
        VHost ->
            apply_defs(Body, Username, SuccessFun, ErrorFun, VHost)
    end.

disable_idle_timeout(#{pid := Pid, streamid := StreamID}) ->
    Pid ! {{Pid, StreamID}, {set_options, #{idle_timeout => infinity}}}.

apply_defs(Body, ActingUser, SuccessFun, ErrorFun) ->
    rabbit_log:info("Asked to import definitions. Acting user: ~s", [rabbit_data_coercion:to_binary(ActingUser)]),
    case rabbit_mgmt_util:decode([], Body) of
        {error, E} ->
            ErrorFun(E);
        {ok, _, All} ->
            Version = maps:get(rabbit_version, All, undefined),
            try
                rabbit_log:info("Importing users..."),
                for_all(users,              ActingUser, All,
                        fun(User, _Username) ->
                                rabbit_mgmt_wm_user:put_user(
                                  User,
                                  Version,
                                  ActingUser)
                        end),
                rabbit_log:info("Importing vhosts..."),
                for_all(vhosts,             ActingUser, All, fun add_vhost/2),
                validate_limits(All),
                rabbit_log:info("Importing user permissions..."),
                for_all(permissions,        ActingUser, All, fun add_permission/2),
                rabbit_log:info("Importing topic permissions..."),
                for_all(topic_permissions,  ActingUser, All, fun add_topic_permission/2),
                rabbit_log:info("Importing parameters..."),
                for_all(parameters,         ActingUser, All, fun add_parameter/2),
                rabbit_log:info("Importing global parameters..."),
                for_all(global_parameters,  ActingUser, All, fun add_global_parameter/2),
                rabbit_log:info("Importing policies..."),
                for_all(policies,           ActingUser, All, fun add_policy/2),
                rabbit_log:info("Importing queues..."),
                for_all(queues,             ActingUser, All, fun add_queue/2),
                rabbit_log:info("Importing exchanges..."),
                for_all(exchanges,          ActingUser, All, fun add_exchange/2),
                rabbit_log:info("Importing bindings..."),
                for_all(bindings,           ActingUser, All, fun add_binding/2),
                SuccessFun()
            catch {error, E} -> ErrorFun(format(E));
                  exit:E     -> ErrorFun(format(E))
            end
    end.

apply_defs(Body, ActingUser, SuccessFun, ErrorFun, VHost) ->
    rabbit_log:info("Asked to import definitions for a virtual host. Virtual host: ~p, acting user: ~p",
                    [VHost, ActingUser]),
    case rabbit_mgmt_util:decode([], Body) of
        {error, E} ->
            ErrorFun(E);
        {ok, _, All} ->
            try
                validate_limits(All, VHost),
                rabbit_log:info("Importing parameters..."),
                for_all(parameters,  ActingUser, All, VHost, fun add_parameter/3),
                rabbit_log:info("Importing policies..."),
                for_all(policies,    ActingUser, All, VHost, fun add_policy/3),
                rabbit_log:info("Importing queues..."),
                for_all(queues,      ActingUser, All, VHost, fun add_queue/3),
                rabbit_log:info("Importing exchanges..."),
                for_all(exchanges,   ActingUser, All, VHost, fun add_exchange/3),
                rabbit_log:info("Importing bindings..."),
                for_all(bindings,    ActingUser, All, VHost, fun add_binding/3),
                SuccessFun()
            catch {error, E} -> ErrorFun(format(E));
                  exit:E     -> ErrorFun(format(E))
            end
    end.

format(#amqp_error{name = Name, explanation = Explanation}) ->
    rabbit_data_coercion:to_binary(rabbit_misc:format("~s: ~s", [Name, Explanation]));
format({no_such_vhost, undefined}) ->
    rabbit_data_coercion:to_binary(
      "Virtual host does not exist and is not specified in definitions file.");
format({no_such_vhost, VHost}) ->
    rabbit_data_coercion:to_binary(
      rabbit_misc:format("Please create virtual host \"~s\" prior to importing definitions.",
                         [VHost]));
format({vhost_limit_exceeded, ErrMsg}) ->
    rabbit_data_coercion:to_binary(ErrMsg);
format(E) ->
    rabbit_data_coercion:to_binary(rabbit_misc:format("~p", [E])).

get_all_parts(Req) ->
    get_all_parts(Req, []).

get_all_parts(Req0, Acc) ->
    case cowboy_req:read_part(Req0) of
        {done, Req1} ->
            {Acc, Req1};
        {ok, Headers, Req1} ->
            Name = case cow_multipart:form_data(Headers) of
                       {data, N} -> N;
                       {file, N, _, _} -> N
                   end,
            {ok, Body, Req2} = stream_part_body(Req1, <<>>),
            get_all_parts(Req2, [{Name, Body}|Acc])
    end.

stream_part_body(Req0, Acc) ->
    case cowboy_req:read_part_body(Req0) of
        {more, Data, Req1} ->
            stream_part_body(Req1, <<Acc/binary, Data/binary>>);
        {ok, Data, Req1} ->
            {ok, <<Acc/binary, Data/binary>>, Req1}
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

for_all(Name, ActingUser, All, Fun) ->
    case maps:get(Name, All, undefined) of
        undefined -> ok;
        List      -> [Fun(maps:from_list([{atomise_name(K), V} || {K, V} <- maps:to_list(M)]),
                          ActingUser) ||
                         M <- List, is_map(M)]
    end.

for_all(Name, ActingUser, All, VHost, Fun) ->
    case maps:get(Name, All, undefined) of
        undefined -> ok;
        List      -> [Fun(VHost, maps:from_list([{atomise_name(K), V} || {K, V} <- maps:to_list(M)]),
                          ActingUser) ||
                         M <- List, is_map(M)]
    end.

atomise_name(N) -> rabbit_data_coercion:to_atom(N).

%%--------------------------------------------------------------------

add_parameter(Param, Username) ->
    VHost = maps:get(vhost,     Param, undefined),
    add_parameter(VHost, Param, Username).

add_parameter(VHost, Param, Username) ->
    Comp  = maps:get(component, Param, undefined),
    Key   = maps:get(name,      Param, undefined),
    Term  = maps:get(value,     Param, undefined),
    Result = case is_map(Term) of
        true ->
            %% coerce maps to proplists for backwards compatibility.
            %% See rabbitmq-management#528.
            TermProplist = rabbit_data_coercion:to_proplist(Term),
            rabbit_runtime_parameters:set(VHost, Comp, Key, TermProplist, Username);
        _ ->
            rabbit_runtime_parameters:set(VHost, Comp, Key, Term, Username)
    end,
    case Result of
        ok                -> ok;
        {error_string, E} ->
            S = rabbit_misc:format(" (~s/~s/~s)", [VHost, Comp, Key]),
            exit(rabbit_data_coercion:to_binary(rabbit_mgmt_format:escape_html_tags(E ++ S)))
    end.

add_global_parameter(Param, Username) ->
    Key   = maps:get(name,      Param, undefined),
    Term  = maps:get(value,     Param, undefined),
    case is_map(Term) of
        true ->
            %% coerce maps to proplists for backwards compatibility.
            %% See rabbitmq-management#528.
            TermProplist = rabbit_data_coercion:to_proplist(Term),
            rabbit_runtime_parameters:set_global(Key, TermProplist, Username);
        _ ->
            rabbit_runtime_parameters:set_global(Key, Term, Username)
    end.

add_policy(Param, Username) ->
    VHost = maps:get(vhost, Param, undefined),
    add_policy(VHost, Param, Username).

add_policy(VHost, Param, Username) ->
    Key   = maps:get(name,  Param, undefined),
    case rabbit_policy:set(
           VHost, Key, maps:get(pattern, Param, undefined),
           case maps:get(definition, Param, undefined) of
               undefined -> undefined;
               Def -> rabbit_data_coercion:to_proplist(Def)
           end,
           maps:get(priority, Param, undefined),
           maps:get('apply-to', Param, <<"all">>),
           Username) of
        ok                -> ok;
        {error_string, E} -> S = rabbit_misc:format(" (~s/~s)", [VHost, Key]),
                             exit(rabbit_data_coercion:to_binary(rabbit_mgmt_format:escape_html_tags(E ++ S)))
    end.

add_vhost(VHost, ActingUser) ->
    VHostName = maps:get(name, VHost, undefined),
    VHostTrace = maps:get(tracing, VHost, undefined),
    rabbit_mgmt_wm_vhost:put_vhost(VHostName, VHostTrace, ActingUser).

add_permission(Permission, ActingUser) ->
    rabbit_auth_backend_internal:set_permissions(maps:get(user,      Permission, undefined),
                                                 maps:get(vhost,     Permission, undefined),
                                                 maps:get(configure, Permission, undefined),
                                                 maps:get(write,     Permission, undefined),
                                                 maps:get(read,      Permission, undefined),
                                                 ActingUser).

add_topic_permission(TopicPermission, ActingUser) ->
    rabbit_auth_backend_internal:set_topic_permissions(
        maps:get(user,      TopicPermission, undefined),
        maps:get(vhost,     TopicPermission, undefined),
        maps:get(exchange,  TopicPermission, undefined),
        maps:get(write,     TopicPermission, undefined),
        maps:get(read,      TopicPermission, undefined),
      ActingUser).

add_queue(Queue, ActingUser) ->
    add_queue_int(Queue, r(queue, Queue), ActingUser).

add_queue(VHost, Queue, ActingUser) ->
    add_queue_int(Queue, rv(VHost, queue, Queue), ActingUser).

add_queue_int(Queue, Name, ActingUser) ->
    rabbit_amqqueue:declare(Name,
                            maps:get(durable,                         Queue, undefined),
                            maps:get(auto_delete,                     Queue, undefined),
                            rabbit_mgmt_util:args(maps:get(arguments, Queue, undefined)),
                            none,
                            ActingUser).

add_exchange(Exchange, ActingUser) ->
    add_exchange_int(Exchange, r(exchange, Exchange), ActingUser).

add_exchange(VHost, Exchange, ActingUser) ->
    add_exchange_int(Exchange, rv(VHost, exchange, Exchange), ActingUser).

add_exchange_int(Exchange, Name, ActingUser) ->
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
                            ActingUser).

add_binding(Binding, ActingUser) ->
    DestType = dest_type(Binding),
    add_binding_int(Binding, r(exchange, source, Binding),
                    r(DestType, destination, Binding), ActingUser).

add_binding(VHost, Binding, ActingUser) ->
    DestType = dest_type(Binding),
    add_binding_int(Binding, rv(VHost, exchange, source, Binding),
                    rv(VHost, DestType, destination, Binding), ActingUser).

add_binding_int(Binding, Source, Destination, ActingUser) ->
    rabbit_binding:add(
      #binding{source       = Source,
               destination  = Destination,
               key          = maps:get(routing_key, Binding, undefined),
               args         = rabbit_mgmt_util:args(maps:get(arguments, Binding, undefined))},
      ActingUser).

dest_type(Binding) ->
    list_to_atom(binary_to_list(maps:get(destination_type, Binding, undefined))).

r(Type, Props) -> r(Type, name, Props).

r(Type, Name, Props) ->
    rabbit_misc:r(maps:get(vhost, Props, undefined), Type, maps:get(Name, Props, undefined)).

rv(VHost, Type, Props) -> rv(VHost, Type, name, Props).

rv(VHost, Type, Name, Props) ->
    rabbit_misc:r(VHost, Type, maps:get(Name, Props, undefined)).

%%--------------------------------------------------------------------

validate_limits(All) ->
    case maps:get(queues, All, undefined) of
        undefined -> ok;
        Queues0 ->
            {ok, VHostMap} = filter_out_existing_queues(Queues0),
            maps:fold(fun validate_vhost_limit/3, ok, VHostMap)
    end.

validate_limits(All, VHost) ->
    case maps:get(queues, All, undefined) of
        undefined -> ok;
        Queues0 ->
            Queues1 = filter_out_existing_queues(VHost, Queues0),
            AddCount = length(Queues1),
            validate_vhost_limit(VHost, AddCount, ok)
    end.

filter_out_existing_queues(Queues) ->
    build_filtered_map(Queues, maps:new()).

filter_out_existing_queues(VHost, Queues) ->
    Pred = fun(Queue) ->
               Rec = rv(VHost, queue, <<"name">>, Queue),
               case rabbit_amqqueue:lookup(Rec) of
                   {ok, _} -> false;
                   {error, not_found} -> true
               end
           end,
    lists:filter(Pred, Queues).

build_queue_data(Queue) ->
    VHost = maps:get(<<"vhost">>, Queue, undefined),
    Rec = rv(VHost, queue, <<"name">>, Queue),
    {Rec, VHost}.

build_filtered_map([], AccMap) ->
    {ok, AccMap};
build_filtered_map([Queue|Rest], AccMap0) ->
    {Rec, VHost} = build_queue_data(Queue),
    case rabbit_amqqueue:lookup(Rec) of
        {error, not_found} ->
            AccMap1 = maps_update_with(VHost, fun(V) -> V + 1 end, 1, AccMap0),
            build_filtered_map(Rest, AccMap1);
        {ok, _} ->
            build_filtered_map(Rest, AccMap0)
    end.

%% Copy of maps:with_util/3 from Erlang 20.0.1.
maps_update_with(Key,Fun,Init,Map) when is_function(Fun,1), is_map(Map) ->
    case maps:find(Key,Map) of
        {ok,Val} -> maps:update(Key,Fun(Val),Map);
        error -> maps:put(Key,Init,Map)
    end;
maps_update_with(Key,Fun,Init,Map) ->
    erlang:error(maps_error_type(Map),[Key,Fun,Init,Map]).

%% Copy of maps:error_type/1 from Erlang 20.0.1.
maps_error_type(M) when is_map(M) -> badarg;
maps_error_type(V) -> {badmap, V}.

validate_vhost_limit(VHost, AddCount, ok) ->
    WouldExceed = rabbit_vhost_limit:would_exceed_queue_limit(AddCount, VHost),
    validate_vhost_queue_limit(VHost, AddCount, WouldExceed).

validate_vhost_queue_limit(_VHost, 0, _) ->
    % Note: not adding any new queues so the upload
    % must be update-only
    ok;
validate_vhost_queue_limit(_VHost, _AddCount, false) ->
    % Note: would not exceed queue limit
    ok;
validate_vhost_queue_limit(VHost, AddCount, {true, Limit, QueueCount}) ->
    ErrFmt = "Adding ~B queue(s) to virtual host \"~s\" would exceed the limit of ~B queue(s).~n~nThis virtual host currently has ~B queue(s) defined.~n~nImport aborted!",
    ErrInfo = [AddCount, VHost, Limit, QueueCount],
    ErrMsg = rabbit_misc:format(ErrFmt, ErrInfo),
    exit({vhost_limit_exceeded, ErrMsg}).
