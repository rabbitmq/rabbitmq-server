%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_definitions).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([content_types_accepted/2, allowed_methods/2, accept_json/2]).
-export([accept_multipart/2]).
-export([variances/2]).

-export([apply_defs/3, apply_defs/5]).

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
    Vsn = rabbit:base_product_version(),
    ProductName = rabbit:product_name(),
    ProductVersion = rabbit:product_version(),
    rabbit_mgmt_util:reply(
      [{rabbit_version, rabbit_data_coercion:to_binary(Vsn)},
       {rabbitmq_version, rabbit_data_coercion:to_binary(Vsn)},
       {product_name, rabbit_data_coercion:to_binary(ProductName)},
       {product_version, rabbit_data_coercion:to_binary(ProductVersion)}] ++
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
          Filename  ->
              HeaderVal = "attachment; filename=" ++ binary_to_list(Filename),
              rabbit_mgmt_util:set_resp_header(<<"Content-Disposition">>, HeaderVal, ReqData)
      end,
      Context).

accept_multipart(ReqData0, Context) ->
    {Parts, ReqData} = get_all_parts(ReqData0),
    Redirect = get_part(<<"redirect">>, Parts),
    Payload = get_part(<<"file">>, Parts),
    Resp = {Res, _, _} = accept(Payload, ReqData, Context),
    case {Res, Redirect} of
        {true, unknown} -> {true, ReqData, Context};
        {true, _}       -> {{true, Redirect}, ReqData, Context};
        _               -> Resp
    end.

is_authorized(ReqData, Context) ->
    case rabbit_mgmt_util:qs_val(<<"auth">>, ReqData) of
        undefined ->
            case rabbit_mgmt_util:qs_val(<<"token">>, ReqData) of
                undefined ->
                    rabbit_mgmt_util:is_authorized_admin(ReqData, Context);
                Token ->
                    rabbit_mgmt_util:is_authorized_admin(ReqData, Context, Token)
            end;
        Auth ->
            is_authorized_qs(ReqData, Context, Auth)
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

decode(<<"">>) ->
    {ok, #{}};
decode(Body) ->
    try
      Decoded = rabbit_json:decode(Body),
      Normalised = maps:fold(fun(K, V, Acc) ->
                     Acc#{binary_to_atom(K, utf8) => V}
                   end, Decoded, Decoded),
      {ok, Normalised}
    catch error:_ -> {error, not_json}
    end.

accept(Body, ReqData, Context = #context{user = #user{username = Username}}) ->
    %% At this point the request was fully received.
    %% There is no point in the idle_timeout anymore.
    disable_idle_timeout(ReqData),
    case decode(Body) of
      {error, E} ->
        _ = rabbit_log:error("Encountered an error when parsing definitions: ~p", [E]),
        rabbit_mgmt_util:bad_request(rabbit_data_coercion:to_binary("failed_to_parse_json"),
                                    ReqData, Context);
      {ok, Map} ->
        case rabbit_mgmt_util:vhost(ReqData) of
            none ->
                case apply_defs(Map, Username) of
                  {error, E} ->
                        _ = rabbit_log:error("Encountered an error when importing definitions: ~p", [E]),
                        rabbit_mgmt_util:bad_request(E, ReqData, Context);
                  ok -> {true, ReqData, Context}
                end;
            not_found ->
                rabbit_mgmt_util:not_found(rabbit_data_coercion:to_binary("vhost_not_found"),
                                           ReqData, Context);
            VHost when is_binary(VHost) ->
                case apply_defs(Map, Username, VHost) of
                    {error, E} ->
                        _ = rabbit_log:error("Encountered an error when importing definitions: ~p", [E]),
                        rabbit_mgmt_util:bad_request(E, ReqData, Context);
                    ok -> {true, ReqData, Context}
                end
        end
    end.

disable_idle_timeout(#{pid := Pid, streamid := StreamID}) ->
    Pid ! {{Pid, StreamID}, {set_options, #{idle_timeout => infinity}}}.

-spec apply_defs(Map :: #{atom() => any()}, ActingUser :: rabbit_types:username()) -> 'ok' | {error, term()}.

apply_defs(Body, ActingUser) ->
    rabbit_definitions:apply_defs(Body, ActingUser).

-spec apply_defs(Map :: #{atom() => any()}, ActingUser :: rabbit_types:username(),
                VHost :: vhost:name()) -> 'ok'  | {error, term()}.

apply_defs(Body, ActingUser, VHost) ->
    rabbit_definitions:apply_defs(Body, ActingUser, VHost).

-spec apply_defs(Map :: #{atom() => any()},
                ActingUser :: rabbit_types:username(),
                SuccessFun :: fun(() -> 'ok'),
                ErrorFun :: fun((any()) -> 'ok'),
                VHost :: vhost:name()) -> 'ok' | {error, term()}.

apply_defs(Body, ActingUser, SuccessFun, ErrorFun, VHost) ->
    rabbit_definitions:apply_defs(Body, ActingUser, SuccessFun, ErrorFun, VHost).

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
    [{users,              [name, password_hash, hashing_algorithm, tags, limits]},
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
