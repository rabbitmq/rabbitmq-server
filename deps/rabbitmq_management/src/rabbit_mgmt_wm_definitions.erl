%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_definitions).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([content_types_accepted/2, allowed_methods/2, accept_json/2]).
-export([accept_multipart/2]).
-export([variances/2]).

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

    Contents = [
        {users,             rabbit_mgmt_wm_users:users(all)},
        {vhosts,            rabbit_mgmt_wm_vhosts:basic()},
        {permissions,       rabbit_mgmt_wm_permissions:permissions()},
        {topic_permissions, rabbit_mgmt_wm_topic_permissions:topic_permissions()},
        {parameters,        rabbit_mgmt_wm_parameters:basic(ReqData)},
        {global_parameters, rabbit_mgmt_wm_global_parameters:basic()},
        {policies,          rabbit_mgmt_wm_policies:basic(ReqData)},
        {queues,            Qs},
        {exchanges,         Xs},
        {bindings,          Bs}
    ],

    TopLevelDefsAndMetadata = [
        {rabbit_version, rabbit_data_coercion:to_binary(Vsn)},
        {rabbitmq_version, rabbit_data_coercion:to_binary(Vsn)},
        {product_name, rabbit_data_coercion:to_binary(ProductName)},
        {product_version, rabbit_data_coercion:to_binary(ProductVersion)},
        {rabbitmq_definition_format, <<"cluster">>},
        {original_cluster_name, rabbit_nodes:cluster_name()},
        {explanation, rabbit_data_coercion:to_binary(io_lib:format("Definitions of cluster '~ts'", [rabbit_nodes:cluster_name()]))}
    ],
    Result = TopLevelDefsAndMetadata ++ retain_whitelisted(Contents),
    ReqData1 = case rabbit_mgmt_util:qs_val(<<"download">>, ReqData) of
                   undefined -> ReqData;
                   Filename  -> rabbit_mgmt_util:set_resp_header(
                       <<"Content-Disposition">>,
                       "attachment; filename=" ++
                       binary_to_list(Filename), ReqData)
               end,

    rabbit_mgmt_util:reply(Result, ReqData1, Context).

accept_json(ReqData0, Context) ->
    BodySizeLimit = application:get_env(rabbitmq_management, max_http_body_size, ?MANAGEMENT_DEFAULT_HTTP_MAX_BODY_SIZE),
    case rabbit_mgmt_util:read_complete_body_with_limit(ReqData0, BodySizeLimit) of
        {error, http_body_limit_exceeded, LimitApplied, BytesRead} ->
            _ = rabbit_log:warning("HTTP API: uploaded definition file size (~tp) exceeded the maximum request body limit of ~tp bytes. "
                                   "Use the 'management.http.max_body_size' key in rabbitmq.conf to increase the limit if necessary", [BytesRead, LimitApplied]),
            rabbit_mgmt_util:bad_request("Exceeded HTTP request body size limit", ReqData0, Context);
        {ok, Body, ReqData} ->
            accept(Body, ReqData, Context)
    end.

vhost_definitions(ReqData, VHostName, Context) ->
    %% the existence of this virtual host is verified in the called, 'to_json/2'
    VHost = rabbit_vhost:lookup(VHostName),

    %% rabbit_mgmt_wm_<>:basic/1 filters by VHost if it is available.
    %% TODO: should we stop stripping virtual host? Such files cannot be imported on boot, for example.
    Xs = [strip_vhost(X) || X <- rabbit_mgmt_wm_exchanges:basic(ReqData),
               export_exchange(X)],
    VQs = [Q || Q <- rabbit_mgmt_wm_queues:basic(ReqData), export_queue(Q)],
    Qs = [strip_vhost(Q) || Q <- VQs],
    QNames = [{pget(name, Q), pget(vhost, Q)} || Q <- VQs],
    %% TODO: should we stop stripping virtual host? Such files cannot be imported on boot, for example.
    Bs = [strip_vhost(B) || B <- rabbit_mgmt_wm_bindings:basic(ReqData),
                            export_binding(B, QNames)],
    Parameters = [strip_vhost(
                    rabbit_mgmt_format:parameter(P))
                  || P <- rabbit_runtime_parameters:list(VHostName)],
    Contents = [
        {parameters,  Parameters},
        {policies,    [strip_vhost(P) || P <- rabbit_mgmt_wm_policies:basic(ReqData)]},
        {queues,      Qs},
        {exchanges,   Xs},
        {bindings,    Bs}
    ],

    Vsn = rabbit:base_product_version(),
    ProductName = rabbit:product_name(),
    ProductVersion = rabbit:product_version(),

    DQT = rabbit_queue_type:short_alias_of(rabbit_vhost:default_queue_type(VHostName)),
    %% note: the type changes to a map
    VHost1 = rabbit_queue_type:inject_dqt(VHost),
    Metadata = maps:get(metadata, VHost1),

    TopLevelDefsAndMetadata = [
        {rabbit_version, rabbit_data_coercion:to_binary(Vsn)},
        {rabbitmq_version, rabbit_data_coercion:to_binary(Vsn)},
        {product_name, rabbit_data_coercion:to_binary(ProductName)},
        {product_version, rabbit_data_coercion:to_binary(ProductVersion)},
        {rabbitmq_definition_format, <<"single_virtual_host">>},
        {original_vhost_name, VHostName},
        {explanation, rabbit_data_coercion:to_binary(io_lib:format("Definitions of virtual host '~ts'", [VHostName]))},
        {metadata, Metadata},
        {description, vhost:get_description(VHost)},
        {default_queue_type, DQT},
        {limits, vhost:get_limits(VHost)}
    ],
    Result = TopLevelDefsAndMetadata ++ retain_whitelisted(Contents),

    ReqData1 = case rabbit_mgmt_util:qs_val(<<"download">>, ReqData) of
                   undefined -> ReqData;
                   Filename  ->
                       HeaderVal = "attachment; filename=" ++ binary_to_list(Filename),
                       rabbit_mgmt_util:set_resp_header(<<"Content-Disposition">>, HeaderVal, ReqData)
               end,
    rabbit_mgmt_util:reply(Result, ReqData1, Context).

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
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).


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
    _ = disable_idle_timeout(ReqData),
    case decode(Body) of
      {error, E} ->
        rabbit_log:error("Encountered an error when parsing definitions: ~tp", [E]),
        rabbit_mgmt_util:bad_request(rabbit_data_coercion:to_binary("failed_to_parse_json"),
                                    ReqData, Context);
      {ok, Map} ->
        case rabbit_mgmt_util:vhost(ReqData) of
            none ->
                case apply_defs(Map, Username) of
                  {error, E} ->
                        rabbit_log:error("Encountered an error when importing definitions: ~tp", [E]),
                        rabbit_mgmt_util:bad_request(E, ReqData, Context);
                  ok -> {true, ReqData, Context}
                end;
            not_found ->
                rabbit_mgmt_util:not_found(rabbit_data_coercion:to_binary("vhost_not_found"),
                                           ReqData, Context);
            VHost when is_binary(VHost) ->
                case apply_defs(Map, Username, VHost) of
                    {error, E} ->
                        rabbit_log:error("Encountered an error when importing definitions: ~tp", [E]),
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
     {vhosts,             [name, description, tags, default_queue_type, metadata]},
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

retain_whitelisted(Items) ->
    [retain_whitelisted_items(N, V, proplists:get_value(N, rw_state())) || {N, V} <- Items].

retain_whitelisted_items(Name, List, Allowed) ->
    {Name, [only_whitelisted_for_item(I, Allowed) || I <- List]}.

only_whitelisted_for_item(Item, Allowed) when is_map(Item) ->
    Map1 = maps:with(Allowed, Item),
    maps:filter(fun(_Key, Val) ->
                    Val =/= undefined
                end, Map1);
only_whitelisted_for_item(Item, Allowed) when is_list(Item) ->
    [{K, Fact} || {K, Fact} <- Item, lists:member(K, Allowed), Fact =/= undefined].

strip_vhost(Item) ->
    lists:keydelete(vhost, 1, Item).
