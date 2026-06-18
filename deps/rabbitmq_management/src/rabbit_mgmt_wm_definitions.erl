%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_definitions).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([content_types_accepted/2, allowed_methods/2, accept_json/2]).
-export([accept_multipart/2]).
-export([variances/2]).

-include("rabbit_mgmt.hrl").
-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("kernel/include/logger.hrl").

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
            export(all, ReqData, Context);
        not_found ->
            rabbit_mgmt_util:bad_request(rabbit_data_coercion:to_binary("vhost_not_found"),
                                         ReqData, Context);
        VHostName ->
            export({vhost, VHostName}, ReqData, Context)
    end.

%% Single, scope-parameterised definitions export shared by the cluster-wide
%% (GET /api/definitions) and per-virtual-host (GET /api/definitions/:vhost)
%% endpoints. Scope is 'all' or {vhost, Name}.
%%
%% Everything but queues and bindings is gathered up front as maps (the
%% canonical definition format from rabbit_definitions, the same one used by
%% `rabbitmqctl export_definitions`), filtered to the virtual host when scoped.
%% Queues and bindings -- the categories that grow without bound -- are streamed
%% via rabbit_definitions:fold_queues/3 and fold_bindings/3, so they are never
%% materialised in memory.
export(Scope, ReqData0, Context) ->
    ReqData = maybe_content_disposition(ReqData0),
    case cowboy_req:method(ReqData) of
        %% A HEAD response carries no body, so skip gathering the definitions.
        <<"HEAD">> ->
            {<<>>, cowboy_req:set_resp_header(<<"cache-control">>, <<"no-cache">>, ReqData), Context};
        _ ->
            SmallParts = small_parts(Scope),
            case maps:get(media_type, ReqData, undefined) of
                {<<"application">>, <<"bert">>, _} ->
                    buffered(Scope, SmallParts, fun erlang:term_to_binary/1, ReqData, Context);
                _ ->
                    stream_export(Scope, SmallParts, ReqData, Context)
            end
    end.

%% Gathers every category except queues and bindings. The version/product
%% metadata and which categories are included depend on the scope; for a single
%% virtual host the per-item 'vhost' field is stripped, matching the historical
%% behaviour.
small_parts(all) ->
    Vsn = rabbit_data_coercion:to_binary(rabbit:base_product_version()),
    ClusterName = rabbit_nodes:cluster_name(),
    #{
        rabbit_version             => Vsn,
        rabbitmq_version           => Vsn,
        product_name               => rabbit_data_coercion:to_binary(rabbit:product_name()),
        product_version            => rabbit_data_coercion:to_binary(rabbit:product_version()),
        rabbitmq_definition_format => <<"cluster">>,
        original_cluster_name      => ClusterName,
        explanation                => rabbit_data_coercion:to_binary(
                                        io_lib:format("Definitions of cluster '~ts'", [ClusterName])),
        users                      => rabbit_definitions:list_users(),
        vhosts                     => rabbit_definitions:list_vhosts(),
        permissions                => rabbit_definitions:list_permissions(),
        topic_permissions          => rabbit_definitions:list_topic_permissions(),
        parameters                 => rabbit_definitions:list_runtime_parameters(),
        global_parameters          => rabbit_definitions:list_global_runtime_parameters(),
        policies                   => rabbit_definitions:list_policies(),
        exchanges                  => rabbit_definitions:list_exchanges()
    };
small_parts({vhost, VHostName}) ->
    VHost = rabbit_vhost:lookup(VHostName),
    Vsn = rabbit_data_coercion:to_binary(rabbit:base_product_version()),
    %% Filter the cluster-wide category to this virtual host and drop the now
    %% redundant 'vhost' field from each item.
    Keep = fun(List) ->
                   [maps:remove(<<"vhost">>, Item)
                    || Item <- List, maps:get(<<"vhost">>, Item, undefined) =:= VHostName]
           end,
    #{
        rabbit_version             => Vsn,
        rabbitmq_version           => Vsn,
        product_name               => rabbit_data_coercion:to_binary(rabbit:product_name()),
        product_version            => rabbit_data_coercion:to_binary(rabbit:product_version()),
        rabbitmq_definition_format => <<"single_virtual_host">>,
        original_vhost_name        => VHostName,
        explanation                => rabbit_data_coercion:to_binary(
                                        io_lib:format("Definitions of virtual host '~ts'", [VHostName])),
        metadata                   => vhost:get_metadata(VHost),
        description                => vhost:get_description(VHost),
        limits                     => maps:from_list(vhost:get_limits(VHost)),
        parameters                 => Keep(rabbit_definitions:list_runtime_parameters()),
        policies                   => Keep(rabbit_definitions:list_policies()),
        exchanges                  => Keep(rabbit_definitions:list_exchanges())
    }.

%% Buffered path for BERT, which cannot be streamed incrementally. Queues and
%% bindings are materialised here; the common GET+JSON path streams them instead.
buffered(Scope, SmallParts, Encode, ReqData0, Context) ->
    Queues   = lists:reverse(
                 rabbit_definitions:fold_queues(
                   Scope, fun(Q, Acc) -> [maybe_strip(Scope, Q) | Acc] end, [])),
    Bindings = lists:reverse(
                 rabbit_definitions:fold_bindings(
                   Scope, fun(B, Acc) -> [maybe_strip(Scope, B) | Acc] end, [])),
    ReqData = cowboy_req:set_resp_header(<<"cache-control">>, <<"no-cache">>, ReqData0),
    {Encode(SmallParts#{queues => Queues, bindings => Bindings}), ReqData, Context}.

maybe_content_disposition(ReqData) ->
    Constraint = cowboy_constraints:from_fun(fun cow_http:ensure_token/1),
    case cowboy_req:match_qs([{download, [Constraint], undefined}], ReqData) of
        #{download := undefined} ->
            ReqData;
        #{download := Filename} ->
            cowboy_req:set_resp_header(
                <<"content-disposition">>,
                [<<"attachment; filename=">>, Filename],
                ReqData)
    end.

maybe_strip({vhost, _}, Map) -> maps:remove(<<"vhost">>, Map);
maybe_strip(all, Map)        -> Map.

%% Number of array elements (queues, bindings) encoded per streamed chunk.
%% Large enough to keep HTTP chunk overhead low, small enough to bound peak
%% memory.
-define(STREAM_BATCH_SIZE, 2000).

%% Streams the JSON response without ever materialising the whole document. The
%% small categories are streamed first, then queues and bindings are folded
%% straight from the store into the socket in batches. The cowboy_rest
%% content-type response header is already set when the provider callback runs,
%% so stream_reply/3 picks it up; we only add cache-control. Returns {stop, ...}
%% so cowboy_rest treats the response as already sent.
stream_export(Scope, SmallParts, ReqData0, Context) ->
    ReqData1 = cowboy_req:set_resp_header(<<"cache-control">>, <<"no-cache">>, ReqData0),
    ReqData = cowboy_req:stream_reply(200, #{}, ReqData1),
    ok = cowboy_req:stream_body(<<"{">>, nofin, ReqData),
    %% small_parts/1 always contains metadata, so it is never empty: the folded
    %% categories below can unconditionally be comma-prefixed.
    stream_entries(maps:to_list(SmallParts), ReqData, true),
    stream_folded(<<"queues">>, fun rabbit_definitions:fold_queues/3, Scope, ReqData),
    stream_folded(<<"bindings">>, fun rabbit_definitions:fold_bindings/3, Scope, ReqData),
    ok = cowboy_req:stream_body(<<"}">>, fin, ReqData),
    {stop, ReqData, Context}.

%% Streams `"Key":[ ... ]' where the array elements are produced by
%% FoldFun(Scope, Fun, Acc), batched so at most ?STREAM_BATCH_SIZE items are
%% held at a time. Always comma-prefixed (small_parts is never empty).
stream_folded(Key, FoldFun, Scope, ReqData) ->
    ok = cowboy_req:stream_body([$,, $", Key, $", $:, $[], nofin, ReqData),
    {ReqData, First, Pending, _N} =
        FoldFun(Scope,
                fun(Item, {Req, Fst, Batch, N}) ->
                        Item1 = maybe_strip(Scope, Item),
                        case N + 1 >= ?STREAM_BATCH_SIZE of
                            true ->
                                ok = cowboy_req:stream_body(
                                       encode_batch(lists:reverse([Item1 | Batch]), Fst), nofin, Req),
                                {Req, false, [], 0};
                            false ->
                                {Req, Fst, [Item1 | Batch], N + 1}
                        end
                end,
                {ReqData, true, [], 0}),
    case Pending of
        [] -> ok;
        _  -> ok = cowboy_req:stream_body(encode_batch(lists:reverse(Pending), First), nofin, ReqData)
    end,
    ok = cowboy_req:stream_body(<<"]">>, nofin, ReqData).

stream_entries([], _ReqData, _First) ->
    ok;
stream_entries([{Key, Value} | Rest], ReqData, First) ->
    Prefix = case First of
                 true  -> [encode_key(Key), $:];
                 false -> [$,, encode_key(Key), $:]
             end,
    ok = cowboy_req:stream_body(Prefix, nofin, ReqData),
    stream_value(Value, ReqData),
    stream_entries(Rest, ReqData, false).

encode_key(Key) when is_atom(Key)   -> [$", atom_to_binary(Key, utf8), $"];
encode_key(Key) when is_binary(Key) -> [$", Key, $"].

%% Category lists (exchanges, users, ...) are lists of maps; they are streamed
%% element by element, in batches, so a huge category never sits fully encoded
%% in memory. Any other value -- scalars, maps, and proplists such as a virtual
%% host's `limits' -- is encoded whole by rabbit_json, which correctly renders
%% proplists as JSON objects.
stream_value([], ReqData) ->
    ok = cowboy_req:stream_body(<<"[]">>, nofin, ReqData);
stream_value([H | _] = Value, ReqData) when is_map(H) ->
    ok = cowboy_req:stream_body(<<"[">>, nofin, ReqData),
    stream_array(Value, ReqData, true),
    ok = cowboy_req:stream_body(<<"]">>, nofin, ReqData);
stream_value(Value, ReqData) ->
    ok = cowboy_req:stream_body(rabbit_json:encode(Value), nofin, ReqData).

stream_array([], _ReqData, _First) ->
    ok;
stream_array(List, ReqData, First) ->
    {Batch, Rest} = split_batch(List, ?STREAM_BATCH_SIZE, []),
    ok = cowboy_req:stream_body(encode_batch(Batch, First), nofin, ReqData),
    stream_array(Rest, ReqData, false).

split_batch([H | T], N, Acc) when N > 0 -> split_batch(T, N - 1, [H | Acc]);
split_batch(Rest, _N, Acc)              -> {lists:reverse(Acc), Rest}.

%% First =:= true means this is the opening batch, whose first element must not
%% be preceded by a comma. Every other element is comma-prefixed.
encode_batch([], _First) ->
    [];
encode_batch([H | T], true) ->
    [rabbit_json:encode(H) | encode_batch(T, false)];
encode_batch([H | T], false) ->
    [$,, rabbit_json:encode(H) | encode_batch(T, false)].

accept_json(ReqData0, Context) ->
    BodySizeLimit = application:get_env(rabbitmq_management, max_http_body_size, ?MANAGEMENT_DEFAULT_HTTP_MAX_BODY_SIZE),
    case rabbit_mgmt_util:read_complete_body_with_limit(ReqData0, BodySizeLimit) of
        {error, http_body_limit_exceeded, LimitApplied, BytesRead} ->
            _ = ?LOG_WARNING("HTTP API: uploaded definition file size (~tp) exceeded the maximum request body limit of ~tp bytes. "
                                   "Use the 'management.http.max_body_size' key in rabbitmq.conf to increase the limit if necessary", [BytesRead, LimitApplied]),
            rabbit_mgmt_util:bad_request("Exceeded HTTP request body size limit", ReqData0, Context);
        {ok, Body, ReqData} ->
            accept(Body, ReqData, Context)
    end.

accept_multipart(ReqData0, Context) ->
    BodySizeLimit = application:get_env(rabbitmq_management, max_http_body_size,
                                        ?MANAGEMENT_DEFAULT_HTTP_MAX_BODY_SIZE),
    case get_all_parts(ReqData0, BodySizeLimit) of
        {error, http_body_limit_exceeded, LimitApplied, BytesRead} ->
            _ = ?LOG_WARNING("HTTP API: uploaded definition file size (~tp) exceeded the maximum request body limit of ~tp bytes. "
                             "Use the 'management.http.max_body_size' key in rabbitmq.conf to increase the limit if necessary",
                             [BytesRead, LimitApplied]),
            rabbit_mgmt_util:bad_request("Exceeded HTTP request body size limit", ReqData0, Context);
        {Parts, ReqData} ->
            Redirect = get_part(<<"redirect">>, Parts),
            Payload = get_part(<<"file">>, Parts),
            Resp = {Res, _, _} = accept(Payload, ReqData, Context),
            case {Res, Redirect} of
                {true, unknown} -> {true, ReqData, Context};
                {true, _}       -> {{true, Redirect}, ReqData, Context};
                _               -> Resp
            end
    end.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).


%%--------------------------------------------------------------------

decode(<<"">>) ->
    {ok, #{}};
decode(Body0) ->
    Body = rabbit_misc:strip_bom(Body0),
    try
      Decoded = rabbit_json:decode(Body),
      Normalised = atomise_known_keys(Decoded),
      {ok, Normalised}
    catch error:_ -> {error, not_json}
    end.

%% Only atomize the atoms that already exist.
atomise_known_keys(Map) ->
    maps:fold(fun(K, V, Acc) ->
        try binary_to_existing_atom(K, utf8) of
            Atom -> Acc#{Atom => V}
        catch
            error:badarg -> Acc
        end
    end, #{}, Map).

accept(Body, ReqData, Context = #context{user = #user{username = Username}}) ->
    %% At this point the request was fully received.
    %% There is no point in the idle_timeout anymore.
    _ = disable_idle_timeout(ReqData),
    case decode(Body) of
      {error, E} ->
        ?LOG_ERROR("Encountered an error when parsing definitions: ~tp", [E]),
        rabbit_mgmt_util:bad_request(rabbit_data_coercion:to_binary("failed_to_parse_json"),
                                    ReqData, Context);
      {ok, Map} ->
        case rabbit_mgmt_util:vhost(ReqData) of
            none ->
                case apply_defs(Map, Username) of
                  {error, E} ->
                        ?LOG_ERROR("Encountered an error when importing definitions: ~tp", [E]),
                        rabbit_mgmt_util:bad_request(E, ReqData, Context);
                  ok -> {true, ReqData, Context}
                end;
            not_found ->
                rabbit_mgmt_util:not_found(rabbit_data_coercion:to_binary("vhost_not_found"),
                                           ReqData, Context);
            VHost when is_binary(VHost) ->
                case apply_defs(Map, Username, VHost) of
                    {error, E} ->
                        ?LOG_ERROR("Encountered an error when importing definitions: ~tp", [E]),
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

get_all_parts(Req, BodySizeLimit) ->
    %% Early rejection when the declared Content-Length exceeds the limit.
    %% This mirrors the behaviour of read_complete_body_with_limit/2 for
    %% regular (non-multipart) bodies.
    case cowboy_req:body_length(Req) of
        N when is_integer(N), N > BodySizeLimit ->
            {error, http_body_limit_exceeded, BodySizeLimit, N};
        _ ->
            get_all_parts(Req, 0, BodySizeLimit, [])
    end.

get_all_parts(Req0, BodySize0, BodySizeLimit, Acc) ->
    case cowboy_req:read_part(Req0) of
        {done, Req1} ->
            {Acc, Req1};
        {ok, Headers, Req1} ->
            %% Approximate maximum size of part headers.
            BodySize1 = BodySize0 + 2048,
            Name = case cow_multipart:form_data(Headers) of
                       {data, N} -> N;
                       {file, N, _, _} -> N
                   end,
            case stream_part_body(Req1, BodySize1, BodySizeLimit, <<>>) of
                {ok, Body, BodySize, Req2} ->
                    get_all_parts(Req2, BodySize, BodySizeLimit, [{Name, Body}|Acc]);
                {error, http_body_limit_exceeded, _, _} = Error ->
                    Error
            end
    end.

stream_part_body(Req0, BodySize, BodySizeLimit, Acc) ->
    %% Pass an explicit length to read_part_body so that Cowboy's internal
    %% buffering (default 8 MiB) respects the configured limit instead of
    %% always accumulating large chunks before returning control to us.
    Opts = #{length => BodySizeLimit},
    case cowboy_req:read_part_body(Req0, Opts) of
        {more, Data, _} when BodySize + byte_size(Data) > BodySizeLimit ->
            {error, http_body_limit_exceeded, BodySizeLimit, BodySize + byte_size(Data)};
        {more, Data, Req1} ->
            stream_part_body(Req1, BodySize + byte_size(Data), BodySizeLimit,
                             <<Acc/binary, Data/binary>>);
        {ok, Data, Req1} ->
            Total = BodySize + byte_size(Data),
            case Total > BodySizeLimit of
                true ->
                    {error, http_body_limit_exceeded, BodySizeLimit, Total};
                false ->
                    {ok, <<Acc/binary, Data/binary>>, Total, Req1}
            end
    end.

get_part(Name, Parts) ->
    case lists:keyfind(Name, 1, Parts) of
        false -> unknown;
        {_, Value} -> Value
    end.
