%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_definition).

-import(rabbit_misc, [pget/2]).

-export([
    extract_source_info/2,
    extract_source_info_for_index/2
]).

-export_type([source_type/0, protocol/0, source_info/0]).

-type source_type() :: queue | exchange | unknown.
-type protocol() :: amqp091 | amqp10 | local.

-type source_info() :: #{
    type := source_type(),
    protocol := protocol(),
    queue := rabbit_types:option(binary()),
    exchange => binary(),
    vhost := vhost:name()
}.

%% @doc Extract source info from a shovel definition.
%% Parses URIs to extract vhost. Not Horus-compatible; use
%% extract_source_info_for_index/2 for Khepri projections.
-spec extract_source_info(Definition :: list() | map(), ShovelVHost :: vhost:name()) -> source_info().
extract_source_info(Def0, ShovelVHost) ->
    Def = rabbit_data_coercion:to_proplist(Def0),
    Protocol = rabbit_shovel_parameters:src_protocol(Def),
    VHost = extract_source_vhost(Def, ShovelVHost),
    BaseInfo = #{protocol => Protocol, vhost => VHost},
    case Protocol of
        amqp10 ->
            case pget(<<"src-address">>, Def) of
                undefined ->
                    BaseInfo#{type => unknown, queue => undefined};
                <<>> ->
                    BaseInfo#{type => unknown, queue => undefined};
                Address ->
                    parse_amqp10_address(Address, BaseInfo)
            end;
        _ ->
            extract_amqp091_local_source(Def, BaseInfo)
    end.

%% @doc Horus-compatible version for Khepri projections.
%% Avoids uri_string calls; uses ShovelVHost as-is, no percent-decoding.
-spec extract_source_info_for_index(Definition :: list() | map(), ShovelVHost :: vhost:name()) -> source_info().
extract_source_info_for_index(Def0, ShovelVHost) ->
    Def = rabbit_data_coercion:to_proplist(Def0),
    Protocol = rabbit_shovel_parameters:src_protocol(Def),
    BaseInfo = #{protocol => Protocol, vhost => ShovelVHost},
    case Protocol of
        amqp10 ->
            case pget(<<"src-address">>, Def) of
                undefined ->
                    BaseInfo#{type => unknown, queue => undefined};
                <<>> ->
                    BaseInfo#{type => unknown, queue => undefined};
                Address ->
                    parse_amqp10_address_simple(Address, BaseInfo)
            end;
        _ ->
            extract_amqp091_local_source(Def, BaseInfo)
    end.

%% Parse AMQP 1.0 address using RabbitMQ address scheme v2.
%% For source addresses, the only valid v2 format is `/queues/:queue`.
%% See https://www.rabbitmq.com/docs/amqp#addresses
-spec parse_amqp10_address(binary(), map()) -> source_info().
parse_amqp10_address(Address, BaseInfo) ->
    case Address of
        <<"/queues/", QNameQuoted/binary>> when QNameQuoted =/= <<>> ->
            %% Address v2: /queues/:queue (percent-encoded)
            QueueName = uri_string:percent_decode(QNameQuoted),
            BaseInfo#{type => queue, queue => QueueName};
        _ ->
            %% Unrecognized format: could be v1 (deprecated), bare name, or external.
            %% Mark as unknown since we can't reliably determine the type.
            BaseInfo#{type => unknown, queue => undefined}
    end.

%% Horus-compatible version: no uri_string calls.
-spec parse_amqp10_address_simple(binary(), map()) -> source_info().
parse_amqp10_address_simple(Address, BaseInfo) ->
    case Address of
        <<"/queues/", QNameQuoted/binary>> when QNameQuoted =/= <<>> ->
            BaseInfo#{type => queue, queue => QNameQuoted};
        _ ->
            BaseInfo#{type => unknown, queue => undefined}
    end.

-spec extract_amqp091_local_source(list(), map()) -> source_info().
extract_amqp091_local_source(Def, BaseInfo) ->
    case pget(<<"src-queue">>, Def) of
        undefined ->
            extract_exchange_source(Def, BaseInfo);
        %% `none' is the default value used by rabbit_amqp091_shovel and
        %% rabbit_local_shovel when the key is absent (via pget/3 with default).
        none ->
            extract_exchange_source(Def, BaseInfo);
        <<>> ->
            extract_exchange_source(Def, BaseInfo);
        Queue ->
            BaseInfo#{type => queue, queue => Queue}
    end.

-spec extract_exchange_source(list(), map()) -> source_info().
extract_exchange_source(Def, BaseInfo) ->
    case pget(<<"src-exchange">>, Def) of
        undefined ->
            BaseInfo#{type => unknown, queue => undefined};
        %% See comment in extract_amqp091_local_source/2 about `none'.
        none ->
            BaseInfo#{type => unknown, queue => undefined};
        <<>> ->
            BaseInfo#{type => unknown, queue => undefined};
        Exchange ->
            BaseInfo#{type => exchange, exchange => Exchange, queue => undefined}
    end.

%% @doc Extract the source vhost from URI, falling back to shovel's vhost.
%%
%% URIs are typically stored encrypted as `{encrypted, Base64Binary}' tuples.
%% The deobfuscation step handles both encrypted and plaintext formats.
%% If URI parsing fails, falls back to the virtual host the shovel belongs to.
-spec extract_source_vhost(list(), vhost:name()) -> vhost:name().
extract_source_vhost(Def, ShovelVHost) ->
    case pget(<<"src-uri">>, Def) of
        undefined ->
            ShovelVHost;
        URIs ->
            case rabbit_data_coercion:as_list(URIs) of
                [] -> ShovelVHost;
                [FirstURI | _] -> safe_parse_vhost_from_uri(FirstURI, ShovelVHost)
            end
    end.

-spec safe_parse_vhost_from_uri(term(), vhost:name()) -> vhost:name().
safe_parse_vhost_from_uri(MaybeEncryptedURI, Fallback) ->
    try
        Deobfuscated = rabbit_shovel_util:deobfuscate_value(MaybeEncryptedURI),
        parse_vhost_from_uri(Deobfuscated, Fallback)
    catch
        _:_ ->
            Fallback
    end.

-spec parse_vhost_from_uri(binary() | string(), vhost:name()) -> vhost:name().
parse_vhost_from_uri(URI, Fallback) when is_binary(URI) ->
    parse_vhost_from_uri(binary_to_list(URI), Fallback);
parse_vhost_from_uri(URI, Fallback) when is_list(URI) ->
    case uri_string:parse(URI) of
        #{path := [$/| VHostEncoded]} when VHostEncoded =/= [] ->
            %% Pattern match strips leading "/". This handles "//" (vhost "/"
            %% without percent-encoding) correctly, unlike string:trim.
            list_to_binary(uri_string:percent_decode(VHostEncoded));
        #{} ->
            <<"/">>;
        {error, _, _} ->
            Fallback
    end.
