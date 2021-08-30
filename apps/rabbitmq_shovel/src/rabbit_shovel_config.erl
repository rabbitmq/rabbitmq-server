%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_shovel_config).

-export([parse/2,
         ensure_defaults/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

resolve_module(amqp091) -> rabbit_amqp091_shovel;
resolve_module(amqp10) -> rabbit_amqp10_shovel.

is_legacy(Config) ->
    not proplists:is_defined(source, Config).

get_brokers(Props) ->
    case proplists:get_value(brokers, Props) of
        undefined ->
            [get_value(broker, Props)];
        Brokers ->
            Brokers
    end.

convert_from_legacy(Config) ->
    S = get_value(sources, Config),
    validate(S),
    SUris = get_brokers(S),
    validate_uris(brokers, SUris),
    D = get_value(destinations, Config),
    validate(D),
    DUris = get_brokers(D),
    validate_uris(brokers, DUris),
    Q = get_value(queue, Config),
    DA = proplists:get_value(delete_after, Config, never),
    Pref = proplists:get_value(prefetch_count, Config, ?DEFAULT_PREFETCH),
    RD = proplists:get_value(reconnect_delay, Config, ?DEFAULT_RECONNECT_DELAY),
    AckMode = proplists:get_value(ack_mode, Config, ?DEFAULT_ACK_MODE),
    validate_ack_mode(AckMode),
    PubFields = proplists:get_value(publish_fields, Config, []),
    PubProps = proplists:get_value(publish_properties, Config, []),
    AFH = proplists:get_value(add_forward_headers, Config, false),
    ATH = proplists:get_value(add_timestamp_header, Config, false),
    SourceDecls = proplists:get_value(declarations, S, []),
    validate_list(SourceDecls),
    DestDecls = proplists:get_value(declarations, D, []),
    validate_list(DestDecls),
    [{source, [{protocol, amqp091},
               {uris, SUris},
               {declarations, SourceDecls},
               {queue, Q},
               {delete_after, DA},
               {prefetch_count, Pref}]},
     {destination, [{protocol, amqp091},
                    {uris, DUris},
                    {declarations, DestDecls},
                    {publish_properties, PubProps},
                    {publish_fields, PubFields},
                    {add_forward_headers, AFH},
                    {add_timestamp_header, ATH}]},
     {ack_mode, AckMode},
     {reconnect_delay, RD}].

parse(ShovelName, Config0) ->
    try
        validate(Config0),
        case is_legacy(Config0) of
            true ->
                Config = convert_from_legacy(Config0),
                parse_current(ShovelName, Config);
            false ->
                parse_current(ShovelName, Config0)
        end
    catch throw:{error, Reason} ->
              {error, {invalid_shovel_configuration, ShovelName, Reason}};
          throw:Reason ->
              {error, {invalid_shovel_configuration, ShovelName, Reason}}
    end.

validate(Props) ->
    validate_proplist(Props),
    validate_duplicates(Props).

validate_proplist(Props) when is_list (Props) ->
    case lists:filter(fun ({_, _}) -> false;
                          (_) -> true
                      end, Props) of
        [] -> ok;
        Invalid ->
            throw({invalid_parameters, Invalid})
    end;
validate_proplist(X) ->
    throw({require_list, X}).

validate_duplicates(Props) ->
    case duplicate_keys(Props) of
        [] -> ok;
        Invalid ->
            throw({duplicate_parameters, Invalid})
    end.

validate_list(L) when is_list(L) -> ok;
validate_list(L) ->
    throw({require_list, L}).

validate_uris(Key, L) when not is_list(L) ->
    throw({require_list, Key, L});
validate_uris(Key, []) ->
    throw({expected_non_empty_list, Key});
validate_uris(_Key, L) ->
    validate_uris0(L).

validate_uris0([Uri | Uris]) ->
    case amqp_uri:parse(Uri) of
        {ok, _Params} ->
            validate_uris0(Uris);
        {error, _} = Err ->
            throw(Err)
    end;
validate_uris0([]) -> ok.

parse_current(ShovelName, Config) ->
    {source, Source} = proplists:lookup(source, Config),
    validate(Source),
    SrcMod = resolve_module(proplists:get_value(protocol, Source, amqp091)),
    {destination, Destination} = proplists:lookup(destination, Config),
    validate(Destination),
    DstMod = resolve_module(proplists:get_value(protocol, Destination, amqp091)),
    AckMode = proplists:get_value(ack_mode, Config, no_ack),
    validate_ack_mode(AckMode),
    {ok, #{name => ShovelName,
           shovel_type => static,
           ack_mode => AckMode,
           reconnect_delay => proplists:get_value(reconnect_delay, Config,
                                                  ?DEFAULT_RECONNECT_DELAY),
           source => rabbit_shovel_behaviour:parse(SrcMod, ShovelName,
                                                   {source, Source}),
           dest => rabbit_shovel_behaviour:parse(DstMod, ShovelName,
                                                 {destination, Destination})}}.

%% ensures that any defaults that have been applied to a parsed
%% shovel, are written back to the original proplist
ensure_defaults(ShovelConfig, ParsedShovel) ->
    lists:keystore(reconnect_delay, 1,
                   ShovelConfig,
                   {reconnect_delay,
                    ParsedShovel#shovel.reconnect_delay}).

-spec fail(term()) -> no_return().
fail(Reason) -> throw({error, Reason}).

validate_ack_mode(Val) when Val =:= no_ack orelse
                                Val =:= on_publish orelse
                                Val =:= on_confirm ->
    ok;
validate_ack_mode(WrongVal) ->
    fail({invalid_parameter_value, ack_mode,
          {ack_mode_value_requires_one_of, {no_ack, on_publish, on_confirm},
          WrongVal}}).

duplicate_keys(PropList) when is_list(PropList) ->
    proplists:get_keys(
      lists:foldl(fun (K, L) -> lists:keydelete(K, 1, L) end, PropList,
                  proplists:get_keys(PropList))).

get_value(Key, Props) ->
    case proplists:get_value(Key, Props) of
        undefined ->
            throw({missing_parameter, Key});
        V -> V
    end.
