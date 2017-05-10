%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_shovel_config).

-export([parse/2,
         ensure_defaults/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

-define(IGNORE_FIELDS, [delete_after]).
-define(EXTRA_KEYS, [add_forward_headers, add_timestamp_header]).

resolve_module(amqp091) -> rabbit_amqp091_shovel;
resolve_module(amqp10) -> rabbit_amqp10_shovel.

is_legacy(Config) ->
    not proplists:is_defined(source, Config).

convert_from_legacy(Config) ->
    S = proplists:get_value(sources, Config),
    SUris = proplists:get_value(brokers, S, [proplists:get_value(broker, S)]),
    D = proplists:get_value(destinations, Config),
    DUris = proplists:get_value(brokers, D, [proplists:get_value(broker, D)]),
    Q = proplists:get_value(queue, Config),
    DA = proplists:get_value(delete_after, Config, never),
    Pref = proplists:get_value(prefetch_count, Config, ?DEFAULT_PREFETCH),
    RD = proplists:get_value(reconnect_delay, Config, ?DEFAULT_RECONNECT_DELAY),
    AckMode = proplists:get_value(ack_mode, Config, ?DEFAULT_ACK_MODE),
    PubFields = proplists:get_value(publish_fields, Config, []),
    PubProps = proplists:get_value(publish_properties, Config, []),
    AFH = proplists:get_value(add_forward_headers, Config, false),
    ATH = proplists:get_value(add_timestamp_header, Config, false),
    [{source, [{protocol, amqp091},
               {uris, SUris},
               {declarations, proplists:get_value(declarations, S, [])},
               {queue, Q},
               {delete_after, DA},
               {prefetch_count, Pref}]},
     {destination, [{protocol, amqp091},
                    {uris, DUris},
                    {declarations, proplists:get_value(declarations, D, [])},
                    {publish_properties, PubProps},
                    {publish_fields, PubFields},
                    {add_forward_headers, AFH},
                    {add_timestamp_header, ATH}]},
     {ack_mode, AckMode},
     {reconnect_delay, RD}].

parse(ShovelName, Config0) ->
    case is_legacy(Config0) of
        true ->
            case parse_legacy(ShovelName, Config0) of
                {error, _} = Err -> Err;
                _ ->
                    Config = convert_from_legacy(Config0),
                    try_parse_current(ShovelName, Config)
            end;
        false ->
            try_parse_current(ShovelName, Config0)
    end.

try_parse_current(ShovelName, Config) ->
    try
        parse_current(ShovelName, Config)
    catch throw:{error, Reason} ->
            {error, {invalid_shovel_configuration, ShovelName, Reason}}
    end.

parse_current(ShovelName, Config) ->
    {source, Source} = proplists:lookup(source, Config),
    SrcMod = resolve_module(proplists:get_value(protocol, Source, amqp091)),
    {destination, Destination} = proplists:lookup(destination, Config),
    DstMod = resolve_module(proplists:get_value(protocol, Destination, amqp091)),
    {ok, #{name => ShovelName,
           shovel_type => static,
           ack_mode => proplists:get_value(ack_mode, Config, no_ack),
           reconnect_delay => proplists:get_value(reconnect_delay, Config,
                                                  ?DEFAULT_RECONNECT_DELAY),
           source => rabbit_shovel_behaviour:parse(SrcMod, ShovelName,
                                                  {source, Source}),
           dest => rabbit_shovel_behaviour:parse(DstMod, ShovelName,
                                                 {destination, Destination})}}.

parse_legacy(ShovelName, Config) ->
    {ok, Defaults} = application:get_env(defaults),
    try
        {ok, parse_shovel_config_dict(
               ShovelName, parse_shovel_config_proplist(
                             enrich_shovel_config(Config, Defaults)))}
    catch throw:{error, Reason} ->
            {error, {invalid_shovel_configuration, ShovelName, Reason}}
    end.

%% ensures that any defaults that have been applied to a parsed
%% shovel, are written back to the original proplist
ensure_defaults(ShovelConfig, ParsedShovel) ->
    lists:keystore(reconnect_delay, 1,
                   ShovelConfig,
                   {reconnect_delay,
                    ParsedShovel#shovel.reconnect_delay}).

enrich_shovel_config(Config, Defaults) ->
    error_logger:info_msg("enrich_shovel_config ~p ~p~n", [Config, Defaults]),
    Config1 = proplists:unfold(Config),
    case [E || E <- Config1, not (is_tuple(E) andalso tuple_size(E) == 2)] of
        []      -> case duplicate_keys(Config1) of
                       [] ->
                           return(lists:ukeysort(1, Config1 ++ Defaults));
                       Dups ->
                           fail({duplicate_parameters, Dups})
                   end;
        Invalid -> fail({invalid_parameters, Invalid})
    end.

parse_shovel_config_proplist(Config) ->
    Dict = dict:from_list(Config),
    Fields = record_info(fields, shovel) -- ?IGNORE_FIELDS,
    Keys = dict:fetch_keys(Dict) -- ?EXTRA_KEYS,
    case {Keys -- Fields, Fields -- Keys} of
        {[], []} -> {_Pos, Dict1} =
                        lists:foldl(
                          fun (FieldName, {Pos, Acc}) ->
                                  {Pos + 1,
                                   dict:update(FieldName,
                                               fun (V) -> {V, Pos} end,
                                               Acc)}
                          end, {2, Dict}, Fields),
                    return(Dict1);
        {[], Missing} -> fail({missing_parameters, Missing});
        {Unknown, _} -> fail({unrecognised_parameters, Unknown})
    end.

parse_shovel_config_dict(_Name, Dict) ->
    Cfg = run_state_monad(
            [fun (Shovel) ->
                     {ok, Value} = dict:find(Key, Dict),
                     try {ParsedValue, Pos} = Fun(Value),
                          return(setelement(Pos, Shovel, ParsedValue))
                     catch throw:{error, Reason} ->
                             fail({invalid_parameter_value, Key, Reason})
                     end
             end || {Fun, Key} <-
                        [{fun parse_endpoint/1,             sources},
                         {fun parse_endpoint/1,             destinations},
                         {fun parse_non_negative_integer/1, prefetch_count},
                         {fun parse_ack_mode/1,             ack_mode},
                         {fun parse_binary/1,               queue},
                         {fun parse_non_negative_number/1,  reconnect_delay}]],
            #shovel{}),
    Cfg.

%% --=: Plain state monad implementation start :=--
run_state_monad(FunList, State) ->
    lists:foldl(fun (Fun, StateN) -> Fun(StateN) end, State, FunList).

return(V) -> V.

-spec fail(term()) -> no_return().
fail(Reason) -> throw({error, Reason}).
%% --=: end :=--

parse_endpoint({Endpoint, Pos}) when is_list(Endpoint) ->
    Brokers = case proplists:get_value(brokers, Endpoint) of
                  undefined ->
                      case proplists:get_value(broker, Endpoint) of
                          undefined -> fail({missing_endpoint_parameter,
                                             broker_or_brokers});
                          B         -> [B]
                      end;
                  Bs when is_list(Bs) ->
                      Bs;
                  B ->
                      fail({expected_list, brokers, B})
              end,
    {[], Brokers1} = run_state_monad(
                       lists:duplicate(length(Brokers),
                                       fun check_uri/1),
                       {Brokers, []}),

    case proplists:get_value(declarations, Endpoint, []) of
        Decls when is_list(Decls) ->
            ok;
        Decls ->
            fail({expected_list, declarations, Decls})
    end,
    return({#endpoint{uris = Brokers1},
            Pos});
parse_endpoint({Endpoint, _Pos}) ->
    fail({require_list, Endpoint}).

check_uri({[Uri | Uris], Acc}) ->
    case amqp_uri:parse(Uri) of
        {ok, _Params} ->
            return({Uris, [Uri | Acc]});
        {error, _} = Err ->
            throw(Err)
    end.

parse_non_negative_integer({N, Pos}) when is_integer(N) andalso N >= 0 ->
    return({N, Pos});
parse_non_negative_integer({N, _Pos}) ->
    fail({require_non_negative_integer, N}).

parse_non_negative_number({N, Pos}) when is_number(N) andalso N >= 0 ->
    return({N, Pos});
parse_non_negative_number({N, _Pos}) ->
    fail({require_non_negative_number, N}).

parse_binary({Binary, Pos}) when is_binary(Binary) ->
    return({Binary, Pos});
parse_binary({NotABinary, _Pos}) ->
    fail({require_binary, NotABinary}).

parse_ack_mode({Val, Pos}) when Val =:= no_ack orelse
                                Val =:= on_publish orelse
                                Val =:= on_confirm ->
    return({Val, Pos});
parse_ack_mode({WrongVal, _Pos}) ->
    fail({ack_mode_value_requires_one_of, {no_ack, on_publish, on_confirm},
          WrongVal}).

duplicate_keys(PropList) when is_list(PropList) ->
    error_logger:info_msg("duplicate_keys ~p~n", [PropList]),
    proplists:get_keys(
      lists:foldl(fun (K, L) -> lists:keydelete(K, 1, L) end, PropList,
                  proplists:get_keys(PropList)));
duplicate_keys(Fields) ->
    fail({require_list, Fields}).
