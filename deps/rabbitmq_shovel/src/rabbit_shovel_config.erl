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
%%  Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_shovel_config).

-export([parse/2,
         ensure_defaults/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

-define(IGNORE_FIELDS, [delete_after]).
-define(EXTRA_KEYS, [add_forward_headers]).

parse(ShovelName, Config) ->
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
    Config1 = proplists:unfold(Config),
    case [E || E <- Config1, not (is_tuple(E) andalso tuple_size(E) == 2)] of
        []      -> case duplicate_keys(Config1) of
                       []   -> return(lists:ukeysort(1, Config1 ++ Defaults));
                       Dups -> fail({duplicate_parameters, Dups})
                   end;
        Invalid -> fail({invalid_parameters, Invalid})
    end.

parse_shovel_config_proplist(Config) ->
    Dict = dict:from_list(Config),
    Fields = record_info(fields, shovel) -- ?IGNORE_FIELDS,
    Keys = dict:fetch_keys(Dict) -- ?EXTRA_KEYS,
    case {Keys -- Fields, Fields -- Keys} of
        {[], []}      -> {_Pos, Dict1} =
                             lists:foldl(
                               fun (FieldName, {Pos, Acc}) ->
                                       {Pos + 1,
                                        dict:update(FieldName,
                                                    fun (V) -> {V, Pos} end,
                                                    Acc)}
                               end, {2, Dict}, Fields),
                         return(Dict1);
        {[], Missing} -> fail({missing_parameters, Missing});
        {Unknown, _}  -> fail({unrecognised_parameters, Unknown})
    end.

parse_shovel_config_dict(Name, Dict) ->
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
                         make_parse_publish(publish_fields),
                         make_parse_publish(publish_properties),
                         {fun parse_non_negative_number/1,  reconnect_delay}]],
            #shovel{}),
    case dict:find(add_forward_headers, Dict) of
        {ok, true} -> add_forward_headers_fun(Name, Cfg);
        _          -> Cfg
    end.

%% --=: Plain state monad implementation start :=--
run_state_monad(FunList, State) ->
    lists:foldl(fun (Fun, StateN) -> Fun(StateN) end, State, FunList).

return(V) -> V.

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

    ResourceDecls =
        case proplists:get_value(declarations, Endpoint, []) of
            Decls when is_list(Decls) ->
                Decls;
            Decls ->
                fail({expected_list, declarations, Decls})
        end,
    {[], ResourceDecls1} =
        run_state_monad(
          lists:duplicate(length(ResourceDecls), fun parse_declaration/1),
          {ResourceDecls, []}),

    DeclareFun =
        fun (_Conn, Ch) ->
                [amqp_channel:call(Ch, M) || M <- lists:reverse(ResourceDecls1)]
        end,
    return({#endpoint{uris = Brokers1,
                      resource_declaration = DeclareFun},
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

parse_declaration({[{Method, Props} | Rest], Acc}) when is_list(Props) ->
    FieldNames = try rabbit_framing_amqp_0_9_1:method_fieldnames(Method)
                 catch exit:Reason -> fail(Reason)
                 end,
    case proplists:get_keys(Props) -- FieldNames of
        []            -> ok;
        UnknownFields -> fail({unknown_fields, Method, UnknownFields})
    end,
    {Res, _Idx} = lists:foldl(
                    fun (K, {R, Idx}) ->
                            NewR = case proplists:get_value(K, Props) of
                                       undefined -> R;
                                       V         -> setelement(Idx, R, V)
                                   end,
                            {NewR, Idx + 1}
                    end, {rabbit_framing_amqp_0_9_1:method_record(Method), 2},
                    FieldNames),
    return({Rest, [Res | Acc]});
parse_declaration({[{Method, Props} | _Rest], _Acc}) ->
    fail({expected_method_field_list, Method, Props});
parse_declaration({[Method | Rest], Acc}) ->
    parse_declaration({[{Method, []} | Rest], Acc}).

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

make_parse_publish(publish_fields) ->
    {make_parse_publish1(record_info(fields, 'basic.publish')), publish_fields};
make_parse_publish(publish_properties) ->
    {make_parse_publish1(record_info(fields, 'P_basic')), publish_properties}.

make_parse_publish1(ValidFields) ->
    fun ({Fields, Pos}) when is_list(Fields) ->
            make_publish_fun(Fields, Pos, ValidFields);
        ({Fields, _Pos}) ->
            fail({require_list, Fields})
    end.

make_publish_fun(Fields, Pos, ValidFields) ->
    SuppliedFields = proplists:get_keys(Fields),
    case SuppliedFields -- ValidFields of
        [] ->
            FieldIndices = make_field_indices(ValidFields, Fields),
            Fun = fun (_SrcUri, _DestUri, Publish) ->
                          lists:foldl(fun ({Pos1, Value}, Pub) ->
                                              setelement(Pos1, Pub, Value)
                                      end, Publish, FieldIndices)
                  end,
            return({Fun, Pos});
        Unexpected ->
            fail({unexpected_fields, Unexpected, ValidFields})
    end.

make_field_indices(Valid, Fields) ->
    make_field_indices(Fields, field_map(Valid, 2), []).

make_field_indices([], _Idxs , Acc) ->
    lists:reverse(Acc);
make_field_indices([{Key, Value} | Rest], Idxs, Acc) ->
    make_field_indices(Rest, Idxs, [{dict:fetch(Key, Idxs), Value} | Acc]).

field_map(Fields, Idx0) ->
    {Dict, _IdxMax} =
        lists:foldl(fun (Field, {Dict1, Idx1}) ->
                            {dict:store(Field, Idx1, Dict1), Idx1 + 1}
                    end, {dict:new(), Idx0}, Fields),
    Dict.

duplicate_keys(PropList) ->
    proplists:get_keys(
      lists:foldl(fun (K, L) -> lists:keydelete(K, 1, L) end, PropList,
                  proplists:get_keys(PropList))).

add_forward_headers_fun(Name, #shovel{publish_properties = PubProps} = Cfg) ->
    PubProps2 =
        fun(SrcUri, DestUri, Props) ->
                rabbit_shovel_util:update_headers(
                  [{<<"shovelled-by">>, rabbit_nodes:cluster_name()},
                   {<<"shovel-type">>,  <<"static">>},
                   {<<"shovel-name">>,  list_to_binary(atom_to_list(Name))}],
                  [], SrcUri, DestUri, PubProps(SrcUri, DestUri, Props))
        end,
    Cfg#shovel{publish_properties = PubProps2}.
