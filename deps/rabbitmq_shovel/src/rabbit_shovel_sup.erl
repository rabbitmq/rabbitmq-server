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
%%   The Original Code is RabbitMQ-shovel.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2010 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_shovel_sup).
-behaviour(supervisor3).

-export([start_link/0, init/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

start_link() ->
    case parse_configuration(application:get_env(shovels)) of
        {ok, Configurations} ->
            supervisor3:start_link({local, ?MODULE}, ?MODULE, [Configurations]);
        {error, Reason} ->
            {error, Reason}
    end.

init([Configurations]) ->
    Len = dict:size(Configurations),
    ChildSpecs = [{rabbit_shovel_status,
                   {rabbit_shovel_status, start_link, []},
                   transient,
                   16#ffffffff,
                   worker,
                   [rabbit_shovel_status]} | make_child_specs(Configurations)],
    {ok, {{one_for_one, 2*Len, 2}, ChildSpecs}}.

make_child_specs(Configurations) ->
    dict:fold(
      fun (ShovelName, ShovelConfig, Acc) ->
              [{ShovelName,
                {rabbit_shovel_worker, start_link, [ShovelName, ShovelConfig]},
                case ShovelConfig #shovel.reconnect of
                    0 -> temporary;
                    N -> {transient, N}
                end,
                16#ffffffff,
                worker,
                [rabbit_shovel_worker]} | Acc]
      end, [], Configurations).

parse_configuration(undefined) ->
    {error, no_shovels_configured};
parse_configuration({ok, Env}) ->
    {ok, Defaults} = application:get_env(defaults),
    parse_configuration(Defaults, Env, dict:new()).

parse_configuration(_Defaults, [], Acc) ->
    {ok, Acc};
parse_configuration(Defaults, [{ShovelName, ShovelConfig} | Env], Acc)
  when is_atom(ShovelName) ->
    case dict:is_key(ShovelName, Acc) of
        true  -> {error, {duplicate_shovel_definition, ShovelName}};
        false -> ShovelConfig1 = lists:ukeysort(1, ShovelConfig ++ Defaults),
                 case parse_shovel_config(ShovelName, ShovelConfig1) of
                     {ok, Config} -> parse_configuration(
                                       Defaults, Env,
                                       dict:store(ShovelName, Config, Acc));
                     Other        -> Other
                 end
    end;
parse_configuration(_Defaults, _, _Acc) ->
    {error, require_list_of_shovel_configurations_with_atom_names}.

parse_shovel_config(ShovelName, ShovelConfig) ->
    Dict = dict:from_list(ShovelConfig),
    case dict:size(Dict) == length(ShovelConfig) of
        true ->
            Fields = record_info(fields, shovel),
            Keys = dict:fetch_keys(Dict),
            case {Keys -- Fields, Fields -- Keys} of
                {[], []} ->
                    {_Pos, Dict1} =
                        lists:foldl(
                          fun (FieldName, {Pos, Acc}) ->
                                  {Pos + 1,
                                   dict:update(FieldName,
                                               fun (V) -> {V, Pos} end, Acc)}
                          end, {2, Dict}, Fields),
                    try
                        {ok, run_reader_state_monad(
                               [{fun parse_endpoint/1, sources},
                                {fun parse_endpoint/1, destinations},
                                {fun parse_non_negative_integer/1, qos},
                                {fun parse_boolean/1, auto_ack},
                                {fun parse_non_negative_integer/1, tx_size},
                                {fun parse_binary/1, queue},
                                make_parse_publish(publish_fields),
                                make_parse_publish(publish_properties),
                                {fun parse_non_negative_integer/1, reconnect}
                               ],
                               fun (_Key, {Value, Pos}, Shovel) ->
                                       setelement(Pos, Shovel, Value)
                               end, #shovel{}, Dict1)}
                    catch throw:{error, Reason} ->
                            {error, {error_when_parsing_shovel_configuration,
                                     ShovelName, Reason}}
                    end;
                {[], Missing} ->
                    {error, {missing_shovel_configuration_keys,
                             ShovelName, Missing}};
                {Unknown, _} ->
                    {error, {unrecognised_shovel_configuration_keys,
                             ShovelName, Unknown}}
            end;
        false ->
            {error, {duplicate_or_missing_fields_in_shovel_definition,
                     ShovelName}}
    end.

%% --=: Combined state-reader monad implementation start :=--
%% This is really a state + reader monad with an implicit initial ask,
%% and a combinator for constructing a final value (hence is also a
%% catamorphism).
run_reader_state_monad(FunList, Comb, State, Reader) ->
    lists:foldl(fun ({Fun, Key}, StateN) ->
                        case dict:find(Key, Reader) of
                            {ok, Value} -> try Comb(Key, Fun(Value), StateN)
                                           catch throw:{error, Reason} ->
                                                   fail({Key, Reason})
                                           end;
                            error       -> fail({missing, Key})
                        end
                end, State, FunList).

%% --=: Plain state monad implementation start :=--
run_state_monad(FunList, State) ->
    lists:foldl(fun (Fun, StateN) -> Fun(StateN) end, State, FunList).

return(V) -> V.

fail(Reason) -> throw({error, Reason}).
%% --=: end :=--

parse_endpoint({Endpoint, Pos}) ->
    Brokers = case proplists:get_value(brokers, Endpoint) of
                  undefined ->
                      case proplists:get_value(broker, Endpoint) of
                          undefined ->
                              fail({require_broker_or_brokers});
                          B when is_list(B) ->
                              [B];
                          B ->
                              fail({broker_should_be_string_uri, B})
                      end;
                  [B|_] = Bs when is_list(B) ->
                      Bs;
                  B ->
                      fail({require_a_list_of_brokers, B})
              end,
    {[], Brokers1} = run_state_monad(
                       lists:duplicate(length(Brokers), fun parse_uri/1),
                       {Brokers, []}),

    ResourceDecls =
        case proplists:get_value(declarations, Endpoint, []) of
            Decls when is_list(Decls) ->
                Decls;
            Decls ->
                fail({declarations_should_be_a_list, Decls})
        end,
    {[], ResourceDecls1} =
        run_state_monad(
          lists:duplicate(length(ResourceDecls), fun parse_declaration/1),
          {ResourceDecls, []}),

    return({#endpoint { amqp_params = Brokers1,
                        resource_declarations = lists:reverse(ResourceDecls1)},
            Pos}).

parse_declaration({[{Method, Props} | Rest], Acc}) ->
    FieldNames = rabbit_framing:method_fieldnames(Method),
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
                 end, {rabbit_framing:method_record(Method), 2}, FieldNames),
    return({Rest, [Res | Acc]});
parse_declaration({[Method | Rest], Acc}) ->
    parse_declaration({[{Method, []} | Rest], Acc}).

parse_uri({[Uri | Uris], Acc}) ->
    case uri_parser:parse(Uri, [{host, undefined}, {path, "/"},
                                {port, undefined}, {'query', []}]) of
        {error, Reason} ->
            fail({unable_to_parse_uri, Uri, Reason});
        Parsed ->
            Endpoint = case proplists:get_value(scheme, Parsed) of
                           "amqp"  -> build_plain_broker(Parsed);
                           "amqps" -> build_ssl_broker(Parsed);
                           Scheme  -> fail({unexpected_uri_scheme, Scheme, Uri})
                       end,
            return({Uris, [Endpoint | Acc]})
    end.

build_broker(ParsedUri) ->
    [Host, Port, Path] =
        [proplists:get_value(F, ParsedUri) || F <- [host, port, path]],
    VHost = case Path of
                "/"       -> <<"/">>;
                [$/|Rest] -> list_to_binary(Rest)
            end,
    Params = #amqp_params { host = Host, port = Port, virtual_host = VHost },
    case proplists:get_value(userinfo, ParsedUri) of
        [Username, Password | _ ] ->
            Params #amqp_params { username = list_to_binary(Username),
                                  password = list_to_binary(Password) };
        _ ->
            Params
    end.

build_plain_broker(ParsedUri) ->
    Params = build_broker(ParsedUri),
    case Params #amqp_params.port of
        undefined -> Params #amqp_params { port = ?PROTOCOL_PORT };
        _         -> Params
    end.

build_ssl_broker(ParsedUri) ->
    Params = build_broker(ParsedUri),
    Query = proplists:get_value('query', ParsedUri),
    SSLOptions = run_reader_state_monad(
                   [{fun find_path_parameter/1,    "cacertfile"},
                    {fun find_path_parameter/1,    "certfile"},
                    {fun find_path_parameter/1,    "keyfile"},
                    {fun find_atom_parameter/1,    "verify"},
                    {fun find_boolean_parameter/1, "fail_if_no_peer_cert"}],
                   fun (K, V, L) -> [{list_to_atom(K), V} | L] end,
                   [], dict:from_list(Query)),
    Params1 = Params #amqp_params { ssl_options = SSLOptions },
    case Params1 #amqp_params.port of
        undefined -> Params1 #amqp_params { port = ?PROTOCOL_PORT - 1 };
        _         -> Params1
    end.

find_path_parameter(Value) -> return(Value).

find_boolean_parameter(Value) ->
    Bool = list_to_atom(Value),
    case is_boolean(Bool) of
        true  -> return(Bool);
        false -> fail({require_boolean_value, Bool})
    end.

find_atom_parameter(Value) ->
    return(list_to_atom(Value)).

parse_non_negative_integer({N, Pos}) when is_integer(N) andalso N >= 0 ->
    return({N, Pos});
parse_non_negative_integer({N, _Pos}) ->
    fail({require_non_negative_integer, N}).

parse_boolean({Bool, Pos}) when is_boolean(Bool) ->
    return({Bool, Pos});
parse_boolean({NotABool, _Pos}) ->
    fail({require_boolean, NotABool}).

parse_binary({Binary, Pos}) when is_binary(Binary) ->
    return({Binary, Pos});
parse_binary({NotABinary, _Pos}) ->
    fail({require_binary, NotABinary}).

make_parse_publish(publish_fields) ->
    {make_parse_publish1(record_info(fields, 'basic.publish')), publish_fields};
make_parse_publish(publish_properties) ->
    {make_parse_publish1(record_info(fields, 'P_basic')), publish_properties}.

make_parse_publish1(ValidFields) ->
    fun ({Fields, Pos})
          when is_list(Fields) ->
            make_publish_fun(Fields, Pos, ValidFields);
        ({Fields, _Pos}) ->
            fail({require_list, Fields})
    end.

make_publish_fun(Fields, Pos, ValidFields) ->
    SuppliedFields = proplists:get_keys(Fields),
    case SuppliedFields -- ValidFields of
        [] ->
            FieldIndices =
                make_field_indices(ValidFields, SuppliedFields, Fields),
            Fun = fun (Publish) ->
                          lists:foldl(fun ({Pos1, Value}, Pub) ->
                                              setelement(Pos1, Pub, Value)
                                      end, Publish, FieldIndices)
                  end,
            return({Fun, Pos});
        Unexpected ->
            fail({unexpected_fields, Unexpected, ValidFields})
    end.

make_field_indices(Valid, Supplied, Fields) ->
    make_field_indices(Valid, Supplied, Fields, 2, []).

make_field_indices(_Valid, [], _Fields, _Idx, Acc) ->
    lists:reverse(Acc);
make_field_indices([F|Valid], [F|Supplied], Fields, Idx, Acc) ->
    Value = proplists:get_value(F, Fields),
    make_field_indices(Valid, Supplied, Fields, Idx+1, [{Idx, Value}|Acc]);
make_field_indices([_V|Valid], Supplied, Fields, Idx, Acc) ->
    make_field_indices(Valid, Supplied, Fields, Idx+1, Acc).
