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
    {ok, {{one_for_one, 2*Len, 2}, make_child_specs(Configurations)}}.

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
    parse_configuration(Env, dict:new()).

parse_configuration([], Acc) ->
    {ok, Acc};
parse_configuration([{ShovelName, ShovelConfig} | Env], Acc)
  when is_atom(ShovelName) ->
    case dict:is_key(ShovelName, Acc) of
        true ->
            {error, {duplicate_shovel_definition, ShovelName}};
        false ->
            case parse_shovel_config(ShovelName, ShovelConfig) of
                {ok, Config} ->
                    parse_configuration(
                      Env, dict:store(ShovelName, Config, Acc));
                {error, Reason} ->
                    {error, Reason}
            end
    end;
parse_configuration(_, _Acc) ->
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
                               [{fun parse_endpoint/3, sources},
                                {fun parse_endpoint/3, destinations},
                                {fun parse_non_negative_integer/3, qos},
                                {fun parse_boolean/3, auto_ack},
                                {fun parse_non_negative_integer/3, tx_size},
                                {fun parse_delivery_mode/3, delivery_mode},
                                {fun parse_non_negative_integer/3, reconnect}
                               ], #shovel{}, Dict1)}
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
run_reader_state_monad(FunList, State, Reader) ->
    {ok, State1, _Reader} =
        lists:foldl(fun reader_state_monad/2,
                    {ok, State, Reader},
                    FunList),
    State1.

reader_state_monad({Fun, Key}, {ok, State, Reader}) ->
    {ok, State1} = Fun(dict:find(Key, Reader), Key, State),
    {ok, State1, Reader}.

%% --=: Plain state monad implementation start :=--
run_state_monad(FunList, State) ->
    {ok, State1} =
        lists:foldl(fun state_monad/2, {ok, State}, FunList),
    State1.

state_monad(Fun, {ok, State}) ->
    Fun(State).

return(State) -> {ok, State}.

fail(Reason) -> throw({error, Reason}).
%% --=: end :=--

parse_endpoint({ok, {Endpoint, Pos}}, FieldName, Shovel) ->
    Brokers = case proplists:get_value(brokers, Endpoint) of
                  undefined ->
                      case proplists:get_value(broker, Endpoint) of
                          undefined ->
                              fail({require_broker_or_brokers, FieldName});
                          B when is_list(B) ->
                              [B];
                          B ->
                              fail({broker_should_be_string_uri, B, FieldName})
                      end;
                  [B|_] = Bs when is_list(B) ->
                      Bs;
                  B ->
                      fail({require_a_list_of_brokers, B, FieldName})
              end,
    {[], Brokers1} = run_state_monad(
                       lists:duplicate(length(Brokers), fun parse_uri/1),
                       {Brokers, []}),

    QueueExchangeField = case FieldName of
                             sources      -> queue;
                             destinations -> exchange
                         end,
    QueueExchange =
        case proplists:get_value(QueueExchangeField, Endpoint) of
            undefined ->
                fail({field_required, QueueExchangeField, FieldName});
            Value when is_binary(Value) ->
                Value;
            Value ->
                fail({need_binary_for_field, QueueExchangeField, Value, FieldName})
        end,

    ResourceDecls =
        case proplists:get_value(declarations, Endpoint, []) of
            Decls when is_list(Decls) ->
                Decls;
            Decls ->
                fail({declarations_should_be_a_list, Decls, FieldName})
        end,
    {[], ResourceDecls1} =
        run_state_monad(
          lists:duplicate(length(ResourceDecls), fun parse_declaration/1),
          {ResourceDecls, []}),

    Result = #endpoint { amqp_params = Brokers1,
                         queue_or_exchange = QueueExchange,
                         resource_declarations = lists:reverse(ResourceDecls1) },
    return(setelement(Pos, Shovel, Result)).

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
    return({Rest, [Res | Acc]}).

parse_uri({[Uri | Uris], Acc}) ->
    case uri_parser:parse(Uri, [{host, undefined}, {path, "/"},
                                {port, undefined}, {'query', []}]) of
        {error, Reason} ->
            fail({unable_to_parse_uri, Uri, Reason});
        Parsed ->
            Endpoint =
                run_state_monad(
                  [fun (_) ->
                           case proplists:get_value(scheme, Parsed) of
                               "amqp" ->
                                   build_plain_broker(Parsed);
                               "amqps" ->
                                   build_ssl_broker(Parsed);
                               Scheme ->
                                   fail({unexpected_uri_scheme, Scheme, Uri})
                           end
                   end], undefined),
            return({Uris, [Endpoint | Acc]})
    end.

build_broker(ParsedUri) ->
    [Host, Port, Path] =
        [proplists:get_value(F, ParsedUri) || F <- [host, port, path]],
    Params = #amqp_params { host = Host, port = Port,
                            virtual_host = list_to_binary(Path) },
    Params1 =
        case proplists:get_value(userinfo, ParsedUri) of
            [Username, Password | _ ] ->
                Params #amqp_params { username = list_to_binary(Username),
                                      password = list_to_binary(Password) };
            _ ->
                Params
        end,
    return(Params1).

build_plain_broker(ParsedUri) ->
    Params = run_state_monad([fun build_broker/1], ParsedUri),
    return(case Params #amqp_params.port of
               undefined -> Params #amqp_params { port = ?PROTOCOL_PORT };
               _         -> Params
           end).

build_ssl_broker(ParsedUri) ->
    Params = run_state_monad([fun build_broker/1], ParsedUri),
    Query = proplists:get_value('query', ParsedUri),
    SSLOptions = run_reader_state_monad(
                   [{fun find_path_parameter/3,    "cacertfile"},
                    {fun find_path_parameter/3,    "certfile"},
                    {fun find_path_parameter/3,    "keyfile"},
                    {fun find_boolean_parameter/3, "verify"},
                    {fun find_boolean_parameter/3, "fail_if_no_peer_cert"}],
                   [], dict:from_list(Query)),
    Params1 = Params #amqp_params { ssl_options = SSLOptions },
    return(case Params1 #amqp_params.port of
               undefined -> Params1 #amqp_params { port = ?PROTOCOL_PORT - 1 };
               _         -> Params1
           end).

find_path_parameter({ok, Value}, FieldName, Acc) ->
    return([{list_to_atom(FieldName), Value} | Acc]);
find_path_parameter(error, FieldName, _Acc) ->
    fail({require_field, list_to_atom(FieldName)}).

find_boolean_parameter({ok, Value}, FieldName, Acc) ->
    Bool = list_to_atom(Value),
    Field = list_to_atom(FieldName),
    case is_boolean(Bool) of
        true  -> return([{Field, Bool} | Acc]);
        false -> fail({require_boolean_for_field, Field, Bool})
    end;
find_boolean_parameter(error, FieldName, _Acc) ->
    fail({require_boolean_field, list_to_atom(FieldName)}).

parse_non_negative_integer({ok, {N, Pos}}, _FieldName, Shovel)
  when is_integer(N) andalso N >= 0 ->
    return(setelement(Pos, Shovel, N));
parse_non_negative_integer({ok, {N, _Pos}}, FieldName, _Shovel) ->
    fail({require_non_negative_integer_in_field, FieldName, N});
parse_non_negative_integer(error, FieldName, _Shovel) ->
    fail({require_field, FieldName}).

parse_boolean({ok, {Bool, Pos}}, _FieldName, Shovel)
  when is_boolean(Bool) ->
    return(setelement(Pos, Shovel, Bool));
parse_boolean({ok, {NotABool}, _Pos}, FieldName, _Shovel) ->
    fail({require_boolean_in_field, FieldName, NotABool});
parse_boolean(error, FieldName, _Shovel) ->
    fail({require_field, FieldName}).

parse_delivery_mode({ok, {N, Pos}}, _FieldName, Shovel)
  when N =:= 0 orelse N =:= 2 ->
    return(setelement(Pos, Shovel, N));
parse_delivery_mode({ok, {keep, Pos}}, _FieldName, Shovel) ->
    return(setelement(Pos, Shovel, keep));
parse_delivery_mode({ok, {N, _Pos}}, FieldName, _Shovel) ->
    fail({require_valid_delivery_mode_in_field, FieldName, N});
parse_delivery_mode(error, FieldName, _Shovel) ->
    fail({require_field, FieldName}).
