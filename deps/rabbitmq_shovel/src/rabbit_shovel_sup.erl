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
    io:format("Configs: ~p~n", [dict:to_list(Configurations)]),
    {ok, {{one_for_one, 10, 1},
          [{rabbit_shovel_worker_a,
            {rabbit_shovel_worker, start_link, [10000]},
            {permanent, 1},
            16#ffffffff,
            worker,
            [rabbit_shovel_worker]},
           {rabbit_shovel_worker_b,
            {rabbit_shovel_worker, start_link, [5]},
            {permanent, 3},
            16#ffffffff,
            worker,
            [rabbit_shovel_worker]}
          ]}}.

parse_configuration(undefined) ->
    {error, no_shovels_configured};
parse_configuration({ok, Env}) ->
    parse_configuration(Env, dict:new()).

parse_configuration([], Acc) ->
    {ok, Acc};
parse_configuration([{ShovelName, ShovelConfig} | Env], Acc) ->
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
    end.

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
                               [{fun parse_uris/3, sources},
                                {fun parse_uris/3, destinations},
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
    {ok, State1} = Fun(dict:fetch(Key, Reader), Key, State),
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

parse_uris({{Uris = [Uri|_], QueueExchange, ResourceDecls}, Pos}, _FieldName,
           Shovel) when is_list(Uri) ->
    Len = length(Uris),
    {[], ParsedUris} =
        run_state_monad(lists:duplicate(Len, fun parse_uri/1), {Uris, []}),
    Endpoint = #endpoint { amqp_params = ParsedUris,
                           queue_or_exchange = QueueExchange,
                           resource_declarations = ResourceDecls },
    return(setelement(Pos, Shovel, Endpoint));
parse_uris({{Uri, QueueExchange, ResourceDecls}, Pos}, FieldName, Shovel)
  when is_list(Uri) ->
    parse_uris({{[Uri], QueueExchange, ResourceDecls}, Pos}, FieldName, Shovel);
parse_uris({{Uri, QueueExchange}, Pos}, FieldName, Shovel) when is_list(Uri) ->
    parse_uris({{Uri, QueueExchange, undefined}, Pos}, FieldName, Shovel);
parse_uris({NotUri, _Pos}, FieldName, _Shovel) ->
    fail({require_something_closer_to_a_uri_and_queue_or_exchange,
          FieldName, NotUri}).

parse_uri({[Uri | Uris], Acc}) ->
    case uri_parser:parse(Uri, [{host, undefined}, {path, "/"}]) of
        {error, Reason} ->
            fail({unable_to_parse_uri, Uri, Reason});
        Parsed ->
            Endpoint =
                run_state_monad(
                  [fun (_) ->
                           case lists:keysearch(scheme, 1, Parsed) of
                               {value, {scheme, "amqp"}} ->
                                   build_endpoint(Parsed);
                               {value, {scheme, "amqps"}} ->
                                   build_ssl_endpoint(Parsed);
                               {value, {scheme, Scheme}} ->
                                   fail({unexpected_uri_scheme, Scheme, Uri})
                           end
                  end], undefined),
            return({Uris, [Endpoint | Acc]})
    end.

build_endpoint(ParsedUri) ->
    {value, {host, Host}} = lists:keysearch(host, 1, ParsedUri),
    {value, {path, Path}} = lists:keysearch(path, 1, ParsedUri),
    Params = #amqp_params { host = Host, virtual_host = list_to_binary(Path) },
    Params1 =
        case lists:keysearch(userinfo, 1, ParsedUri) of
            {value, {userinfo, [Username, Password | _ ]}} ->
                Params #amqp_params { username = list_to_binary(Username),
                                      password = list_to_binary(Password) };
            _ ->
                Params
        end,
    return(Params1).

build_ssl_endpoint(ParsedUri) ->
    #amqp_params{}.

parse_non_negative_integer({N, Pos}, _FieldName, Shovel)
  when is_integer(N) andalso N >= 0 ->
    return(setelement(Pos, Shovel, N));
parse_non_negative_integer({N, _Pos}, FieldName, _Shovel) ->
    fail({require_non_negative_integer_in_field, FieldName, N}).

parse_boolean({Bool, Pos}, _FieldName, Shovel)
  when is_boolean(Bool) ->
    return(setelement(Pos, Shovel, Bool));
parse_boolean({NotABool, _Pos}, FieldName, _Shovel) ->
    fail({require_boolean_in_field, FieldName, NotABool}).

parse_delivery_mode({N, Pos}, _FieldName, Shovel)
  when N =:= 0 orelse N =:= 2 ->
    return(setelement(Pos, Shovel, N));
parse_delivery_mode({keep, Pos}, _FieldName, Shovel) ->
    return(setelement(Pos, Shovel, keep));
parse_delivery_mode({N, _Pos}, FieldName, _Shovel) ->
    fail({require_valid_delivery_mode_in_field, FieldName, N}).
