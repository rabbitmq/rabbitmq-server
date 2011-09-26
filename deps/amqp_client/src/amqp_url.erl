%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(amqp_url).

-include("amqp_client.hrl").

-export([parse/1]).

%%---------------------------------------------------------------------------
%% AMQP URL Parsing
%%---------------------------------------------------------------------------

%% @spec (Url) -> {ok, #amqp_params_network{}} | {error, Info}
%% where
%%      Url =  string()
%%      Info = any()
%%
%% @doc Parses an AMQP URL.  If any of the URL parts are missing, the
%% default values are used. The connection is assumed to be a network
%% one, and an #amqp_params_network{} record is returned.  In case of
%% failure, an {error, Info} tuple is returned.
parse(Url) ->
    try parse1(Url)
    catch throw:Err -> {error, {Err, Url}}
    end.

parse1(Url) when is_list(Url) ->
    case uri_parser:parse(Url, [{host, undefined}, {path, "/"},
                                {port, undefined}, {'query', []}]) of
        {error, Err} ->
            throw(Err);
        Parsed ->
            Endpoint = case proplists:get_value(scheme, Parsed) of
                           "amqp"  -> build_broker(Parsed);
                           "amqps" -> build_ssl_broker(Parsed);
                           Scheme  -> fail({unexpected_url_scheme, Scheme, Url})
                       end,
            return({ok, broker_add_query(Endpoint, Parsed)})
    end;
parse1(Url) ->
    fail({expected_string_url, Url}).

unescape_string(undefined) ->
    undefined;
unescape_string([]) ->
    [];
unescape_string([$%, N1, N2 | Rest]) ->
    try
        [list_to_integer([N1, N2], 16) | unescape_string(Rest)]
    catch
        error:badarg -> throw({invalid_entitiy, ['%', N1, N2]})
    end;
unescape_string([$% | Rest]) ->
    fail({unterminated_entity, ['%' | Rest]});
unescape_string([C | Rest]) ->
    [C | unescape_string(Rest)].

build_broker(ParsedUrl) ->
    [Host, Port, Path] =
        [proplists:get_value(F, ParsedUrl) || F <- [host, port, path]],
    case Port =:= undefined orelse (0 < Port andalso Port < 65535) of
        true  -> ok;
        false -> fail({port_out_of_range, Port})
    end,
    VHost = case Path of
                "/"       -> <<"">>;
                [$/|Rest] -> case string:chr(Rest, $/) of
                                 0 -> list_to_binary(unescape_string(Rest));
                                 _ -> fail({invalid_vhost, Rest})
                             end
            end,
    UserInfo = proplists:get_value(userinfo, ParsedUrl),
    Ps = #amqp_params_network{host = unescape_string(Host),
                              port = Port,
                              virtual_host = VHost},
    case UserInfo of
        [U, P | _] -> Ps#amqp_params_network{
                        username = list_to_binary(unescape_string(U)),
                        password = list_to_binary(unescape_string(P))};
        [U | _]    -> Ps#amqp_params_network{
                        username = list_to_binary(unescape_string(U))};
        _          -> Ps
    end.

build_ssl_broker(ParsedUrl) ->
    Params = build_broker(ParsedUrl),
    Query = proplists:get_value('query', ParsedUrl),
    SSLOptions =
        run_state_monad(
          [fun (L) -> KeyString = atom_to_list(Key),
                      case lists:keysearch(KeyString, 1, Query) of
                          {value, {_, Value}} ->
                              try return([{Key, unescape_string(Fun(Value))} | L])
                              catch throw:{error, Reason} ->
                                      fail({invalid_ssl_parameter,
                                            Key, Value, Query, Reason})
                              end;
                          false ->
                              fail({missing_ssl_parameter, Key, Query})
                      end
           end || {Fun, Key} <-
                      [{fun find_path_parameter/1,    cacertfile},
                       {fun find_path_parameter/1,    certfile},
                       {fun find_path_parameter/1,    keyfile},
                       {fun find_atom_parameter/1,    verify},
                       {fun find_boolean_parameter/1, fail_if_no_peer_cert}]],
          []),
    Params#amqp_params_network{ssl_options = SSLOptions}.

broker_add_query(Params = #amqp_params_network{}, Url) ->
    broker_add_query(Params, Url, record_info(fields, amqp_params_network)).

broker_add_query(Params, ParsedUrl, Fields) ->
    Query = proplists:get_value('query', ParsedUrl),
    {Params1, _Pos} =
        run_state_monad(
          [fun ({ParamsN, Pos}) ->
                   Pos1 = Pos + 1,
                   KeyString = atom_to_list(Field),
                   case proplists:get_value(KeyString, Query) of
                       undefined ->
                           return({ParamsN, Pos1});
                       true -> %% proplists short form, not permitted
                           return({ParamsN, Pos1});
                       Value ->
                           try
                               ValueParsed = parse_amqp_param(Field, Value),
                               return(
                                 {setelement(Pos, ParamsN, ValueParsed), Pos1})
                           catch throw:{error, Reason} ->
                                   fail({invalid_amqp_params_parameter,
                                         Field, Value, Query, Reason})
                           end
                   end
           end || Field <- Fields], {Params, 2}),
    Params1.

parse_amqp_param(Field, String) when Field =:= channel_max orelse
                                     Field =:= frame_max   orelse
                                     Field =:= heartbeat   ->
    try return(list_to_integer(String))
    catch error:badarg -> fail({not_an_integer, String})
    end;
parse_amqp_param(Field, String) ->
    fail({parameter_unconfigurable_in_query, Field, String}).

find_path_parameter(Value) -> return(Value).

find_boolean_parameter(Value) ->
    Bool = list_to_atom(Value),
    case is_boolean(Bool) of
        true  -> return(Bool);
        false -> fail({require_boolean, Bool})
    end.

find_atom_parameter(Value) ->
    return(list_to_atom(Value)).

%% --=: Plain state monad implementation start :=--
run_state_monad(FunList, State) ->
    lists:foldl(fun (Fun, StateN) -> Fun(StateN) end, State, FunList).

return(V) -> V.

fail(Reason) -> throw(Reason).
%% --=: end :=--
