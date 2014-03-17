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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(amqp_uri).

-include("amqp_client.hrl").

-export([parse/1, parse/2, remove_credentials/1]).

%%---------------------------------------------------------------------------
%% AMQP URI Parsing
%%---------------------------------------------------------------------------

%% Reformat a URI to remove authentication secrets from it (before we
%% log it or display it anywhere).
remove_credentials(URI) ->
    Props = uri_parser:parse(URI,
                             [{host, undefined}, {path, undefined},
                              {port, undefined}, {'query', []}]),
    PortPart = case proplists:get_value(port, Props) of
                   undefined -> "";
                   Port      -> rabbit_misc:format(":~B", [Port])
               end,
    PGet = fun(K, P) -> case proplists:get_value(K, P) of
                            undefined -> "";
                            R         -> R
                        end
           end,
    rabbit_misc:format(
      "~s://~s~s~s", [proplists:get_value(scheme, Props), PGet(host, Props),
                      PortPart,                           PGet(path, Props)]).

%% @spec (Uri) -> {ok, #amqp_params_network{} | #amqp_params_direct{}} |
%%                {error, {Info, Uri}}
%% where
%%      Uri  = string()
%%      Info = any()
%%
%% @doc Parses an AMQP URI.  If any of the URI parts are missing, the
%% default values are used.  If the hostname is zero-length, an
%% #amqp_params_direct{} record is returned; otherwise, an
%% #amqp_params_network{} record is returned.  Extra parameters may be
%% specified via the query string
%% (e.g. "?heartbeat=5&amp;auth_mechanism=external"). In case of failure,
%% an {error, {Info, Uri}} tuple is returned.
%%
%% The extra parameters that may be specified are channel_max,
%% frame_max, heartbeat and auth_mechanism (the latter can appear more
%% than once).  The extra parameters that may be specified for an SSL
%% connection are cacertfile, certfile, keyfile, verify, and
%% fail_if_no_peer_cert.
parse(Uri) -> parse(Uri, <<"/">>).

parse(Uri, DefaultVHost) ->
    try return(parse1(Uri, DefaultVHost))
    catch throw:Err -> {error, {Err, Uri}};
          error:Err -> {error, {Err, Uri}}
    end.

parse1(Uri, DefaultVHost) when is_list(Uri) ->
    case uri_parser:parse(Uri, [{host, undefined}, {path, undefined},
                                {port, undefined}, {'query', []}]) of
        {error, Err} ->
            throw({unable_to_parse_uri, Err});
        Parsed ->
            Endpoint =
                case string:to_lower(proplists:get_value(scheme, Parsed)) of
                    "amqp"  -> build_broker(Parsed, DefaultVHost);
                    "amqps" -> build_ssl_broker(Parsed, DefaultVHost);
                    Scheme  -> fail({unexpected_uri_scheme, Scheme})
                end,
            return({ok, broker_add_query(Endpoint, Parsed)})
    end;
parse1(_, _DefaultVHost) ->
    fail(expected_string_uri).

unescape_string(Atom) when is_atom(Atom) ->
    Atom;
unescape_string([]) ->
    [];
unescape_string([$%, N1, N2 | Rest]) ->
    try
        [erlang:list_to_integer([N1, N2], 16) | unescape_string(Rest)]
    catch
        error:badarg -> throw({invalid_entitiy, ['%', N1, N2]})
    end;
unescape_string([$% | Rest]) ->
    fail({unterminated_entity, ['%' | Rest]});
unescape_string([C | Rest]) ->
    [C | unescape_string(Rest)].

build_broker(ParsedUri, DefaultVHost) ->
    [Host, Port, Path] =
        [proplists:get_value(F, ParsedUri) || F <- [host, port, path]],
    case Port =:= undefined orelse (0 < Port andalso Port =< 65535) of
        true  -> ok;
        false -> fail({port_out_of_range, Port})
    end,
    VHost = case Path of
                undefined -> DefaultVHost;
                [$/|Rest] -> case string:chr(Rest, $/) of
                                 0 -> list_to_binary(unescape_string(Rest));
                                 _ -> fail({invalid_vhost, Rest})
                             end
            end,
    UserInfo = proplists:get_value(userinfo, ParsedUri),
    set_user_info(case unescape_string(Host) of
                      undefined -> #amqp_params_direct{virtual_host = VHost};
                      Host1     -> Mech = mechanisms(ParsedUri),
                                   #amqp_params_network{host            = Host1,
                                                        port            = Port,
                                                        virtual_host    = VHost,
                                                        auth_mechanisms = Mech}
                  end, UserInfo).

set_user_info(Ps, UserInfo) ->
    case UserInfo of
        [U, P | _] -> set([{username, list_to_binary(unescape_string(U))},
                           {password, list_to_binary(unescape_string(P))}], Ps);

        [U]        -> set([{username, list_to_binary(unescape_string(U))}], Ps);
        []         -> Ps
    end.

set(KVs, Ps = #amqp_params_direct{}) ->
    set(KVs, Ps, record_info(fields, amqp_params_direct));
set(KVs, Ps = #amqp_params_network{}) ->
    set(KVs, Ps, record_info(fields, amqp_params_network)).

set(KVs, Ps, Fields) ->
    {Ps1, _Ix} = lists:foldl(fun (Field, {PsN, Ix}) ->
                                     {case lists:keyfind(Field, 1, KVs) of
                                          false  -> PsN;
                                          {_, V} -> setelement(Ix, PsN, V)
                                      end, Ix + 1}
                             end, {Ps, 2}, Fields),
    Ps1.

build_ssl_broker(ParsedUri, DefaultVHost) ->
    Params = build_broker(ParsedUri, DefaultVHost),
    Query = proplists:get_value('query', ParsedUri),
    SSLOptions =
        run_state_monad(
          [fun (L) -> KeyString = atom_to_list(Key),
                      case lists:keysearch(KeyString, 1, Query) of
                          {value, {_, Value}} ->
                              try return([{Key, unescape_string(Fun(Value))} | L])
                              catch throw:Reason ->
                                      fail({invalid_ssl_parameter,
                                            Key, Value, Query, Reason})
                              end;
                          false ->
                              L
                      end
           end || {Fun, Key} <-
                      [{fun find_path_parameter/1,    cacertfile},
                       {fun find_path_parameter/1,    certfile},
                       {fun find_path_parameter/1,    keyfile},
                       {fun find_atom_parameter/1,    verify},
                       {fun find_boolean_parameter/1, fail_if_no_peer_cert}]],
          []),
    Params#amqp_params_network{ssl_options = SSLOptions}.

broker_add_query(Params = #amqp_params_direct{}, Uri) ->
    broker_add_query(Params, Uri, record_info(fields, amqp_params_direct));
broker_add_query(Params = #amqp_params_network{}, Uri) ->
    broker_add_query(Params, Uri, record_info(fields, amqp_params_network)).

broker_add_query(Params, ParsedUri, Fields) ->
    Query = proplists:get_value('query', ParsedUri),
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
                           catch throw:Reason ->
                                   fail({invalid_amqp_params_parameter,
                                         Field, Value, Query, Reason})
                           end
                   end
           end || Field <- Fields], {Params, 2}),
    Params1.

parse_amqp_param(Field, String) when Field =:= channel_max        orelse
                                     Field =:= frame_max          orelse
                                     Field =:= heartbeat          orelse
                                     Field =:= connection_timeout ->
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

find_atom_parameter(Value) -> return(list_to_atom(Value)).

mechanisms(ParsedUri) ->
    Query = proplists:get_value('query', ParsedUri),
    Mechanisms = case proplists:get_all_values("auth_mechanism", Query) of
                     []    -> ["plain", "amqplain"];
                     Mechs -> Mechs
                 end,
    [case [list_to_atom(T) || T <- string:tokens(Mech, ":")] of
         [F]    -> fun (R, P, S) -> amqp_auth_mechanisms:F(R, P, S) end;
         [M, F] -> fun (R, P, S) -> M:F(R, P, S) end;
         L      -> throw({not_mechanism, L})
     end || Mech <- Mechanisms].

%% --=: Plain state monad implementation start :=--
run_state_monad(FunList, State) ->
    lists:foldl(fun (Fun, StateN) -> Fun(StateN) end, State, FunList).

return(V) -> V.

fail(Reason) -> throw(Reason).
%% --=: end :=--
