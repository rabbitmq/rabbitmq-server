%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2022 VMware, Inc. or its affiliates. All rights reserved.
%%
-module(rabbit_peer_discovery_util).

%% API
-export([getenv/1,
         as_atom/1,
         as_integer/1,
         as_string/1,
         as_list/1,
         nic_ipv4/1,
         node_hostname/1,
         node_name/1,
         parse_port/1,
         as_proplist/1,
         as_map/1,
         stringify_error/1,
         maybe_backend_configured/4
        ]).

-include_lib("kernel/include/logger.hrl").
-include("rabbit_peer_discovery.hrl").

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-type ifopt() :: {flag,[atom()]} | {addr, inet:ip_address()} |
                 {netmask,inet:ip_address()} | {broadaddr,inet:ip_address()} |
                 {dstaddr,inet:ip_address()} | {hwaddr,[byte()]}.

-type stringifyable() :: atom() | binary() | string() | integer().


-spec getenv(Key :: string() | undefined) -> string() | false.
getenv(undefined) ->
  false;
getenv(Key) ->
  process_getenv_value(Key, os:getenv(Key)).

-define(DEFAULT_NODE_PREFIX, "rabbit").

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check the response of os:getenv/1 to see if it's false and if it is
%% chain down to maybe_getenv_with_subkey/2 to see if the environment
%% variable has a prefix of RABBITMQ_, potentially trying to get an
%% environment variable without the prefix.
%% @end
%%--------------------------------------------------------------------
-spec process_getenv_value(Key :: string(), Value :: string() | false)
    -> string() | false.
process_getenv_value(Key, false) ->
  maybe_getenv_with_subkey(Key, string:left(Key, 9));
process_getenv_value(_, Value) -> Value.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check to see if the OS environment variable starts with RABBITMQ_
%% and if so, try and fetch the value from an environment variable
%% with the prefix removed.
%% @end
%%--------------------------------------------------------------------
-spec maybe_getenv_with_subkey(Key :: string(), Prefix :: string())
    -> string() | false.
maybe_getenv_with_subkey(Key, "RABBITMQ_") ->
  os:getenv(string:sub_string(Key, 10));
maybe_getenv_with_subkey(_, _) ->
  false.


%%--------------------------------------------------------------------
%% @doc
%% Return the passed in value as an atom.
%% @end
%%--------------------------------------------------------------------
-spec as_atom(atom() | binary() | string()) -> atom().
as_atom(Value) when is_atom(Value) ->
  Value;
as_atom(Value) when is_binary(Value) ->
  list_to_atom(binary_to_list(Value));
as_atom(Value) when is_list(Value) ->
  list_to_atom(Value);
as_atom(Value) ->
  ?LOG_ERROR(
     "Unexpected data type for atom value: ~p",
     [Value],
     #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  Value.


%%--------------------------------------------------------------------
%% @doc
%% Return the passed in value as an integer.
%% @end
%%--------------------------------------------------------------------
-spec as_integer(binary() | integer() | string()) -> integer().
as_integer([]) -> undefined;
as_integer(Value) when is_binary(Value) ->
  list_to_integer(as_string(Value));
as_integer(Value) when is_list(Value) ->
  list_to_integer(Value);
as_integer(Value) when is_integer(Value) ->
  Value;
as_integer(Value) ->
  ?LOG_ERROR(
     "Unexpected data type for integer value: ~p",
     [Value],
     #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  Value.


%%--------------------------------------------------------------------
%% @doc
%% Return the passed in value as a string.
%% @end
%%--------------------------------------------------------------------
-spec as_string(Value :: atom() | binary() | integer() | string())
    -> string().
as_string([]) -> "";
as_string(Value) when is_atom(Value) ->
  as_string(atom_to_list(Value));
as_string(Value) when is_binary(Value) ->
  as_string(binary_to_list(Value));
as_string(Value) when is_integer(Value) ->
  as_string(integer_to_list(Value));
as_string(Value) when is_list(Value) ->
  lists:flatten(Value);
as_string(Value) ->
  ?LOG_ERROR(
     "Unexpected data type for list value: ~p",
     [Value],
     #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  Value.


%%--------------------------------------------------------------------
%% @doc
%% Return the IP address for the current node for the specified
%% network interface controller.
%% @end
%%--------------------------------------------------------------------
-spec nic_ipv4(Device :: string())
    -> {ok, string()} | {error, not_found}.
nic_ipv4(Device) ->
  {ok, Interfaces} = inet:getifaddrs(),
  nic_ipv4(Device, Interfaces).


%%--------------------------------------------------------------------
%% @doc
%% Parse the interface out of the list of interfaces, returning the
%% IPv4 address if found.
%% @end
%%--------------------------------------------------------------------
-spec nic_ipv4(Device :: string(), Interfaces :: list())
    -> {ok, string()} | {error, not_found}.
nic_ipv4(_, []) -> {error, not_found};
nic_ipv4(Device, [{Interface, Opts}|_]) when Interface =:= Device ->
  {ok, nic_ipv4_address(Opts)};
nic_ipv4(Device, [_|T]) ->
  nic_ipv4(Device,T).


%%--------------------------------------------------------------------
%% @doc
%% Return the formatted IPv4 address out of the list of addresses
%% for the interface.
%% @end
%%--------------------------------------------------------------------
-spec nic_ipv4_address([ifopt()]) -> {ok, string()} | {error, not_found}.
nic_ipv4_address([]) -> {error, not_found};
nic_ipv4_address([{addr, {A,B,C,D}}|_]) ->
  inet_parse:ntoa({A,B,C,D});
nic_ipv4_address([_|T]) ->
  nic_ipv4_address(T).


%%--------------------------------------------------------------------
%% @doc
%% Return the hostname for the current node (without the tuple)
%% Hostname can be taken from network info or nodename.
%% @end
%%--------------------------------------------------------------------
-spec node_hostname(boolean()) -> string().
node_hostname(false = _FromNodename) ->
    {ok, Hostname} = inet:gethostname(),
    Hostname;
node_hostname(true = _FromNodename) ->
    {match, [Hostname]} = re:run(atom_to_list(node()), "@(.*)",
                                 [{capture, [1], list}]),
    Hostname.

%%--------------------------------------------------------------------
%% @doc
%% Return the proper node name for clustering purposes
%% @end
%%--------------------------------------------------------------------
-spec node_name(Value :: atom() | binary() | string()) -> atom().
node_name(Value) when is_atom(Value) ->
    node_name(atom_to_list(Value));
node_name(Value) when is_binary(Value) ->
    node_name(binary_to_list(Value));
node_name(Value) when is_list(Value) ->
  case lists:member($@, Value) of
      true ->
          list_to_atom(Value);
      false ->
          list_to_atom(string:join([node_prefix(),
                                    node_name_parse(as_string(Value))],
                                   "@"))
  end.

%%--------------------------------------------------------------------
%% @doc
%% Parse the value passed into nodename, checking if it's an IP
%% address. If so, return it. If not, then check to see if longname
%% support is turned on. if so, return it. If not, extract the left
%% most part of the name, delimited by periods.
%% @end
%%--------------------------------------------------------------------
-spec node_name_parse(Value :: string()) -> string().
node_name_parse(Value) ->
  %% TODO: support IPv6 here
  case inet:parse_ipv4strict_address(Value) of
    {ok, _} ->
      Value;
    {error, einval} ->
      node_name_parse(net_kernel:longnames(), Value)
  end.


%%--------------------------------------------------------------------
%% @doc
%% Continue the parsing logic from node_name_parse/1. This is where
%% result of the IPv4 check is processed.
%% @end
%%--------------------------------------------------------------------
-spec node_name_parse(IsIPv4 :: true | false, Value :: string())
    -> string().
node_name_parse(true, Value) -> Value;
node_name_parse(false, Value) ->
  Parts = string:tokens(Value, "."),
  node_name_parse(length(Parts), Value, Parts).


%%--------------------------------------------------------------------
%% @doc
%% Properly deal with returning the hostname if it's made up of
%% multiple segments like www.rabbitmq.com, returning www, or if it's
%% only a single segment, return that.
%% @end
%%--------------------------------------------------------------------
-spec node_name_parse(Segments :: integer(),
                      Value :: string(),
                      Parts :: [string()])
    -> string().
node_name_parse(1, Value, _) -> Value;
node_name_parse(_, _, Parts) ->
  as_string(lists:nth(1, Parts)).


%%--------------------------------------------------------------------
%% @doc
%% Extract the "local part" of the ``RABBITMQ_NODENAME`` environment
%% variable, if set, otherwise use the default node name value
%% (rabbit).
%% @end
%%--------------------------------------------------------------------
-spec node_prefix() -> string().
node_prefix() ->
  case getenv("RABBITMQ_NODENAME") of
      false -> ?DEFAULT_NODE_PREFIX;
      Value ->
          rabbit_data_coercion:to_list(getenv("RABBITMQ_NODENAME")),
          lists:nth(1, string:tokens(Value, "@"))
  end.


%%--------------------------------------------------------------------
%% @doc
%% Returns the port, even if Docker linking overwrites a configuration
%% value to be a URI instead of numeric value
%% @end
%%--------------------------------------------------------------------
-spec parse_port(Value :: integer() | string()) -> integer().
parse_port(Value) when is_list(Value) ->
  as_integer(lists:last(string:tokens(Value, ":")));
parse_port(Value) -> as_integer(Value).


%%--------------------------------------------------------------------
%% @doc
%% Returns a proplist from a JSON structure (environment variable) or
%% the settings key (already a proplist).
%% @end
%%--------------------------------------------------------------------
-spec as_proplist(Value :: string() | [{string(), string()}]) ->
                         [{string(), string()}].
as_proplist([Tuple | _] = Json) when is_tuple(Tuple) ->
    Json;
as_proplist([]) ->
    [];
as_proplist(List) when is_list(List) ->
    Value = rabbit_data_coercion:to_binary(List),
    case rabbit_json:try_decode(Value) of
        {ok, Map} ->
            [{binary_to_list(K), binary_to_list(V)}
             || {K, V} <- maps:to_list(Map)];
        {error, Error} ->
            ?LOG_ERROR(
               "Unexpected data type for proplist value: ~p. JSON parser returned an error: ~p!",
               [Value, Error],
               #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
            []
    end;
as_proplist(Value) ->
    ?LOG_ERROR(
       "Unexpected data type for proplist value: ~p.",
       [Value],
       #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
    [].

%%--------------------------------------------------------------------
%% @doc
%% Returns a map from a JSON structure (environment variable) or
%% the settings key, converts from proplists.
%% @end
%%--------------------------------------------------------------------
-spec as_map(Value :: string() | [{string(), string()}]) ->
                    #{}.
as_map([Tuple | _] = Json) when is_tuple(Tuple) ->
    maps:from_list(Json);
as_map([]) ->
    #{};
as_map(List) when is_list(List) ->
    Value = rabbit_data_coercion:to_binary(List),
    case rabbit_json:try_decode(Value) of
        {ok, Map} ->
            Map;
        {error, Error} ->
            ?LOG_ERROR(
               "Unexpected data type for map value: ~p. JSON parser returned an error: ~p!",
               [Value, Error],
               #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
            []
    end;
as_map(Map) when is_map(Map) ->
    Map;
as_map(Value) ->
    ?LOG_ERROR(
       "Unexpected data type for map value: ~p.",
       [Value],
       #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
    [].

-spec stringify_error({ok, term()} | {error, term()}) -> {ok, term()} | {error, string()}.
stringify_error({ok, Val}) ->
    {ok, Val};
stringify_error({error, Str}) when is_list(Str) ->
    {error, Str};
stringify_error({error, Term}) ->
    {error, lists:flatten(io_lib:format("~p", [Term]))}.

-spec maybe_backend_configured(BackendConfigKey :: atom(),
                               ClusterFormationUndefinedFun :: fun(() -> {ok, term()} | ok),
                               BackendUndefinedFun :: fun(() -> {ok, term()} | ok),
                               ConfiguredFun :: fun((list()) -> {ok, term()})) -> {ok, term()}.
maybe_backend_configured(BackendConfigKey,
                         ClusterFormationUndefinedFun,
                         BackendUndefinedFun,
                         ConfiguredFun) ->
    case application:get_env(rabbit, cluster_formation) of
        undefined ->
            ClusterFormationUndefinedFun();
        {ok, ClusterFormation} ->
            ?LOG_DEBUG(
               "Peer discovery: translated cluster formation configuration: ~p",
               [ClusterFormation],
               #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
            case proplists:get_value(BackendConfigKey, ClusterFormation) of
                undefined ->
                    BackendUndefinedFun();
                Proplist  ->
                    ?LOG_DEBUG(
                       "Peer discovery: cluster formation backend configuration: ~p",
                       [Proplist],
                       #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
                    ConfiguredFun(Proplist)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Return the passed in value as a list of strings.
%% @end
%%--------------------------------------------------------------------
-spec as_list(Value :: stringifyable() | list()) -> list().
as_list([]) -> [];
as_list(Value) when is_atom(Value) ; is_integer(Value) ; is_binary(Value) ->
  [Value];
as_list(Value) when is_list(Value) ->
  case io_lib:printable_list(Value) or io_lib:printable_unicode_list(Value) of
    true -> [case string:to_float(S) of
               {Float, []} -> Float;
               _ -> case string:to_integer(S) of
                      {Integer, []} -> Integer;
                      _ -> string:strip(S)
                    end
             end || S <- string:tokens(Value, ",")];
    false -> Value
  end;
as_list(Value) ->
  ?LOG_ERROR(
     "Unexpected data type for list value: ~p",
     [Value],
     #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  Value.
