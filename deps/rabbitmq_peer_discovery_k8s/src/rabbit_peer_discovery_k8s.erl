%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(rabbit_peer_discovery_k8s).
-behaviour(rabbit_peer_discovery_backend).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_peer_discovery_common/include/rabbit_peer_discovery.hrl").
-include("rabbit_peer_discovery_k8s.hrl").


-export([init/0, list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1, send_event/3, generate_v1_event/7]).

-ifdef(TEST).
-compile(export_all).
-endif.

%%
%% API
%%

init() ->
    rabbit_log:debug("Peer discovery Kubernetes: initialising..."),
    ok = application:ensure_started(inets),
    %% we cannot start this plugin yet since it depends on the rabbit app,
    %% which is in the process of being started by the time this function is called
    application:load(rabbitmq_peer_discovery_common),
    rabbit_peer_discovery_httpc:maybe_configure_proxy(),
    rabbit_peer_discovery_httpc:maybe_configure_inet6().

-spec list_nodes() -> {ok, {Nodes :: list(), NodeType :: rabbit_types:node_type()}} | {error, Reason :: string()}.

list_nodes() ->
  case make_request() of
    {ok, Response} ->
      Addresses = extract_node_list(Response),
      Nodes = lists:map(fun node_name/1, Addresses),
      {ok, {Nodes, disc}};
    {error, Reason} ->
      Details = io_lib:format("Failed to fetch a list of nodes from Kubernetes API: ~s", [Reason]),
      rabbit_log:error(Details),
      send_event("Warning", "Failed", Details),
      {error, Reason}
  end.

-spec supports_registration() -> boolean().

supports_registration() ->
    true. %% to send event in post_registration/0

-spec post_registration() -> ok | {error, Reason :: string()}.
post_registration() ->
    Details = io_lib:format("Node ~s is registered", [node()]),
    send_event("Normal", "Created", Details).

-spec register() -> ok.
register() ->
    ok.

-spec unregister() -> ok.
unregister() ->
    ok.

-spec lock(Node :: node()) -> {ok, {ResourceId :: string(), LockRequesterId :: node()}} | {error, Reason :: string()}.

lock(Node) ->
  %% call list_nodes/0 externally such that meck can mock the function
  case ?MODULE:list_nodes() of
    {ok, {Nodes, disc}} ->
      case lists:member(Node, Nodes) of
        true ->
          rabbit_log:info("Will try to lock connecting to nodes ~p", [Nodes]),
          LockId = rabbit_nodes:lock_id(Node),
          Retries = rabbit_nodes:lock_retries(),
          case global:set_lock(LockId, Nodes, Retries) of
            true ->
              {ok, LockId};
            false ->
              {error, io_lib:format("Acquiring lock taking too long, bailing out after ~b retries", [Retries])}
          end;
        false ->
          %% Don't try to acquire the global lock when local node is not discoverable by peers.
          %% This branch is just an additional safety check. We should never run into this branch
          %% because the local Pod is in state 'Running' and we listed both ready and not-ready addresses.
          {error, lists:flatten(io_lib:format("Local node ~s is not part of discovered nodes ~p", [Node, Nodes]))}
      end;
    {error, _} = Error ->
      Error
  end.

-spec unlock({ResourceId :: string(), LockRequesterId :: node()}) -> ok | {error, Reason :: string()}.

unlock(LockId) ->
  case ?MODULE:list_nodes() of
    {ok, {Nodes, disc}} ->
      global:del_lock(LockId, Nodes),
      ok;
    {error, _} = Error ->
      Error
  end.

%%
%% Implementation
%%

-spec get_config_key(Key :: atom(), Map :: #{atom() => peer_discovery_config_value()})
                    -> peer_discovery_config_value().

get_config_key(Key, Map) ->
    ?CONFIG_MODULE:get(Key, ?CONFIG_MAPPING, Map).

%% @doc Perform a HTTP GET request to K8s
%% @end
%%
-spec make_request() -> {ok, map() | list() | term()} | {error, term()}.

make_request() ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    {ok, Token} = file:read_file(get_config_key(k8s_token_path, M)),
    Token1 = binary:replace(Token, <<"\n">>, <<>>),
    ?HTTPC_MODULE:get(
      get_config_key(k8s_scheme, M),
      get_config_key(k8s_host, M),
      get_config_key(k8s_port, M),
      base_path(endpoints,get_config_key(k8s_service_name, M)),
      [],
      [{"Authorization", "Bearer " ++ binary_to_list(Token1)}],
      [{ssl, [{cacertfile, get_config_key(k8s_cert_path, M)}]}]).

%% @spec node_name(k8s_endpoint) -> list()
%% @doc Return a full rabbit node name, appending hostname suffix
%% @end
%%
node_name(Address) ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    ?UTIL_MODULE:node_name(
       ?UTIL_MODULE:as_string(Address) ++ get_config_key(k8s_hostname_suffix, M)).

%% @spec address(k8s_subsets()) -> list()
%% @doc Return a list of both ready and not-ready nodes.
%% For the purpose of peer discovery, consider both ready and not-ready addresses.
%% Discover peers as quickly as possible not waiting for their readiness check to succeed.
%% @end
%%
-spec address([map()]) -> list().

address(Subset) ->
  maps:get(<<"notReadyAddresses">>, Subset, []) ++
  maps:get(<<"addresses">>, Subset, []).

%% @doc Return a list of nodes
%% "The set of all endpoints is the union of all subsets."
%%  https://kubernetes.io/docs/reference/kubernetes-api/service-resources/endpoints-v1/
%% @end
%%
-spec extract_node_list(map()) -> list().

extract_node_list(Response) ->
    IpLists = [[get_address(Address)
		|| Address <- address(Subset)] || Subset <- maps:get(<<"subsets">>, Response, [])],
    sets:to_list(sets:union(lists:map(fun sets:from_list/1, IpLists))).


%% @doc Return a list of path segments that are the base path for k8s key actions
%% @end
%%
-spec base_path(events | endpoints, term()) -> string().
base_path(Type, Args) ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    {ok, Namespace} = file:read_file(get_config_key(k8s_namespace_path, M)),
    NameSpace1 = binary:replace(Namespace, <<"\n">>, <<>>),
    rabbit_peer_discovery_httpc:build_path([api, v1, namespaces, NameSpace1, Type, Args]).

%% get_config_key(k8s_service_name, M)

get_address(Address) ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    maps:get(list_to_binary(get_config_key(k8s_address_type, M)), Address).


generate_v1_event(Namespace, Type, Reason, Message) ->
    {ok, HostName} = inet:gethostname(),
    Name =
        io_lib:format(HostName ++ ".~B",[os:system_time(millisecond)]),
    TimeInSeconds = calendar:system_time_to_rfc3339(erlang:system_time(second)),
    generate_v1_event(Namespace, Name, Type, Reason, Message, TimeInSeconds, HostName).


generate_v1_event(Namespace, Name, Type, Reason, Message, Timestamp, HostName) ->
    #{
      metadata => #{
		    namespace => rabbit_data_coercion:to_binary(Namespace),
		    name => rabbit_data_coercion:to_binary(Name)
		   },
      type => rabbit_data_coercion:to_binary(Type),
      reason => rabbit_data_coercion:to_binary(Reason),
      message => rabbit_data_coercion:to_binary(Message),
      count => 1,
      lastTimestamp =>  rabbit_data_coercion:to_binary(Timestamp),
      involvedObject => #{
			  apiVersion => <<"v1">>,
			  kind => <<"RabbitMQ">>,
			  name =>  rabbit_data_coercion:to_binary("pod/" ++ HostName),
			  namespace => rabbit_data_coercion:to_binary(Namespace)
			 },
      source => #{
		  component => rabbit_data_coercion:to_binary(HostName ++ "/" ++
						  ?K8S_EVENT_SOURCE_DESCRIPTION),
		  host => rabbit_data_coercion:to_binary(HostName)
		 }
     }.


%% @doc Perform a HTTP POST request to K8s to send and k8s v1.Event
%% @end
%%
-spec send_event(term(),term(), term()) -> {ok, term()} | {error, term()}.
send_event(Type, Reason, Message) ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    {ok, Token} = file:read_file(get_config_key(k8s_token_path, M)),
    Token1 = binary:replace(Token, <<"\n">>, <<>>),
    {ok, NameSpace} = file:read_file(
			get_config_key(k8s_namespace_path, M)),
    NameSpace1 = binary:replace(NameSpace, <<"\n">>, <<>>),

    V1Event = generate_v1_event(NameSpace1, Type, Reason, Message),

    Body = rabbit_data_coercion:to_list(rabbit_json:encode(V1Event)),
    ?HTTPC_MODULE:post(
       get_config_key(k8s_scheme, M),
       get_config_key(k8s_host, M),
       get_config_key(k8s_port, M),
       base_path(events,""),
       [],
       [{"Authorization", "Bearer " ++ rabbit_data_coercion:to_list(Token1)}],
       [{ssl, [{cacertfile, get_config_key(k8s_cert_path, M)}]}],
        Body
      ).
