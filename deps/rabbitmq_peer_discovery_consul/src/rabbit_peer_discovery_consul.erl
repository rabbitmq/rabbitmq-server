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
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2017 Pivotal Software, Inc. All rights reserved.
%%

-module(rabbit_peer_discovery_consul).
-behaviour(rabbit_peer_discovery_backend).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_peer_discovery_common/include/rabbit_peer_discovery.hrl").

-export([list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0]).
-export([init_health_check_notifier/0, send_health_check_pass/0]).
%% useful for debugging from the REPL with RABBITMQ_ALLOW_INPUT
-export([service_id/0, service_address/0]).
%% for tests
-export([build_registration_body/0, service_ttl/1]).

-define(CONFIG_MODULE, rabbit_peer_discovery_config).
-define(UTIL_MODULE,   rabbit_peer_discovery_util).

-define(BACKEND_CONFIG_KEY, peer_discovery_consul).

-define(CONSUL_CHECK_NOTES, "RabbitMQ Consul-based peer discovery plugin TTL check").

-define(CONFIG_MAPPING,
         #{
          cluster_name                       => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CLUSTER_NAME",
                                                   default_value = "undefined"
                                                  },
          consul_acl_token                   => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_ACL_TOKEN",
                                                   default_value = "undefined"
                                                  },
          consul_include_nodes_with_warnings => #peer_discovery_config_entry_meta{
                                                   type          = atom,
                                                   env_variable  = "CONSUL_INCLUDE_NODES_WITH_WARNINGS",
                                                   default_value = false
                                                  },
          consul_scheme                      => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_SCHEME",
                                                   default_value = "http"
                                                  },
          consul_host                        => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_HOST",
                                                   default_value = "localhost"
                                                  },
          consul_port                        => #peer_discovery_config_entry_meta{
                                                   type          = port,
                                                   env_variable  = "CONSUL_PORT",
                                                   default_value = 8500
                                                  },
          consul_domain                      => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_DOMAIN",
                                                   default_value = "consul"
                                                  },
          consul_svc                         => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_SVC",
                                                   default_value = "rabbitmq"
                                                  },
          consul_svc_addr                    => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_SVC_ADDR",
                                                   default_value = "undefined"
                                                  },
          consul_svc_addr_auto               => #peer_discovery_config_entry_meta{
                                                   type          = atom,
                                                   env_variable  = "CONSUL_SVC_ADDR_AUTO",
                                                   default_value = false
                                                  },
          consul_svc_addr_nic                => #peer_discovery_config_entry_meta{
                                                   type          = string,
                                                   env_variable  = "CONSUL_SVC_ADDR_NIC",
                                                   default_value = "undefined"
                                                  },
          consul_svc_addr_nodename           => #peer_discovery_config_entry_meta{
                                                   type          = atom,
                                                   env_variable  = "CONSUL_SVC_ADDR_NODENAME",
                                                   default_value = false
                                                  },
          consul_svc_port                    => #peer_discovery_config_entry_meta{
                                                   type          = integer,
                                                   env_variable  = "CONSUL_SVC_PORT",
                                                   default_value = 5672
                                                  },
          consul_svc_ttl                     => #peer_discovery_config_entry_meta{
                                                   type          = integer,
                                                   env_variable  = "CONSUL_SVC_TTL",
                                                   default_value = 30
                                                  },
          consul_deregister_after            => #peer_discovery_config_entry_meta{
                                                   type          = integer,
                                                   env_variable  = "CONSUL_DEREGISTER_AFTER",
                                                   default_value = ""
                                                  },
          consul_use_longname                => #peer_discovery_config_entry_meta{
                                                   type          = atom,
                                                   env_variable  = "CONSUL_USE_LONGNAME",
                                                   default_value = false
                                                  }
         }).

%%
%% API
%%

-spec list_nodes() -> {ok, {Nodes :: list(), NodeType :: rabbit_types:node_type()}} | {error, Reason :: string()}.

list_nodes() ->
    case application:get_env(rabbit, cluster_formation) of
      undefined         ->
        {ok, {[], disc}};
      {ok, ClusterFormation} ->
        case proplists:get_value(?BACKEND_CONFIG_KEY, ClusterFormation) of
            undefined ->
              rabbit_log:warning("Peer discovery backend is set to ~s "
                                 "but final config does not contain rabbit.cluster_formation.peer_discovery_consul. "
                                 "Cannot discover any nodes because Consul cluster details are not configured!",
                                 [?MODULE]),
              {ok, {[], disc}};
            Proplist  ->
              M = maps:from_list(Proplist),
              case rabbit_peer_discovery_httpc:get(get_config_key(consul_scheme, M),
                                                   get_config_key(consul_host, M),
                                                   get_config_key(consul_port, M),
                                                   [v1, health, service, get_config_key(consul_svc, M)],
                                                   list_nodes_query_args()) of
                  {ok, Nodes} ->
                      IncludeWithWarnings = get_config_key(consul_include_nodes_with_warnings, M),
                      Result = extract_nodes(
                                 filter_nodes(Nodes, IncludeWithWarnings)),
                      {ok, {Result, disc}};
                  {error, _} = Error ->
                      Error
              end
        end
    end.


-spec supports_registration() -> boolean().

supports_registration() ->
    true.


-spec register() -> ok | {error, Reason :: string()}.
register() ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  case registration_body() of
    {ok, Body} ->
      rabbit_log:debug("Consul registration body: ~s", [Body]),
      case rabbit_peer_discovery_httpc:post(get_config_key(consul_scheme, M),
                                            get_config_key(consul_host, M),
                                            get_config_key(consul_port, M),
                                            [v1, agent, service, register],
                                            maybe_add_acl([]), Body) of
        {ok, _} -> ok;
        Error   -> Error
      end;
    Error -> Error
  end.


-spec unregister() -> ok | {error, Reason :: string()}.
unregister() ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  ID = service_id(),
  rabbit_log:debug("Unregistering with Consul using service ID '~s'", [ID]),
  case rabbit_peer_discovery_httpc:get(get_config_key(consul_scheme, M),
                                       get_config_key(consul_host, M),
                                       get_config_key(consul_port, M),
                                       [v1, agent, service, deregister, ID],
                                       maybe_add_acl([])) of
    {ok, Response} ->
          rabbit_log:info("Consul's response to the unregistration attempt: ~p", [Response]),
          ok;
    Error   ->
          rabbit_log:info("Failed to unregister service with ID '~s` with Consul: ~p",
                         [ID, Error]),
          Error
  end.

-spec post_registration() -> ok | {error, Reason :: string()}.

post_registration() ->
    init_health_check_notifier().

%%
%% Implementation
%%

-spec get_config_key(Key :: atom(), Map :: #{atom() => peer_discovery_config_value()})
             -> peer_discovery_config_value().

get_config_key(Key, Map) ->
    ?CONFIG_MODULE:get(Key, ?CONFIG_MAPPING, Map).

-spec init_health_check_notifier() -> ok.

init_health_check_notifier() ->
  case rabbit_peer_discovery:backend() of
    ?MODULE ->
      M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
      case get_config_key(consul_svc_ttl, M) of
        undefined -> ok;
        %% in seconds
        Interval  ->
          %% We cannot use timer:apply_interval/4 here because this
          %% function is executed in a short live process and when it
          %% exits, the timer module will automatically cancel the
          %% timer.
          %%
          %% Instead we delegate to a locally registered gen_server,
          %% `rabbitmq_peer_discovery_consul_health_check_helper`.
          %%
          %% The value is 1/2 of what's configured to avoid a race
          %% condition between check TTL expiration and in flight
          %% notifications
          rabbitmq_peer_discovery_consul_health_check_helper:start_timer(Interval * 500),
          ok
      end;
    _ -> ok
  end.

-spec filter_nodes(ConsulResult :: list(), AllowWarning :: atom()) -> list().
filter_nodes(Nodes, Warn) ->
  case Warn of
    true ->
      lists:filter(fun(Node) ->
                    Checks = maps:get(<<"Checks">>, Node),
                    lists:all(fun(Check) ->
                      lists:member(maps:get(<<"Status">>, Check),
                                   [<<"passing">>, <<"warning">>])
                              end,
                              Checks)
                   end,
                   Nodes);
    false -> Nodes
  end.

-spec extract_nodes(ConsulResult :: list()) -> list().
extract_nodes(Data) -> extract_nodes(Data, []).

-spec extract_nodes(ConsulResult :: list(), Nodes :: list())
    -> list().
extract_nodes([], Nodes)    -> Nodes;
extract_nodes([H | T], Nodes) ->
  Service  = maps:get(<<"Service">>, H),
  Value    = maps:get(<<"Address">>, Service),
  NodeName = case ?UTIL_MODULE:as_string(Value) of
    "" ->
      NodeData = maps:get(<<"Node">>, H),
      Node = maps:get(<<"Node">>, NodeData),
      maybe_add_domain(?UTIL_MODULE:node_name(Node));
    Address ->
      ?UTIL_MODULE:node_name(Address)
  end,
  extract_nodes(T, lists:merge(Nodes, [NodeName])).

-spec maybe_add_acl(QArgs :: list()) -> list().
maybe_add_acl(QArgs) ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  case get_config_key(consul_acl_token, M) of
    "undefined" -> QArgs;
    ACL         -> lists:append(QArgs, [{token, ACL}])
  end.

-spec list_nodes_query_args() -> list().
list_nodes_query_args() ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  maybe_add_acl(list_nodes_query_args(get_config_key(cluster_name, M))).

-spec list_nodes_query_args(ClusterName :: string()) -> list().
list_nodes_query_args(Cluster) ->
  ClusterTag = case Cluster of
    "undefined" -> [];
    _           -> [{tag, Cluster}]
  end,
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  list_nodes_query_args(ClusterTag, get_config_key(consul_include_nodes_with_warnings, M)).

-spec list_nodes_query_args(Args :: list(), AllowWarn :: atom()) -> list().
list_nodes_query_args(Value, Warn) ->
    case Warn of
        true  -> Value;
        false -> [passing | Value]
    end.

-spec registration_body() -> {ok, Body :: binary()} | {error, atom()}.
registration_body() ->
  Payload = build_registration_body(),
  registration_body(rabbit_json:try_encode(Payload)).

-spec registration_body(Response :: {ok, Body :: string()} |
                                    {error, Reason :: atom()})
  -> {ok, Body :: binary()} | {error, Reason :: atom()}.
registration_body({ok, Body}) ->
  {ok, rabbit_data_coercion:to_binary(Body)};
registration_body({error, Reason}) ->
  rabbit_log:error("Error serializing the request body: ~p",
    [Reason]),
  {error, Reason}.


-spec build_registration_body() -> list().
build_registration_body() ->
  Payload1 = registration_body_add_id(),
  Payload2 = registration_body_add_name(Payload1),
  Payload3 = registration_body_maybe_add_address(Payload2),
  Payload4 = registration_body_add_port(Payload3),
  Payload5 = registration_body_maybe_add_check(Payload4),
  registration_body_maybe_add_tag(Payload5).

-spec registration_body_add_id() -> list().
registration_body_add_id() ->
  [{'ID', list_to_atom(service_id())}].

-spec registration_body_add_name(Payload :: list()) -> list().
registration_body_add_name(Payload) ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  Name = list_to_atom(get_config_key(consul_svc, M)),
  lists:append(Payload, [{'Name', Name}]).

-spec registration_body_maybe_add_address(Payload :: list())
    -> list().
registration_body_maybe_add_address(Payload) ->
  registration_body_maybe_add_address(Payload, service_address()).

-spec registration_body_maybe_add_address(Payload :: list(), string())
    -> list().
registration_body_maybe_add_address(Payload, "undefined") -> Payload;
registration_body_maybe_add_address(Payload, Address) ->
  lists:append(Payload, [{'Address', list_to_atom(Address)}]).

registration_body_maybe_add_check(Payload) ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  TTL = get_config_key(consul_svc_ttl, M),
  registration_body_maybe_add_check(Payload, TTL).

-spec registration_body_maybe_add_check(Payload :: list(),
                                        TTL :: integer() | undefined)
    -> list().
registration_body_maybe_add_check(Payload, undefined) ->
    case registration_body_maybe_add_deregister([]) of
        [{'Deregister_critical_service_after', _}]->
            rabbit_log:warning("Can't use Consul Deregister After without " ++
            "using TTL. The parameter CONSUL_DEREGISTER_AFTER will be ignored"),
            Payload;

        _ -> Payload
    end;
registration_body_maybe_add_check(Payload, TTL) ->
    CheckItems = [{'Notes', list_to_atom(?CONSUL_CHECK_NOTES)},
        {'TTL', list_to_atom(service_ttl(TTL))}],
    Check = [{'Check', registration_body_maybe_add_deregister(CheckItems)}],
    lists:append(Payload, Check).

-spec registration_body_add_port(Payload :: list()) -> list().
registration_body_add_port(Payload) ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  lists:append(Payload,
               [{'Port', get_config_key(consul_svc_port, M)}]).

registration_body_maybe_add_deregister(Payload) ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    Deregister = get_config_key(consul_deregister_after, M),
    registration_body_maybe_add_deregister(Payload, Deregister).

-spec registration_body_maybe_add_deregister(Payload :: list(),
    TTL :: integer() | undefined)
        -> list().
registration_body_maybe_add_deregister(Payload, undefined) -> Payload;
registration_body_maybe_add_deregister(Payload, Deregister_After) ->
    Deregister = {'Deregister_critical_service_after',
        list_to_atom(service_ttl(Deregister_After))},
    Payload ++ [Deregister].

-spec registration_body_maybe_add_tag(Payload :: list()) -> list().
registration_body_maybe_add_tag(Payload) ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  Value = get_config_key(cluster_name, M),
  registration_body_maybe_add_tag(Payload, Value).

-spec registration_body_maybe_add_tag(Payload :: list(),
                                      ClusterName :: string())
    -> list().
registration_body_maybe_add_tag(Payload, "undefined") -> Payload;
registration_body_maybe_add_tag(Payload, Cluster) ->
  lists:append(Payload, [{'Tags', [list_to_atom(Cluster)]}]).


-spec validate_addr_parameters(false | true, false | true) -> false | true.
validate_addr_parameters(false, true) ->
    rabbit_log:warning("The parameter CONSUL_SVC_ADDR_NODENAME" ++
                           " can be used only if CONSUL_SVC_ADDR_AUTO is true." ++
                           " CONSUL_SVC_ADDR_NODENAME value will be ignored."),
    false;
validate_addr_parameters(_, _) ->
    true.


-spec service_address() -> string().
service_address() ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  validate_addr_parameters(get_config_key(consul_svc_addr_auto, M),
      get_config_key(consul_svc_addr_nodename, M)),
  service_address(get_config_key(consul_svc_addr, M),
                  get_config_key(consul_svc_addr_auto, M),
                  get_config_key(consul_svc_addr_nic, M),
                  get_config_key(consul_svc_addr_nodename, M)).


-spec service_address(Static :: string(),
                      Auto :: boolean(),
                      AutoNIC :: string(),
                      FromNodename :: boolean()) -> string().
service_address(_, true, "undefined", FromNodename) ->
  rabbit_peer_discovery_util:node_hostname(FromNodename);
service_address(Value, false, "undefined", _) ->
  Value;
service_address(_, false, NIC, _) ->
  {ok, Addr} = rabbit_peer_discovery_util:nic_ipv4(NIC),
  Addr.


-spec service_id() -> string().
service_id() ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  service_id(get_config_key(consul_svc, M),
             service_address()).

-spec service_id(Name :: string(), Address :: string()) -> string().
service_id(Service, "undefined") -> Service;
service_id(Service, Address) ->
  string:join([Service, Address], ":").

-spec service_ttl(TTL :: integer()) -> string().
service_ttl(Value) ->
  rabbit_peer_discovery_util:as_string(Value) ++ "s".

-spec maybe_add_domain(Domain :: atom()) -> atom().
maybe_add_domain(Value) ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  case get_config_key(consul_use_longname, M) of
      true ->
          list_to_atom(string:join([atom_to_list(Value),
                                    "node",
                                    get_config_key(consul_domain, M)],
                                   "."));
      false -> Value
  end.


%%--------------------------------------------------------------------
%% @doc
%% Let Consul know that this node is still around
%% @end
%%--------------------------------------------------------------------

-spec send_health_check_pass() -> ok.

send_health_check_pass() ->
  Service = string:join(["service", service_id()], ":"),
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  rabbit_log:debug("Running Consul health check"),
  case rabbit_peer_discovery_httpc:get(get_config_key(consul_scheme, M),
                                       get_config_key(consul_host, M),
                                       get_config_key(consul_port, M),
                                       [v1, agent, check, pass, Service],
                                       maybe_add_acl([])) of
    {ok, []} -> ok;
    {error, "429"} ->
          %% Too Many Requests, see https://www.consul.io/docs/agent/checks.html
          rabbit_log:warning("Consul responded to a health check with 429 Too Many Requests"),
          ok;
    {error, "500"} ->
          rabbit_log:warning("Consul responded to a health check with a 500 status, will wait and try re-registering"),
          maybe_re_register(wait_for_list_nodes()),
          ok;
    {error, Reason} ->
          rabbit_log:error("Error running Consul health check: ~p",
                           [Reason]),
      ok
  end.

maybe_re_register({error, Reason}) ->
    rabbit_log:error("Internal error in Consul while updating health check. "
                     "Cannot obtain list of nodes registered in Consul either: ~p",
                     [Reason]);
maybe_re_register({ok, {Members, _NodeType}}) ->
    maybe_re_register(Members);
maybe_re_register({ok, Members}) ->
    maybe_re_register(Members);
maybe_re_register(Members) ->
    case lists:member(node(), Members) of
        true ->
            rabbit_log:error("Internal error in Consul while updating health check",
                             []);
        false ->
            rabbit_log:error("Internal error in Consul while updating health check, "
                             "node is not registered. Re-registering", []),
            register()
    end.

wait_for_list_nodes() ->
    wait_for_list_nodes(60).

wait_for_list_nodes(N) ->
    case {list_nodes(), N} of
        {Reply, 0} ->
            Reply;
        {{ok, _} = Reply, _} ->
            Reply;
        {{error, _}, _} ->
            timer:sleep(1000),
            wait_for_list_nodes(N - 1)
    end.
