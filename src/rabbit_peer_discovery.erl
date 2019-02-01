%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_peer_discovery).

%%
%% API
%%

-export([maybe_init/0, discover_cluster_nodes/0, backend/0, node_type/0,
         normalize/1, format_discovered_nodes/1, log_configured_backend/0,
         register/0, unregister/0, maybe_register/0, maybe_unregister/0,
         maybe_inject_randomized_delay/0, lock/0, unlock/1]).
-export([append_node_prefix/1, node_prefix/0, retry_timeout/0,
         lock_acquisition_failure_mode/0]).

-define(DEFAULT_BACKEND,   rabbit_peer_discovery_classic_config).
%% what node type is used by default for this node when joining
%% a new cluster as a virgin node
-define(DEFAULT_NODE_TYPE, disc).
%% default node prefix to attach to discovered hostnames
-define(DEFAULT_PREFIX, "rabbit").
%% default randomized delay range, in seconds
-define(DEFAULT_STARTUP_RANDOMIZED_DELAY, {5, 60}).

-define(NODENAME_PART_SEPARATOR, "@").


-spec backend() -> atom().

backend() ->
  case application:get_env(rabbit, cluster_formation) of
    {ok, Proplist} ->
      proplists:get_value(peer_discovery_backend, Proplist, ?DEFAULT_BACKEND);
    undefined      ->
      ?DEFAULT_BACKEND
  end.



-spec node_type() -> rabbit_types:node_type().

node_type() ->
  case application:get_env(rabbit, cluster_formation) of
    {ok, Proplist} ->
      proplists:get_value(node_type, Proplist, ?DEFAULT_NODE_TYPE);
    undefined      ->
      ?DEFAULT_NODE_TYPE
  end.

-spec retry_timeout() -> {Retries :: integer(), Timeout :: integer()}.

retry_timeout() ->
    case application:get_env(rabbit, cluster_formation) of
        {ok, Proplist} ->
            Retries = proplists:get_value(lock_retry_limit, Proplist, 10),
            Timeout = proplists:get_value(lock_retry_timeout, Proplist, 30000),
            {Retries, Timeout};
        undefined ->
            {10, 30000}
    end.

-spec lock_acquisition_failure_mode() -> ignore | fail.

lock_acquisition_failure_mode() ->
    case application:get_env(rabbit, cluster_formation) of
        {ok, Proplist} ->
            proplists:get_value(lock_acquisition_failure_mode, Proplist, fail);
        undefined      ->
            fail
  end.

-spec log_configured_backend() -> ok.

log_configured_backend() ->
  rabbit_log:info("Configured peer discovery backend: ~s~n", [backend()]).

maybe_init() ->
    Backend = backend(),
    case erlang:function_exported(Backend, init, 0) of
        true  ->
            rabbit_log:debug("Peer discovery backend supports initialisation."),
            case Backend:init() of
                ok ->
                    rabbit_log:debug("Peer discovery backend initialisation succeeded."),
                    ok;
                {error, Error} ->
                    rabbit_log:warning("Peer discovery backend initialisation failed: ~p.", [Error]),
                    ok
            end;
        false -> ok
    end.


%% This module doesn't currently sanity-check the return value of
%% `Backend:list_nodes()`. Therefore, it could return something invalid:
%% thus the `{œk, any()} in the spec.
%%
%% `rabbit_mnesia:init_from_config()` does some verifications.

-spec discover_cluster_nodes() ->
    {ok, {Nodes :: [node()], NodeType :: rabbit_types:node_type()} | any()} |
    {error, Reason :: string()}.

discover_cluster_nodes() ->
    Backend = backend(),
    normalize(Backend:list_nodes()).


-spec maybe_register() -> ok.

maybe_register() ->
  Backend = backend(),
  case Backend:supports_registration() of
    true  ->
      register(),
      Backend:post_registration();
    false ->
      rabbit_log:info("Peer discovery backend ~s does not support registration, skipping registration.", [Backend]),
      ok
  end.


-spec maybe_unregister() -> ok.

maybe_unregister() ->
  Backend = backend(),
  case Backend:supports_registration() of
    true  ->
      unregister();
    false ->
      rabbit_log:info("Peer discovery backend ~s does not support registration, skipping unregistration.", [Backend]),
      ok
  end.


-spec maybe_inject_randomized_delay() -> ok.
maybe_inject_randomized_delay() ->
  Backend = backend(),
  case Backend:supports_registration() of
    true  ->
      rabbit_log:info("Peer discovery backend ~s supports registration.", [Backend]),
      inject_randomized_delay();
    false ->
      rabbit_log:info("Peer discovery backend ~s does not support registration, skipping randomized startup delay.", [Backend]),
      ok
  end.

-spec inject_randomized_delay() -> ok.

inject_randomized_delay() ->
    {Min, Max} = randomized_delay_range_in_ms(),
    case {Min, Max} of
        %% When the max value is set to 0, consider the delay to be disabled.
        %% In addition, `rand:uniform/1` will fail with a "no function clause"
        %% when the argument is 0.
        {_, 0} ->
            rabbit_log:info("Randomized delay range's upper bound is set to 0. Considering it disabled."),
            ok;
        {_, N} when is_number(N) ->
            rand:seed(exsplus),
            RandomVal  = rand:uniform(round(N)),
            rabbit_log:debug("Randomized startup delay: configured range is from ~p to ~p milliseconds, PRNG pick: ~p...",
                             [Min, Max, RandomVal]),
            Effective  = case RandomVal < Min of
                             true  -> Min;
                             false -> RandomVal
                         end,
            rabbit_log:info("Will wait for ~p milliseconds before proceeding with registration...", [Effective]),
            timer:sleep(Effective),
            ok
    end.

-spec randomized_delay_range_in_ms() -> {integer(), integer()}.

randomized_delay_range_in_ms() ->
  Backend    = backend(),
  Default    = case erlang:function_exported(Backend, randomized_startup_delay_range, 0) of
                   true  -> Backend:randomized_startup_delay_range();
                   false -> ?DEFAULT_STARTUP_RANDOMIZED_DELAY
               end,
  {Min, Max} = case application:get_env(rabbit, cluster_formation) of
                   {ok, Proplist} ->
                       proplists:get_value(randomized_startup_delay_range, Proplist, Default);
                   undefined      ->
                       Default
               end,
    {Min * 1000, Max * 1000}.


-spec register() -> ok.

register() ->
  Backend = backend(),
  rabbit_log:info("Will register with peer discovery backend ~s", [Backend]),
  case Backend:register() of
    ok             -> ok;
    {error, Error} ->
      rabbit_log:error("Failed to register with peer discovery backend ~s: ~p",
        [Backend, Error]),
      ok
  end.


-spec unregister() -> ok.

unregister() ->
  Backend = backend(),
  rabbit_log:info("Will unregister with peer discovery backend ~s", [Backend]),
  case Backend:unregister() of
    ok             -> ok;
    {error, Error} ->
      rabbit_log:error("Failed to unregister with peer discovery backend ~s: ~p",
        [Backend, Error]),
      ok
  end.

-spec lock() -> ok | {ok, Data :: term()} | not_supported | {error, Reason :: string()}.

lock() ->
    Backend = backend(),
    rabbit_log:info("Will try to lock with peer discovery backend ~s", [Backend]),
    case Backend:lock(node()) of
        {error, Reason} = Error ->
            rabbit_log:error("Failed to lock with peer discovery backend ~s: ~p",
                             [Backend, Reason]),
            Error;
        Any ->
            Any
    end.

-spec unlock(Data :: term()) -> ok | {error, Reason :: string()}.

unlock(Data) ->
    Backend = backend(),
    rabbit_log:info("Will try to unlock with peer discovery backend ~s", [Backend]),
    case Backend:unlock(Data) of
        {error, Reason} = Error ->
            rabbit_log:error("Failed to unlock with peer discovery backend ~s: ~p, "
                             "lock data: ~p",
                             [Backend, Reason, Data]),
            Error;
        Any ->
            Any
    end.

%%
%% Implementation
%%

-spec normalize(Nodes :: [node()] |
                {Nodes :: [node()],
                 NodeType :: rabbit_types:node_type()} |
                {ok, Nodes :: [node()]} |
                {ok, {Nodes :: [node()],
                      NodeType :: rabbit_types:node_type()}} |
                {error, Reason :: string()}) ->
    {ok, {Nodes :: [node()], NodeType :: rabbit_types:node_type()}} |
    {error, Reason :: string()}.

normalize(Nodes) when is_list(Nodes) ->
  {ok, {Nodes, disc}};
normalize({Nodes, NodeType}) when is_list(Nodes) andalso is_atom(NodeType) ->
  {ok, {Nodes, NodeType}};
normalize({ok, Nodes}) when is_list(Nodes) ->
  {ok, {Nodes, disc}};
normalize({ok, {Nodes, NodeType}}) when is_list(Nodes) andalso is_atom(NodeType) ->
  {ok, {Nodes, NodeType}};
normalize({error, Reason}) ->
  {error, Reason}.

-spec format_discovered_nodes(Nodes :: list()) -> string().

format_discovered_nodes(Nodes) ->
  %% NOTE: in OTP 21 string:join/2 is deprecated but still available.
  %%       Its recommended replacement is not a drop-in one, though, so
  %%       we will not be switching just yet.
  string:join(lists:map(fun rabbit_data_coercion:to_list/1, Nodes), ", ").



-spec node_prefix() -> string().

node_prefix() ->
    case string:tokens(atom_to_list(node()), ?NODENAME_PART_SEPARATOR) of
        [Prefix, _] -> Prefix;
        [_]         -> ?DEFAULT_PREFIX
    end.



-spec append_node_prefix(Value :: binary() | string()) -> string().

append_node_prefix(Value) when is_binary(Value) orelse is_list(Value) ->
    Val = rabbit_data_coercion:to_list(Value),
    Hostname = case string:tokens(Val, ?NODENAME_PART_SEPARATOR) of
                   [_ExistingPrefix, HN] -> HN;
                   [HN]                  -> HN
               end,
    string:join([node_prefix(), Hostname], ?NODENAME_PART_SEPARATOR).
