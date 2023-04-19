%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_nodes).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([names/1, diagnostics/1, make/1, make/2, parts/1, cookie_hash/0,
         is_running/2, is_process_running/2,
         cluster_name/0, set_cluster_name/1, set_cluster_name/2, ensure_epmd/0,
         all_running/0,
         is_member/1, list_members/0,
         filter_members/1,
         is_reachable/1, list_reachable/0, list_unreachable/0,
         filter_reachable/1, filter_unreachable/1,
         is_running/1, list_running/0, list_not_running/0,
         filter_running/1, filter_not_running/1,
         is_serving/1, list_serving/0, list_not_serving/0,
         filter_serving/1, filter_not_serving/1,
         name_type/0, running_count/0, total_count/0,
         await_running_count/2, is_single_node_cluster/0,
         boot/0]).
-export([persistent_cluster_id/0, seed_internal_cluster_id/0, seed_user_provided_cluster_name/0]).
-export([all/0, all_running_with_hashes/0, target_cluster_size_hint/0, reached_target_cluster_size/0,
         if_reached_target_cluster_size/2]).
-export([lock_id/1, lock_retries/0]).

-deprecated({all, 0, "Use rabbit_nodes:list_members/0 instead"}).
-deprecated({all_running, 0, "Use rabbit_nodes:list_running/0 instead"}).

-include_lib("kernel/include/inet.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(SAMPLING_INTERVAL, 1000).
-define(FILTER_RPC_TIMEOUT, 10000).

-define(INTERNAL_CLUSTER_ID_PARAM_NAME, internal_cluster_id).

% Retries as passed to https://erlang.org/doc/man/global.html#set_lock-3
% To understand how retries map to the timeout, read
% https://github.com/erlang/otp/blob/d256ae477014158a49bb860b283df9c040011197/lib/kernel/src/global.erl#L2062-L2075
% 80 corresponds to a timeout of ca 300 seconds.
-define(DEFAULT_LOCK_RETRIES, 80).

-define(DEFAULT_TARGET_CLUSTER_SIZE, 1).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

boot() ->
  _ = seed_internal_cluster_id(),
  seed_user_provided_cluster_name().

name_type() ->
    #{nodename_type := NodeType} = rabbit_prelaunch:get_context(),
    NodeType.

-spec names(string()) ->
          rabbit_types:ok_or_error2([{string(), integer()}], term()).

names(Hostname) ->
    rabbit_nodes_common:names(Hostname).

-spec diagnostics([node()]) -> string().

diagnostics(Nodes) ->
    rabbit_nodes_common:diagnostics(Nodes).

make(NameOrParts) ->
    rabbit_nodes_common:make(NameOrParts).

make(ShortName, Hostname) ->
    make({ShortName, Hostname}).

parts(NodeStr) ->
    rabbit_nodes_common:parts(NodeStr).

-spec cookie_hash() -> string().

cookie_hash() ->
    rabbit_nodes_common:cookie_hash().

-spec is_running(node(), atom()) -> boolean().

is_running(Node, Application) ->
    rabbit_nodes_common:is_running(Node, Application).

-spec is_process_running(node(), atom()) -> boolean().

is_process_running(Node, Process) ->
    rabbit_nodes_common:is_process_running(Node, Process).

-spec cluster_name() -> binary().

cluster_name() ->
    case rabbit_runtime_parameters:value_global(cluster_name) of
        not_found -> cluster_name_default();
        Name -> Name
    end.

cluster_name_default() ->
    {ID, _} = parts(node()),
    FQDN = rabbit_net:hostname(),
    list_to_binary(atom_to_list(make({ID, FQDN}))).

-spec persistent_cluster_id() -> binary().
persistent_cluster_id() ->
    case rabbit_runtime_parameters:lookup_global(?INTERNAL_CLUSTER_ID_PARAM_NAME) of
        not_found ->
            _ = seed_internal_cluster_id(),
            persistent_cluster_id();
        Param ->
            #{value := Val, name := ?INTERNAL_CLUSTER_ID_PARAM_NAME} = maps:from_list(Param),
            Val
    end.

-spec seed_internal_cluster_id() -> binary().
seed_internal_cluster_id() ->
    case rabbit_runtime_parameters:lookup_global(?INTERNAL_CLUSTER_ID_PARAM_NAME) of
        not_found ->
            Id = rabbit_guid:binary(rabbit_guid:gen(), "rabbitmq-cluster-id"),
            rabbit_log:info("Initialising internal cluster ID to '~ts'", [Id]),
            rabbit_runtime_parameters:set_global(?INTERNAL_CLUSTER_ID_PARAM_NAME, Id, ?INTERNAL_USER),
            Id;
        Param ->
            #{value := Val, name := ?INTERNAL_CLUSTER_ID_PARAM_NAME} = maps:from_list(Param),
            Val
    end.

seed_user_provided_cluster_name() ->
    case application:get_env(rabbit, cluster_name) of
        undefined -> ok;
        {ok, Name} ->
            rabbit_log:info("Setting cluster name to '~ts' as configured", [Name]),
            set_cluster_name(rabbit_data_coercion:to_binary(Name))
    end.

-spec set_cluster_name(binary()) -> 'ok'.

set_cluster_name(Name) ->
    set_cluster_name(Name, ?INTERNAL_USER).

-spec set_cluster_name(binary(), rabbit_types:username()) -> 'ok'.

set_cluster_name(Name, Username) ->
    %% Cluster name should be binary
    BinaryName = rabbit_data_coercion:to_binary(Name),
    rabbit_runtime_parameters:set_global(cluster_name, BinaryName, Username).

ensure_epmd() ->
    rabbit_nodes_common:ensure_epmd().

-spec all() -> [node()].
all() -> list_members().

-spec all_running() -> [node()].
all_running() -> list_running().

-spec is_member(Node) -> IsMember when
      Node :: node(),
      IsMember :: boolean().
%% @doc Indicates if the given node is a cluster member.
%%
%% @see filter_members/1.

is_member(Node) when is_atom(Node) ->
    [Node] =:= filter_members([Node]).

-spec list_members() -> Nodes when
      Nodes :: [node()].
%% @doc Returns the list of nodes in the cluster.
%%
%% @see filter_members/1.

list_members() ->
    rabbit_db_cluster:members().

-spec filter_members(Nodes) -> Nodes when
      Nodes :: [node()].
%% @doc Filters the given list of nodes to only select those belonging to the
%% cluster.
%%
%% The cluster being considered is the one which the node running this
%% function belongs to.

filter_members(Nodes) ->
    %% Before calling {@link filter_members/2}, we filter out any node which
    %% is not part of the same cluster as the node running this function.
    Members = list_members(),
    [Node || Node <- Nodes, lists:member(Node, Members)].

-spec is_reachable(Node) -> IsReachable when
      Node :: node(),
      IsReachable :: boolean().
%% @doc Indicates if the given node is reachable.
%%
%% @see filter_reachable/1.

is_reachable(Node) when is_atom(Node) ->
    [Node] =:= filter_reachable([Node]).

-spec list_reachable() -> Nodes when
      Nodes :: [node()].
%% @doc Returns the list of nodes in the cluster we can reach.
%%
%% A reachable node is one we can connect to using Erlang distribution.
%%
%% @see filter_reachable/1.

list_reachable() ->
    Members = list_members(),
    filter_reachable(Members).

-spec list_unreachable() -> Nodes when
      Nodes :: [node()].
%% @doc Returns the list of nodes in the cluster we can't reach.
%%
%% A reachable node is one we can connect to using Erlang distribution.
%%
%% @see filter_unreachable/1.

list_unreachable() ->
    Members = list_members(),
    filter_unreachable(Members).

-spec filter_reachable(Nodes) -> Nodes when
      Nodes :: [node()].
%% @doc Filters the given list of nodes to only select those belonging to the
%% cluster and we can reach.
%%
%% The cluster being considered is the one which the node running this
%% function belongs to.
%%
%% A reachable node is one we can connect to using Erlang distribution.

filter_reachable(Nodes) ->
    Members = filter_members(Nodes),
    do_filter_reachable(Members).

-spec filter_unreachable(Nodes) -> Nodes when
      Nodes :: [node()].
%% @doc Filters the given list of nodes to only select those belonging to the
%% cluster but we can't reach.
%%
%% @see filter_reachable/1.

filter_unreachable(Nodes) ->
    Members = filter_members(Nodes),
    Reachable = do_filter_reachable(Members),
    Members -- Reachable.

-spec do_filter_reachable(Members) -> Members when
      Members :: [node()].
%% @doc Filters the given list of cluster members to only select those we can
%% reach.
%%
%% The given list of nodes must have been verified to only contain cluster
%% members.
%%
%% @private

do_filter_reachable(Members) ->
    %% All clustered members we can reach, regardless of the state of RabbitMQ
    %% on those nodes.
    %%
    %% We are using `nodes/0' to get the list of nodes we should be able to
    %% reach. This list might be out-of-date, but it's the only way we can
    %% filter `Members' without trying to effectively connect to each node. If
    %% we try to connect, it breaks `rabbit_node_monitor' partial partition
    %% detection mechanism.
    Nodes = [node() | nodes()],
    lists:filter(
      fun(Member) -> lists:member(Member, Nodes) end,
      Members).

-spec is_running(Node) -> IsRunning when
      Node :: node(),
      IsRunning :: boolean().
%% @doc Indicates if the given node is running.
%%
%% @see filter_running/1.

is_running(Node) when is_atom(Node) ->
    [Node] =:= filter_running([Node]).

-spec list_running() -> Nodes when
      Nodes :: [node()].
%% @doc Returns the list of nodes in the cluster where RabbitMQ is running.
%%
%% Note that even if RabbitMQ is running, the node could reject clients if it
%% is under maintenance.
%%
%% @see filter_running/1.
%% @see rabbit:is_running/0.

list_running() ->
    Members = list_members(),
    filter_running(Members).

-spec list_not_running() -> Nodes when
      Nodes :: [node()].
%% @doc Returns the list of nodes in the cluster where RabbitMQ is not running
%% or we can't reach.
%%
%% @see filter_not_running/1.
%% @see rabbit:is_running/0.

list_not_running() ->
    Members = list_members(),
    filter_not_running(Members).

-spec filter_running(Nodes) -> Nodes when
      Nodes :: [node()].
%% @doc Filters the given list of nodes to only select those belonging to the
%% cluster and where RabbitMQ is running.
%%
%% The cluster being considered is the one which the node running this
%% function belongs to.
%%
%% Note that even if RabbitMQ is running, the node could reject clients if it
%% is under maintenance.
%%
%% @see rabbit:is_running/0.

filter_running(Nodes) ->
    Members = filter_members(Nodes),
    do_filter_running(Members).

-spec filter_not_running(Nodes) -> Nodes when
      Nodes :: [node()].
%% @doc Filters the given list of nodes to only select those belonging to the
%% cluster and where RabbitMQ is not running or we can't reach.
%%
%% The cluster being considered is the one which the node running this
%% function belongs to.
%%
%% @see filter_running/1.

filter_not_running(Nodes) ->
    Members = filter_members(Nodes),
    Running = do_filter_running(Members),
    Members -- Running.

-spec do_filter_running(Members) -> Members when
      Members :: [node()].
%% @doc Filters the given list of cluster members to only select those who
%% run `rabbit'.
%%
%% Those nodes could run `rabbit' without accepting clients.
%%
%% The given list of nodes must have been verified to only contain cluster
%% members.
%%
%% @private

do_filter_running(Members) ->
    %% All clustered members where `rabbit' is running, regardless if they are
    %% under maintenance or not.
    Rets = erpc:multicall(
             Members, rabbit, is_running, [], ?FILTER_RPC_TIMEOUT),
    RetPerMember = lists:zip(Members, Rets),
    lists:filtermap(
      fun
          ({Member, {ok, true}}) ->
              {true, Member};
          ({_, {ok, false}}) ->
              false;
          ({_, {error, {erpc, Reason}}})
            when Reason =:= noconnection orelse Reason =:= timeout ->
              false;
          ({Member, Error}) ->
              ?LOG_ERROR(
                 "~s:~s: Failed to query node ~ts: ~p",
                 [?MODULE, ?FUNCTION_NAME, Member, Error],
                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
              false
      end, RetPerMember).

-spec is_serving(Node) -> IsServing when
      Node :: node(),
      IsServing :: boolean().
%% @doc Indicates if the given node is serving.
%%
%% @see filter_serving/1.

is_serving(Node) when is_atom(Node) ->
    [Node] =:= filter_serving([Node]).

-spec list_serving() -> Nodes when
      Nodes :: [node()].
%% @doc Returns the list of nodes in the cluster who accept clients.
%%
%% @see filter_serving/1.
%% @see rabbit:is_serving/0.

list_serving() ->
    Members = list_members(),
    filter_serving(Members).

-spec list_not_serving() -> Nodes when
      Nodes :: [node()].
%% @doc Returns the list of nodes in the cluster who reject clients, don't
%% run RabbitMQ or we can't reach.
%%
%% @see filter_serving/1.
%% @see rabbit:is_serving/0.

list_not_serving() ->
    Members = list_members(),
    filter_not_serving(Members).

-spec filter_serving(Nodes) -> Nodes when
      Nodes :: [node()].
%% @doc Filters the given list of nodes to only select those belonging to the
%% cluster and who accept clients.
%%
%% The cluster being considered is the one which the node running this
%% function belongs to.
%%
%% @see rabbit:is_serving/0.

filter_serving(Nodes) ->
    Members = filter_members(Nodes),
    do_filter_serving(Members).

-spec filter_not_serving(Nodes) -> Nodes when
      Nodes :: [node()].
%% @doc Filters the given list of nodes to only select those belonging to the
%% cluster and where RabbitMQ is rejecting clients, is not running or we can't
%% reach.
%%
%% The cluster being considered is the one which the node running this
%% function belongs to.
%%
%% @see filter_serving/1.

filter_not_serving(Nodes) ->
    Members = filter_members(Nodes),
    Serving = do_filter_serving(Members),
    Members -- Serving.

-spec do_filter_serving(Members) -> Members when
      Members :: [node()].
%% @doc Filters the given list of cluster members to only select those who
%% accept clients.
%%
%% The given list of nodes must have been verified to only contain cluster
%% members.
%%
%% @private

do_filter_serving(Members) ->
    %% All clustered members serving clients. This implies that `rabbit' is
    %% running.
    Rets = erpc:multicall(
             Members, rabbit, is_serving, [], ?FILTER_RPC_TIMEOUT),
    RetPerMember0 = lists:zip(Members, Rets),
    RetPerMember1 = handle_is_serving_undefined(RetPerMember0, []),
    lists:filtermap(
      fun
          ({Member, {ok, true}}) ->
              {true, Member};
          ({_, {ok, false}}) ->
              false;
          ({_, {error, {erpc, Reason}}})
            when Reason =:= noconnection orelse Reason =:= timeout ->
              false;
          ({Member, Error}) ->
              ?LOG_ERROR(
                 "~s:~s: Failed to query node ~ts: ~p",
                 [?MODULE, ?FUNCTION_NAME, Member, Error],
                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
              false
      end, RetPerMember1).

handle_is_serving_undefined(
  [{Member, {error, {exception, undef, [{rabbit, is_serving, [], _} | _]}}}
   | Rest],
  Result) ->
    %% The remote node must be RabbitMQ 3.11.x without the
    %% `rabbit:is_serving()' function. That's ok, we can perform two calls
    %% instead.
    %%
    %% This function can go away once we require a RabbitMQ version which has
    %% `rabbit:is_serving()'.
    ?LOG_NOTICE(
       "~s:~s: rabbit:is_serving() unavailable on node ~ts, falling back to "
       "two RPC calls",
       [?MODULE, ?FUNCTION_NAME, Member],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    try
        IsRunning = erpc:call(
                      Member, rabbit, is_running, [], ?FILTER_RPC_TIMEOUT),
        case IsRunning of
            true ->
                InMaintenance = erpc:call(
                                  Member,
                                  rabbit_maintenance,
                                  is_being_drained_local_read, [Member],
                                  ?FILTER_RPC_TIMEOUT),
                Result1 = [{Member, {ok, not InMaintenance}} | Result],
                handle_is_serving_undefined(Rest, Result1);
            false ->
                Result1 = [{Member, {ok, false}} | Result],
                handle_is_serving_undefined(Rest, Result1)
        end
    catch
        Class:Reason ->
            Result2 = [{Member, {Class, Reason}} | Result],
            handle_is_serving_undefined(Rest, Result2)
    end;
handle_is_serving_undefined(
  [Ret | Rest], Result) ->
    handle_is_serving_undefined(Rest, [Ret | Result]);
handle_is_serving_undefined(
  [], Result) ->
    lists:reverse(Result).

-spec running_count() -> integer().
running_count() -> length(list_running()).

-spec total_count() -> integer().
total_count() -> length(list_members()).

-spec is_single_node_cluster() -> boolean().
is_single_node_cluster() ->
    total_count() =:= 1.

-spec await_running_count(integer(), integer()) -> 'ok' | {'error', atom()}.
await_running_count(TargetCount, Timeout) ->
    Retries = round(Timeout/?SAMPLING_INTERVAL),
    await_running_count_with_retries(TargetCount, Retries).

await_running_count_with_retries(1, _Retries) -> ok;
await_running_count_with_retries(_TargetCount, Retries) when Retries =:= 0 ->
    {error, timeout};
await_running_count_with_retries(TargetCount, Retries) ->
    case running_count() >= TargetCount of
        true  -> ok;
        false ->
            timer:sleep(?SAMPLING_INTERVAL),
            await_running_count_with_retries(TargetCount, Retries - 1)
    end.

-spec all_running_with_hashes() -> #{non_neg_integer() => node()}.
all_running_with_hashes() ->
    maps:from_list([{erlang:phash2(Node), Node} || Node <- list_running()]).

-spec target_cluster_size_hint() -> non_neg_integer().
target_cluster_size_hint() ->
    cluster_formation_key_or_default(target_cluster_size_hint, ?DEFAULT_TARGET_CLUSTER_SIZE).

-spec reached_target_cluster_size() -> boolean().
reached_target_cluster_size() ->
    total_count() >= target_cluster_size_hint().

-spec if_reached_target_cluster_size(ConditionSatisfiedFun :: fun(), ConditionNotSatisfiedFun :: fun()) -> boolean().
if_reached_target_cluster_size(ConditionSatisfiedFun, ConditionNotSatisfiedFun) ->
    case reached_target_cluster_size() of
        true ->
            ConditionSatisfiedFun(),
            true;
        false ->
            ConditionNotSatisfiedFun(),
            false
    end.

-spec lock_id(Node :: node()) -> {ResourceId :: string(), LockRequesterId :: node()}.
lock_id(Node) ->
  {cookie_hash(), Node}.

-spec lock_retries() -> integer().
lock_retries() ->
    cluster_formation_key_or_default(internal_lock_retries, ?DEFAULT_LOCK_RETRIES).


%%
%% Implementation
%%

cluster_formation_key_or_default(Key, Default) ->
    case application:get_env(rabbit, cluster_formation) of
        {ok, PropList} ->
          proplists:get_value(Key, PropList, Default);
        undefined ->
            Default
    end.
