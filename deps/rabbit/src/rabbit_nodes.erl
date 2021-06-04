%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_nodes).

-export([names/1, diagnostics/1, make/1, make/2, parts/1, cookie_hash/0,
         is_running/2, is_process_running/2,
         cluster_name/0, set_cluster_name/1, set_cluster_name/2, ensure_epmd/0,
         all_running/0, name_type/0, running_count/0, total_count/0,
         await_running_count/2, is_single_node_cluster/0,
         boot/0]).
-export([persistent_cluster_id/0, seed_internal_cluster_id/0, seed_user_provided_cluster_name/0]).
-export([all_running_with_hashes/0]).

-include_lib("kernel/include/inet.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(SAMPLING_INTERVAL, 1000).

-define(INTERNAL_CLUSTER_ID_PARAM_NAME, internal_cluster_id).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

boot() ->
  seed_internal_cluster_id(),
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
            seed_internal_cluster_id(),
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
            rabbit_log:info("Initialising internal cluster ID to '~s'", [Id]),
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
            rabbit_log:info("Setting cluster name to '~s' as configured", [Name]),
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

-spec all_running() -> [node()].
all_running() -> rabbit_mnesia:cluster_nodes(running).

-spec running_count() -> integer().
running_count() -> length(all_running()).

-spec total_count() -> integer().
total_count() -> length(rabbit_mnesia:cluster_nodes(all)).

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
    maps:from_list([{erlang:phash2(Node), Node} || Node <- all_running()]).
