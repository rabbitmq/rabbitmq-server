%% The contents of this file are subject to the Mozilla Public License

%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_nodes).

-export([names/1, diagnostics/1, make/1, make/2, parts/1, cookie_hash/0,
         is_running/2, is_process_running/2,
         cluster_name/0, set_cluster_name/1, set_cluster_name/2, ensure_epmd/0,
         all_running/0, name_type/0, running_count/0,
         await_running_count/2,
         boot/0]).

-include_lib("kernel/include/inet.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(SAMPLING_INTERVAL, 1000).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

-spec names(string()) ->
          rabbit_types:ok_or_error2([{string(), integer()}], term()).
-spec diagnostics([node()]) -> string().
-spec cookie_hash() -> string().
-spec is_running(node(), atom()) -> boolean().
-spec is_process_running(node(), atom()) -> boolean().
-spec cluster_name() -> binary().
-spec all_running() -> [node()].
-spec running_count() -> integer().
%%----------------------------------------------------------------------------

boot() ->
  case application:get_env(rabbit, cluster_name) of
    undefined  -> ok;
    {ok, Name} ->
      rabbit_log:info("Setting cluster name to '~s' as configured", [Name]),
      set_cluster_name(rabbit_data_coercion:to_binary(Name))
  end.

name_type() ->
    case os:getenv("RABBITMQ_USE_LONGNAME") of
        "true" -> longnames;
        _      -> shortnames
    end.

names(Hostname) ->
    rabbit_nodes_common:names(Hostname).

diagnostics(Nodes) ->
    rabbit_nodes_common:diagnostics(Nodes).

make(NameOrParts) ->
    rabbit_nodes_common:make(NameOrParts).

make(ShortName, Hostname) ->
    make({ShortName, Hostname}).

parts(NodeStr) ->
    rabbit_nodes_common:parts(NodeStr).

cookie_hash() ->
    rabbit_nodes_common:cookie_hash().

is_running(Node, Application) ->
    rabbit_nodes_common:is_running(Node, Application).

is_process_running(Node, Process) ->
    rabbit_nodes_common:is_process_running(Node, Process).

cluster_name() ->
    rabbit_runtime_parameters:value_global(
      cluster_name, cluster_name_default()).

cluster_name_default() ->
    {ID, _} = parts(node()),
    FQDN = rabbit_net:hostname(),
    list_to_binary(atom_to_list(make({ID, FQDN}))).

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

all_running() -> rabbit_mnesia:cluster_nodes(running).

running_count() -> length(all_running()).

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
