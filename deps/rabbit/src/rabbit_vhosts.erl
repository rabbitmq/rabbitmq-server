%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% This module exists to avoid circular module dependencies between
%% several others virtual hosts-related modules.
-module(rabbit_vhosts).

-define(PERSISTENT_TERM_COUNTER_KEY, rabbit_vhosts_reconciliation_run_counter).

%% API

-export([
    list_names/0,
    exists/1,
    boot/0,
    reconcile/0,
    start_processes_for_all/0,
    start_on_all_nodes/2
]).

%% Same as rabbit_vhost:exists/1.
-spec exists(vhost:name()) -> boolean().
exists(VirtualHost) ->
    rabbit_db_vhost:exists(VirtualHost).

%% Same as rabbit_vhost:list_names/0.
-spec list_names() -> [vhost:name()].
list_names() -> rabbit_db_vhost:list().

-spec boot() -> 'ok'.
boot() ->
    _ = start_processes_for_all(),
    _ = increment_run_counter(),
    _ = maybe_start_timer(reconcile),
    ok.

%% Performs a round of virtual host process reconciliation. See start_processes_for_all/1.
-spec reconcile() -> 'ok'.
reconcile() ->
    rabbit_log:debug("Will reconcile virtual host processes on all cluster members..."),
    _ = start_processes_for_all(),
    _ = increment_run_counter(),
    N = get_run_counter(),
    rabbit_log:debug("Done with virtual host processes reconciliation (run ~tp)", [N]),
    _ = maybe_start_timer(?FUNCTION_NAME),
    ok.

%% Starts a virtual host process on every specified nodes.
%% Only exists to allow for "virtual host process repair"
%% in clusters where nodes a booted in parallel and seeded
%% (e.g. using definitions) at the same time.
%%
%% In that case, during virtual host insertion into the schema database,
%% some processes predictably won't be started on the yet-to-be-discovered nodes.
-spec start_processes_for_all([node()]) -> 'ok'.
start_processes_for_all(Nodes) ->
    Names = list_names(),
    N = length(Names),
    rabbit_log:debug("Will make sure that processes of ~p virtual hosts are running on all reachable cluster nodes", [N]),
    [begin
         try
             start_on_all_nodes(VH, Nodes)
         catch
             _:Err:_Stacktrace  ->
                 rabbit_log:error("Could not reconcile virtual host ~ts: ~tp", [VH, Err])
         end
     end || VH <- Names],
    ok.

-spec start_processes_for_all() -> 'ok'.
start_processes_for_all() ->
    start_processes_for_all(rabbit_nodes:list_reachable()).

%% Same as rabbit_vhost_sup_sup:start_on_all_nodes/0.
-spec start_on_all_nodes(vhost:name(), [node()]) -> 'ok'.
start_on_all_nodes(VirtualHost, Nodes) ->
    _ = rabbit_vhost_sup_sup:start_on_all_nodes(VirtualHost, Nodes),
    ok.

%%
%% Implementation
%%

-spec get_run_counter() -> non_neg_integer().
get_run_counter() ->
    persistent_term:get(?PERSISTENT_TERM_COUNTER_KEY, 0).

-spec increment_run_counter() -> non_neg_integer().
increment_run_counter() ->
    N = get_run_counter(),
    persistent_term:put(?PERSISTENT_TERM_COUNTER_KEY, N + 1),
    N.

-spec maybe_start_timer(atom()) -> ok | {ok, timer:tref()} | {error, any()}.
maybe_start_timer(FunName) ->
    N = get_run_counter(),
    DelayInSeconds = application:get_env(rabbit, vhost_process_reconciliation_run_interval, 30),
    case N >= 10 of
        true ->
            %% Stop after ten runs
            rabbit_log:debug("Will stop virtual host process reconciliation after ~tp runs", [N]),
            ok;
        false ->
            Delay = DelayInSeconds * 1000,
            rabbit_log:debug("Will reschedule virtual host process reconciliation after ~b seconds", [DelayInSeconds]),
            timer:apply_after(Delay, ?MODULE, FunName, [])
    end.