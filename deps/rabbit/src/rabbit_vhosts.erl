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
    reconcile_once/0,
    is_reconciliation_enabled/0,
    disable_reconciliation/0,
    enable_reconciliation/0,
    start_processes_for_all/0,
    start_on_all_nodes/2,
    on_node_up/1
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
    _ = case is_reconciliation_enabled() of
        false -> ok;
        true -> maybe_start_timer(reconcile)
    end,
    ok.

%% Performs a round of virtual host process reconciliation and sets up a timer to
%% re-run this operation again unless it has been run 10 or more times since cluster boot.
%% See start_processes_for_all/1.
-spec reconcile() -> 'ok'.
reconcile() ->
    case is_reconciliation_enabled() of
        false -> ok;
        true  ->
            _ = reconcile_once(),
            _ = maybe_start_timer(?FUNCTION_NAME),
            ok
    end.

%% Performs a round of virtual host process reconciliation but does not schedule any future runs.
%% See start_processes_for_all/1.
-spec reconcile_once() -> 'ok'.
reconcile_once() ->
    rabbit_log:debug("Will reconcile virtual host processes on all cluster members..."),
    _ = start_processes_for_all(),
    _ = increment_run_counter(),
    N = get_run_counter(),
    rabbit_log:debug("Done with virtual host processes reconciliation (run ~tp)", [N]),
    ok.

-spec on_node_up(Node :: node()) -> 'ok'.
on_node_up(_Node) ->
    case is_reconciliation_enabled() of
        false -> ok;
        true  ->
            DelayInSeconds = 10,
            Delay = DelayInSeconds * 1000,
            rabbit_log:debug("Will reschedule virtual host process reconciliation after ~b seconds", [DelayInSeconds]),
            _ = timer:apply_after(Delay, ?MODULE, reconcile_once, []),
            ok
    end.

-spec is_reconciliation_enabled() -> boolean().
is_reconciliation_enabled() ->
    application:get_env(rabbit, vhost_process_reconciliation_enabled, true).

-spec enable_reconciliation() -> 'ok'.
enable_reconciliation() ->
    %% reset the auto-stop counter
    persistent_term:put(?PERSISTENT_TERM_COUNTER_KEY, 0),
    application:set_env(rabbit, vhost_process_reconciliation_enabled, true).

-spec disable_reconciliation() -> 'ok'.
disable_reconciliation() ->
    application:set_env(rabbit, vhost_process_reconciliation_enabled, false).

-spec reconciliation_interval() -> non_neg_integer().
reconciliation_interval() ->
    application:get_env(rabbit, vhost_process_reconciliation_run_interval, 30).

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
    DelayInSeconds = reconciliation_interval(),
    case N >= 10 of
        true ->
            %% Stop after ten runs
            rabbit_log:debug("Will stop virtual host process reconciliation after ~tp runs", [N]),
            ok;
        false ->
            case is_reconciliation_enabled() of
                false -> ok;
                true  ->
                    Delay = DelayInSeconds * 1000,
                    rabbit_log:debug("Will reschedule virtual host process reconciliation after ~b seconds", [DelayInSeconds]),
                    timer:apply_after(Delay, ?MODULE, FunName, [])
            end
    end.
