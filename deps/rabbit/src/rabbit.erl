%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit).

-include_lib("stdlib/include/assert.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-behaviour(application).

-export([start/0, boot/0, stop/0,
         stop_and_halt/0, await_startup/0, await_startup/1, await_startup/3,
         status/0, is_running/0, is_serving/0, alarms/0,
         is_running/1, is_serving/1, environment/0, rotate_logs/0,
         force_event_refresh/1,
         start_fhc/0]).

-export([start/2, stop/1, prep_stop/1]).
-export([start_apps/1, start_apps/2, stop_apps/1]).
-export([data_dir/0]).
-export([product_info/0,
         product_name/0,
         product_version/0,
         product_license_line/0,
         base_product_name/0,
         base_product_version/0,
         motd_file/0,
         motd/0,
         pg_local_scope/1]).
%% For CLI, testing and mgmt-agent.
-export([set_log_level/1, log_locations/0, config_files/0]).
-export([is_booted/1, is_booted/0, is_booting/1, is_booting/0]).

%%---------------------------------------------------------------------------
%% Boot steps.
-export([update_cluster_tags/0, maybe_insert_default_data/0, boot_delegate/0, recover/0,
         pg_local_amqp_session/0,
         pg_local_amqp_connection/0, prevent_startup_if_node_was_reset/0]).

-rabbit_boot_step({pre_boot, [{description, "rabbit boot start"}]}).

-rabbit_boot_step({codec_correctness_check,
                   [{description, "codec correctness check"},
                    {mfa,         {rabbit_binary_generator,
                                   check_empty_frame_size,
                                   []}},
                    {requires,    pre_boot},
                    {enables,     external_infrastructure}]}).

%% rabbit_alarm currently starts memory and disk space monitors
-rabbit_boot_step({rabbit_alarm,
                   [{description, "alarm handler"},
                    {mfa,         {rabbit_alarm, start, []}},
                    {requires,    pre_boot},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step({feature_flags,
                   [{description, "feature flags registry and initial state"},
                    {mfa,         {rabbit_feature_flags, init, []}},
                    {requires,    pre_boot},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step({rabbit_registry,
                   [{description, "plugin registry"},
                    {mfa,         {rabbit_sup, start_child,
                                   [rabbit_registry]}},
                    {requires,    pre_boot},
                    {enables,     database}]}).

-rabbit_boot_step({database,
                   [{mfa,         {rabbit_db, init, []}},
                    {requires,    file_handle_cache},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step({networking_metadata_store,
                   [{description, "networking infrastructure"},
                    {mfa,         {rabbit_sup, start_child, [rabbit_networking_store]}},
                    {requires,    database},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step({tracking_metadata_store,
                   [{description, "tracking infrastructure"},
                    {mfa,         {rabbit_sup, start_child, [rabbit_tracking_store]}},
                    {requires,    database},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step({code_server_cache,
                   [{description, "code_server cache server"},
                    {mfa,         {rabbit_sup, start_child, [code_server_cache]}},
                    {requires,    rabbit_alarm},
                    {enables,     file_handle_cache}]}).

-rabbit_boot_step({file_handle_cache,
                   [{description, "file handle cache server"},
                    {mfa,         {rabbit, start_fhc, []}},
                    %% FHC needs memory monitor to be running
                    {requires,    code_server_cache},
                    {enables,     worker_pool}]}).

-rabbit_boot_step({worker_pool,
                   [{description, "default worker pool"},
                    {mfa,         {rabbit_sup, start_supervisor_child,
                                   [worker_pool_sup]}},
                    {requires,    pre_boot},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step({definition_import_worker_pool,
                   [{description, "dedicated worker pool for definition import"},
                    {mfa,         {rabbit_definitions, boot, []}},
                    {requires,    external_infrastructure}]}).

-rabbit_boot_step({external_infrastructure,
                   [{description, "external infrastructure ready"}]}).

-rabbit_boot_step({rabbit_core_metrics,
                   [{description, "core metrics storage"},
                    {mfa,         {rabbit_sup, start_child,
                                   [rabbit_metrics]}},
                    {requires,    pre_boot},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step({rabbit_osiris_metrics,
                   [{description, "osiris metrics scraper"},
                    {mfa,         {rabbit_sup, start_child,
                                   [rabbit_osiris_metrics]}},
                    {requires,    pre_boot},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step({rabbit_global_counters,
                   [{description, "global counters"},
                    {mfa,         {rabbit_global_counters, boot_step,
                                   []}},
                    {requires,    pre_boot},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step({rabbit_event,
                   [{description, "statistics event manager"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_event]}},
                    {requires,    external_infrastructure},
                    {enables,     kernel_ready}]}).

-rabbit_boot_step({kernel_ready,
                   [{description, "kernel ready"},
                    {requires,    external_infrastructure}]}).

-rabbit_boot_step({guid_generator,
                   [{description, "guid generator"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_guid]}},
                    {requires,    kernel_ready},
                    {enables,     core_initialized}]}).

-rabbit_boot_step({delegate_sup,
                   [{description, "cluster delegate"},
                    {mfa,         {rabbit, boot_delegate, []}},
                    {requires,    kernel_ready},
                    {enables,     core_initialized}]}).

-rabbit_boot_step({rabbit_node_monitor,
                   [{description, "node monitor"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_node_monitor]}},
                    {requires,    [rabbit_alarm, guid_generator]},
                    {enables,     core_initialized}]}).

-rabbit_boot_step({rabbit_quorum_queue_periodic_membership_reconciliation,
                   [{description, "Quorums Queue membership reconciliation"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_quorum_queue_periodic_membership_reconciliation]}},
                    {requires, [database]}]}).

-rabbit_boot_step({rabbit_epmd_monitor,
                   [{description, "epmd monitor"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_epmd_monitor]}},
                    {requires,    kernel_ready},
                    {enables,     core_initialized}]}).

-rabbit_boot_step({rabbit_sysmon_minder,
                   [{description, "sysmon_handler supervisor"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_sysmon_minder]}},
                    {requires,    kernel_ready},
                    {enables,     core_initialized}]}).

-rabbit_boot_step({core_initialized,
                   [{description, "core initialized"},
                    {requires,    kernel_ready}]}).

-rabbit_boot_step({recovery,
                   [{description, "exchange, queue and binding recovery"},
                    {mfa,         {rabbit, recover, []}},
                    {requires,    [core_initialized]},
                    {enables,     routing_ready}]}).

-rabbit_boot_step({prevent_startup_if_node_was_reset,
                   [{description, "prevents node boot if a prior boot marker file exists but the database is not seeded (requires opt-in configuration in rabbitmq.conf)"},
                    {mfa,         {?MODULE, prevent_startup_if_node_was_reset, []}},
                    {requires,    recovery},
                    {enables,     empty_db_check}]}).

-rabbit_boot_step({empty_db_check,
                   [{description, "empty DB check"},
                    {mfa,         {?MODULE, maybe_insert_default_data, []}},
                    {requires,    prevent_startup_if_node_was_reset},
                    {enables,     routing_ready}]}).


-rabbit_boot_step({cluster_tags,
                   [{description, "Set cluster tags"},
                    {mfa,         {?MODULE, update_cluster_tags, []}},
                    {requires,    core_initialized}]}).

-rabbit_boot_step({routing_ready,
                   [{description, "message delivery logic ready"},
                    {requires,    [core_initialized, recovery]}]}).

-rabbit_boot_step({background_gc,
                   [{description, "background garbage collection"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [background_gc]}},
                    {requires,    [core_initialized, recovery]},
                    {enables,     routing_ready}]}).

-rabbit_boot_step({rabbit_core_metrics_gc,
                   [{description, "background core metrics garbage collection"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_core_metrics_gc]}},
                    {requires,    [core_initialized, recovery]},
                    {enables,     routing_ready}]}).

-rabbit_boot_step({rabbit_observer_cli,
                   [{description, "Observer CLI configuration"},
                    {mfa,         {rabbit_observer_cli, init, []}},
                    {requires,    [core_initialized, recovery]},
                    {enables,     routing_ready}]}).


-rabbit_boot_step({pre_flight,
                   [{description, "ready to communicate with peers and clients"},
                    {requires,    [core_initialized, recovery, routing_ready]}]}).

-rabbit_boot_step({cluster_name,
                   [{description, "sets cluster name if configured"},
                    {mfa,         {rabbit_nodes, boot, []}},
                    {requires,    pre_flight}
                    ]}).

-rabbit_boot_step({direct_client,
                   [{description, "direct client"},
                    {mfa,         {rabbit_direct, boot, []}},
                    {requires,    pre_flight}
                    ]}).

-rabbit_boot_step({notify_cluster,
                   [{description, "notifies cluster peers of our presence"},
                    {mfa,         {rabbit_node_monitor, notify_node_up, []}},
                    {requires,    pre_flight}]}).

-rabbit_boot_step({networking,
                   [{description, "TCP and TLS listeners (backwards compatibility)"},
                    {mfa,         {logger, debug, ["'networking' boot step skipped and moved to end of startup", [], #{domain => ?RMQLOG_DOMAIN_GLOBAL}]}},
                    {requires,    notify_cluster}]}).

%% This mechanism is necessary in environments where a cluster is formed in parallel,
%% which is the case with many container orchestration tools.
%% In such scenarios, a virtual host can be declared before the cluster is formed and all
%% cluster members are known, e.g. via definition import.
-rabbit_boot_step({virtual_host_reconciliation,
    [{description, "makes sure all virtual host have running processes on all nodes"},
        {mfa,         {rabbit_vhosts, boot, []}},
        {requires,    notify_cluster}]}).

-rabbit_boot_step({pg_local_amqp_session,
                   [{description, "local-only pg scope for AMQP sessions"},
                    {mfa,         {rabbit, pg_local_amqp_session, []}},
                    {requires,    kernel_ready},
                    {enables,     core_initialized}]}).

-rabbit_boot_step({pg_local_amqp_connection,
                   [{description, "local-only pg scope for AMQP connections"},
                    {mfa,         {rabbit, pg_local_amqp_connection, []}},
                    {requires,    kernel_ready},
                    {enables,     core_initialized}]}).

%%---------------------------------------------------------------------------

-include_lib("rabbit_common/include/rabbit.hrl").

-define(APPS, [os_mon, mnesia, rabbit_common, rabbitmq_prelaunch, ra, sysmon_handler, rabbit, osiris]).

%% 1 minute
-define(BOOT_START_TIMEOUT,     1 * 60 * 1000).
%% 12 hours
-define(BOOT_FINISH_TIMEOUT,    12 * 60 * 60 * 1000).
%% 100 ms
-define(BOOT_STATUS_CHECK_INTERVAL, 100).

%%----------------------------------------------------------------------------

-type restart_type() :: 'permanent' | 'transient' | 'temporary'.

-type param() :: atom().
-type app_name() :: atom().

%%----------------------------------------------------------------------------

-spec start() -> 'ok'.

start() ->
    %% start() vs. boot(): we want to throw an error in start().
    start_it(temporary).

-spec boot() -> 'ok'.

boot() ->
    %% start() vs. boot(): we want the node to exit in boot(). Because
    %% applications are started with `transient`, any error during their
    %% startup will abort the node.
    start_it(transient).

run_prelaunch_second_phase() ->
    %% Finish the prelaunch phase started by the `rabbitmq_prelaunch`
    %% application.
    %%
    %% The first phase was handled by the `rabbitmq_prelaunch`
    %% application. It was started in one of the following way:
    %%   - from an Erlang release boot script;
    %%   - from the rabbit:boot/0 or rabbit:start/0 functions.
    %%
    %% The `rabbitmq_prelaunch` application creates the context map from
    %% the environment and the configuration files early during Erlang
    %% VM startup. Once it is done, all application environments are
    %% configured (in particular `mnesia` and `ra`).
    %%
    %% This second phase depends on other modules & facilities of
    %% RabbitMQ core. That's why we need to run it now, from the
    %% `rabbit` application start function.

    %% We assert Mnesia is stopped before we run the prelaunch
    %% phases. See `rabbit_prelaunch` for an explanation.
    %%
    %% This is the second assertion, just in case Mnesia is started
    %% between the two prelaunch phases.
    rabbit_prelaunch:assert_mnesia_is_stopped(),

    %% Get the context created by `rabbitmq_prelaunch` then proceed
    %% with all steps in this phase.
    #{initial_pass := IsInitialPass} =
    Context = rabbit_prelaunch:get_context(),

    case IsInitialPass of
        true ->
            ?LOG_DEBUG(""),
            ?LOG_DEBUG(
              "== Prelaunch phase [2/2] (initial pass) ==");
        false ->
            ?LOG_DEBUG(""),
            ?LOG_DEBUG("== Prelaunch phase [2/2] =="),
            ok
    end,

    %% 1. Enabled plugins file.
    ok = rabbit_prelaunch_enabled_plugins_file:setup(Context),

    %% 2. Feature flags registry.
    ok = rabbit_prelaunch_feature_flags:setup(Context),

    %% 3. Logging.
    ok = rabbit_prelaunch_logging:setup(Context),

    %% The clustering steps requires Khepri to be started to check for
    %% consistency. This is the opposite compared to Mnesia which must be
    %% stopped. That's why we setup Khepri and the coordination Ra system it
    %% depends on before, but only handle Mnesia after.
    %%
    %% We also always set it up, even when using Mnesia, to ensure it is ready
    %% if/when the migration begins.
    %%
    %% Note that this is only the Khepri store which is started here. We
    %% perform additional initialization steps in `rabbit_db:init()' which is
    %% triggered from a boot step. This boot step handles both Mnesia and
    %% Khepri and synchronizes the feature flags.
    %%
    %% To sum up:
    %% 1. We start the Khepri store (always)
    %% 2. We verify the cluster, including the feature flags compatibility
    %% 3. We start Mnesia (if Khepri is unused)
    %% 4. We synchronize feature flags in `rabbit_db:init()'
    %% 4. We finish to initialize either Mnesia or Khepri in `rabbit_db:init()'
    ok = rabbit_ra_systems:setup(Context),
    ok = rabbit_khepri:setup(Context),

    %% 4. Clustering checks. This covers the compatibility between nodes,
    %% feature-flags-wise.
    ok = rabbit_prelaunch_cluster:setup(Context),

    case rabbit_khepri:is_enabled() of
        true ->
            ok;
        false ->
            %% Start Mnesia now that everything is ready.
            ?LOG_DEBUG("Starting Mnesia"),
            ok = mnesia:start()
    end,

    ?LOG_DEBUG(""),
    ?LOG_DEBUG("== Prelaunch DONE =="),

    case IsInitialPass of
        true  -> rabbit_prelaunch:initial_pass_finished();
        false -> ok
    end,
    ok.

start_it(StartType) ->
    case spawn_boot_marker() of
        {ok, Marker} ->
            ?LOG_INFO("RabbitMQ is asked to start...", [],
                      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            try
                {Millis, ok} = timer:tc(
                                 fun() ->
                                         {ok, _} = application:ensure_all_started(
                                                     rabbitmq_prelaunch, StartType),
                                         {ok, _} = application:ensure_all_started(
                                                     rabbit, StartType),
                                         wait_for_ready_or_stopped()
                                 end, millisecond),
                ?LOG_INFO("Time to start RabbitMQ: ~b ms", [Millis]),
                stop_boot_marker(Marker),
                ok
            catch
                error:{badmatch, Error}:_ ->
                    %% `rabbitmq_prelaunch' was started before `rabbit' above.
                    %% If the latter fails to start, we must stop the former as
                    %% well.
                    %%
                    %% This is important if the environment changes between
                    %% that error and the next attempt to start `rabbit': the
                    %% environment is only read during the start of
                    %% `rabbitmq_prelaunch' (and the cached context is cleaned
                    %% on stop).
                    _ = application:stop(rabbitmq_prelaunch),
                    stop_boot_marker(Marker),
                    case StartType of
                        temporary -> throw(Error);
                        _         -> exit(Error)
                    end
            end;
        {already_booting, Marker} ->
            stop_boot_marker(Marker),
            ok
    end.

wait_for_ready_or_stopped() ->
    ok = rabbit_boot_state:wait_for(ready, ?BOOT_FINISH_TIMEOUT),
    case rabbit_boot_state:get() of
        ready ->
            ok;
        _ ->
            ok = rabbit_boot_state:wait_for(stopped, ?BOOT_FINISH_TIMEOUT),
            rabbit_prelaunch:get_stop_reason()
    end.

spawn_boot_marker() ->
    %% Compatibility with older RabbitMQ versions:
    %% We register a process doing nothing to indicate that RabbitMQ is
    %% booting. This is checked by `is_booting(Node)` on a remote node.
    Marker = spawn_link(fun() -> receive stop -> ok end end),
    case catch register(rabbit_boot, Marker) of
        true -> {ok, Marker};
        _    -> {already_booting, Marker}
    end.

stop_boot_marker(Marker) ->
    unlink(Marker),
    Marker ! stop,
    ok.

-spec stop() -> 'ok'.

stop() ->
    case wait_for_ready_or_stopped() of
        ok ->
            case rabbit_boot_state:get() of
                ready ->
                    Product = product_name(),
                    ?LOG_INFO("~ts is asked to stop...", [Product],
                              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                    do_stop(),
                    ?LOG_INFO(
                      "Successfully stopped ~ts and its dependencies",
                      [Product],
                      #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                    ok;
                stopped ->
                    ok
            end;
        _ ->
            ok
    end.

do_stop() ->
    Apps0 = ?APPS ++ rabbit_plugins:active(),
    %% We ensure that Mnesia is stopped last (or more exactly, after rabbit).
    Apps1 = app_utils:app_dependency_order(Apps0, true) -- [mnesia],
    Apps = [mnesia | Apps1],
    %% this will also perform unregistration with the peer discovery backend
    %% as needed
    stop_apps(Apps).

-spec stop_and_halt() -> no_return().

stop_and_halt() ->
    try
        stop()
    catch Type:Reason ->
        ?LOG_ERROR(
          "Error trying to stop ~ts: ~tp:~tp",
          [product_name(), Type, Reason],
          #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
        error({Type, Reason})
    after
        %% Enclose all the logging in the try block.
        %% init:stop() will be called regardless of any errors.
        try
            AppsLeft = [ A || {A, _, _} <- application:which_applications() ],
            ?LOG_INFO(
                lists:flatten(
                  ["Halting Erlang VM with the following applications:~n",
                   ["    ~tp~n" || _ <- AppsLeft]]),
                AppsLeft,
                #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            %% Also duplicate this information to stderr, so console where
            %% foreground broker was running (or systemd journal) will
            %% contain information about graceful termination.
            io:format(standard_error, "Gracefully halting Erlang VM~n", [])
        after
            init:stop()
        end
    end,
    ok.

-spec start_apps([app_name()]) -> 'ok'.

start_apps(Apps) ->
    start_apps(Apps, #{}).

-spec start_apps([app_name()],
                 #{app_name() => restart_type()}) -> 'ok'.

%% TODO: start_apps/2 and is now specific to plugins. This function
%% should be moved over `rabbit_plugins', along with stop_apps/1, once
%% the latter stops using app_utils as well.

start_apps(Apps, RestartTypes) ->
    false = lists:member(rabbit, Apps), %% Assertion.
    %% We need to load all applications involved in order to be able to
    %% find new feature flags.
    app_utils:load_applications(Apps),
    case rabbit_feature_flags:refresh_feature_flags_after_app_load() of
        ok    -> ok;
        Error -> throw(Error)
    end,
    rabbit_prelaunch_conf:decrypt_config(Apps),
    lists:foreach(
      fun(App) ->
              RestartType = maps:get(App, RestartTypes, temporary),
              ok = rabbit_boot_steps:run_boot_steps([App]),
              case application:ensure_all_started(App, RestartType) of
                  {ok, _}         -> ok;
                  {error, Reason} -> throw({could_not_start, App, Reason})
              end
      end, Apps).

-spec stop_apps([app_name()]) -> 'ok'.

stop_apps([]) ->
    ok;
stop_apps(Apps) ->
    ?LOG_INFO(
        lists:flatten(
          ["Stopping ~ts applications and their dependencies in the following order:~n",
           ["    ~tp~n" || _ <- Apps]]),
        [product_name() | lists:reverse(Apps)],
        #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ok = app_utils:stop_applications(
           Apps, handle_app_error(error_during_shutdown)),
    case lists:member(rabbit, Apps) of
        %% plugin deactivation
        false -> rabbit_boot_steps:run_cleanup_steps(Apps);
        true  -> ok %% it's all going anyway
    end,
    ok.

-spec handle_app_error(_) -> fun((_, _) -> no_return()).
handle_app_error(Term) ->
    fun(App, {bad_return, {_MFA, {'EXIT', ExitReason}}}) ->
            throw({Term, App, ExitReason});
       (App, Reason) ->
            throw({Term, App, Reason})
    end.

is_booting() -> is_booting(node()).

is_booting(Node) when Node =:= node() ->
    rabbit_boot_state:has_reached_and_is_active(booting)
    andalso
    not rabbit_boot_state:has_reached(ready);
is_booting(Node) ->
    case rpc:call(Node, rabbit, is_booting, []) of
        {badrpc, _} = Err -> Err;
        Ret               -> Ret
    end.


-spec await_startup() -> 'ok' | {'error', 'timeout'}.

await_startup() ->
    await_startup(node(), false).

-spec await_startup(node() | non_neg_integer()) -> 'ok' | {'error', 'timeout'}.

await_startup(Node) when is_atom(Node) ->
    await_startup(Node, false);
  await_startup(Timeout) when is_integer(Timeout) ->
      await_startup(node(), false, Timeout).

-spec await_startup(node(), boolean()) -> 'ok'  | {'error', 'timeout'}.

await_startup(Node, PrintProgressReports) ->
    case is_booting(Node) of
        true  -> wait_for_boot_to_finish(Node, PrintProgressReports);
        false ->
            case is_running(Node) of
                true  -> ok;
                false -> _ = _ = wait_for_boot_to_start(Node),
                         wait_for_boot_to_finish(Node, PrintProgressReports)
            end
    end.

-spec await_startup(node(), boolean(), non_neg_integer()) -> 'ok'  | {'error', 'timeout'}.

await_startup(Node, PrintProgressReports, Timeout) ->
    case is_booting(Node) of
        true  -> wait_for_boot_to_finish(Node, PrintProgressReports, Timeout);
        false ->
            case is_running(Node) of
                true  -> ok;
                false -> _ = wait_for_boot_to_start(Node, Timeout),
                         wait_for_boot_to_finish(Node, PrintProgressReports, Timeout)
            end
    end.

wait_for_boot_to_start(Node) ->
    wait_for_boot_to_start(Node, ?BOOT_START_TIMEOUT).

wait_for_boot_to_start(Node, infinity) ->
    %% This assumes that 100K iterations is close enough to "infinity".
    %% Now that's deep.
    do_wait_for_boot_to_start(Node, 100000);
wait_for_boot_to_start(Node, Timeout) ->
    Iterations = Timeout div ?BOOT_STATUS_CHECK_INTERVAL,
    do_wait_for_boot_to_start(Node, Iterations).

do_wait_for_boot_to_start(_Node, IterationsLeft) when IterationsLeft =< 0 ->
    {error, timeout};
do_wait_for_boot_to_start(Node, IterationsLeft) ->
    case is_booting(Node) of
        false ->
            timer:sleep(?BOOT_STATUS_CHECK_INTERVAL),
            do_wait_for_boot_to_start(Node, IterationsLeft - 1);
        {badrpc, _} = Err ->
            Err;
        true  ->
            ok
    end.

wait_for_boot_to_finish(Node, PrintProgressReports) ->
    wait_for_boot_to_finish(Node, PrintProgressReports, ?BOOT_FINISH_TIMEOUT).

wait_for_boot_to_finish(Node, PrintProgressReports, infinity) ->
    %% This assumes that 100K iterations is close enough to "infinity".
    %% Now that's deep.
    do_wait_for_boot_to_finish(Node, PrintProgressReports, 100000);
wait_for_boot_to_finish(Node, PrintProgressReports, Timeout) ->
    Iterations = Timeout div ?BOOT_STATUS_CHECK_INTERVAL,
    do_wait_for_boot_to_finish(Node, PrintProgressReports, Iterations).

do_wait_for_boot_to_finish(_Node, _PrintProgressReports, IterationsLeft) when IterationsLeft =< 0 ->
    {error, timeout};
do_wait_for_boot_to_finish(Node, PrintProgressReports, IterationsLeft) ->
    case is_booting(Node) of
        false ->
            %% We don't want badrpc error to be interpreted as false,
            %% so we don't call rabbit:is_running(Node)
            case rpc:call(Node, rabbit, is_running, []) of
                true              -> ok;
                false             -> {error, rabbit_is_not_running};
                {badrpc, _} = Err -> Err
            end;
        {badrpc, _} = Err ->
            Err;
        true  ->
            maybe_print_boot_progress(PrintProgressReports, IterationsLeft),
            timer:sleep(?BOOT_STATUS_CHECK_INTERVAL),
            do_wait_for_boot_to_finish(Node, PrintProgressReports, IterationsLeft - 1)
    end.

maybe_print_boot_progress(false = _PrintProgressReports, _IterationsLeft) ->
  ok;
maybe_print_boot_progress(true, IterationsLeft) ->
  case IterationsLeft rem 100 of
    %% This will be printed on the CLI command end to illustrate some
    %% progress.
    0 -> io:format("Still booting, will check again in 10 seconds...~n");
    _ -> ok
  end.

-spec status
        () -> [{pid, integer()} |
               {running_applications, [{atom(), string(), string()}]} |
               {os, {atom(), atom()}} |
               {erlang_version, string()} |
               {memory, any()}].

status() ->
    Version = base_product_version(),
    [CryptoLibInfo] = crypto:info_lib(),
    S1 = [{pid,                  list_to_integer(os:getpid())},
          %% The timeout value used is twice that of gen_server:call/2.
          {running_applications, rabbit_misc:which_applications()},
          {os,                   os:type()},
          {rabbitmq_version,     Version},
          {crypto_lib_info,      CryptoLibInfo},
          {erlang_version,       erlang:system_info(system_version)},
          {memory,               rabbit_vm:memory()},
          {alarms,               alarms()},
          {tags,                 tags()},
          {is_under_maintenance, rabbit_maintenance:is_being_drained_local_read(node())},
          {listeners,            listeners()},
          {vm_memory_calculation_strategy, vm_memory_monitor:get_memory_calculation_strategy()}],
    S2 = rabbit_misc:filter_exit_map(
           fun ({Key, {M, F, A}}) -> {Key, erlang:apply(M, F, A)} end,
           [{vm_memory_high_watermark, {vm_memory_monitor,
                                        get_vm_memory_high_watermark, []}},
            {vm_memory_limit,          {vm_memory_monitor,
                                        get_memory_limit, []}},
            {disk_free_limit,          {rabbit_disk_monitor,
                                        get_disk_free_limit, []}},
            {disk_free,                {rabbit_disk_monitor,
                                        get_disk_free, []}}]),
    S3 = rabbit_misc:with_exit_handler(
           fun () -> [] end,
           fun () -> [{file_descriptors, file_handle_cache:info()}] end),
    S4 = [{processes,        [{limit, erlang:system_info(process_limit)},
                              {used, erlang:system_info(process_count)}]},
          {run_queue,        erlang:statistics(run_queue)},
          {uptime,           begin
                                 {T,_} = erlang:statistics(wall_clock),
                                 T div 1000
                             end},
          {kernel,           {net_ticktime, net_kernel:get_net_ticktime()}}],
    S5 = [{active_plugins, rabbit_plugins:active()},
          {enabled_plugin_file, rabbit_plugins:enabled_plugins_file()}],
    S6 = [{config_files, config_files()},
           {log_files, log_locations()},
           {data_directory, data_dir()},
           {raft_data_directory, ra_env:data_dir()}],
    Totals = case is_running() of
                 true ->
                     [{virtual_host_count, rabbit_vhost:count()},
                      {connection_count,
                       length(rabbit_networking:local_connections()) +
                       length(rabbit_networking:local_non_amqp_connections())},
                      {queue_count, total_queue_count()}];
                 false ->
                     []
             end,
    S7 = [{totals, Totals}],
    S8 = lists:filter(
           fun
               ({product_base_name, _}) -> true;
               ({product_base_version, _}) -> true;
               ({product_name, _}) -> true;
               ({product_version, _}) -> true;
               (_) -> false
           end,
           maps:to_list(product_info())),
    S1 ++ S2 ++ S3 ++ S4 ++ S5 ++ S6 ++ S7 ++ S8.

alarms() ->
    Alarms = rabbit_misc:with_exit_handler(rabbit_misc:const([]),
                                           fun rabbit_alarm:get_alarms/0),
    N = node(),
    %% [{{resource_limit,memory,rabbit@mercurio},[]}]
    [{resource_limit, Limit, Node} || {{resource_limit, Limit, Node}, _} <- Alarms, Node =:= N].

tags() ->
    application:get_env(rabbit, node_tags, []).

listeners() ->
    Listeners = try
                    rabbit_networking:active_listeners()
                catch
                    exit:{aborted, _} -> []
                end,
    [L || L = #listener{node = Node} <- Listeners, Node =:= node()].

total_queue_count() ->
    lists:foldl(fun (VirtualHost, Acc) ->
                  Acc + rabbit_amqqueue:count(VirtualHost)
                end,
                0, rabbit_vhost:list_names()).

-spec is_serving() -> IsServing when
      IsServing :: boolean().
%% @doc Indicates if this RabbitMQ node is ready to serve clients or not.
%%
%% It differs from {@link is_running/0} in the sense that a running RabbitMQ
%% node could be under maintenance. A serving node is one where RabbitMQ is
%% running and is not under maintenance currently, thus accepting clients.

is_serving() ->
    ThisNode = node(),
    is_running() andalso
    not rabbit_maintenance:is_being_drained_local_read(ThisNode).

-spec is_serving(Node) -> IsServing when
      Node :: node(),
      IsServing :: boolean().
%% @doc Indicates if the given node is ready to serve client or not.

is_serving(Node) when Node =:= node() ->
    is_serving();
is_serving(Node) ->
    case rpc:call(Node, rabbit, is_serving, []) of
        true -> true;
        _    -> false
    end.

-spec is_running() -> IsRunning when
      IsRunning :: boolean().
%% @doc Indicates if the `rabbit' application is running on this RabbitMQ node
%% or not.
%%
%% A RabbitMQ node is considered to run as soon as the `rabbit' application is
%% running. It means this node participates to the cluster. However, it could
%% accept or reject clients if it is under maintenance. See {@link
%% is_serving/0} to check if this node is running and is ready to serve
%% clients.

is_running() -> is_running(node()).

-spec is_running(node()) -> boolean().

is_running(Node) when Node =:= node() ->
    case rabbit_boot_state:get() of
        ready       -> true;
        _           -> false
    end;
is_running(Node) ->
    case rpc:call(Node, rabbit, is_running, []) of
        true -> true;
        _    -> false
    end.

is_booted() -> is_booted(node()).

is_booted(Node) ->
    case is_booting(Node) of
        false ->
            is_running(Node);
        _ -> false
    end.

-spec environment() -> [{param(), term()}].

environment() ->
    %% The timeout value is twice that of gen_server:call/2.
    [{A, environment(A)} ||
        {A, _, _} <- lists:keysort(1, application:which_applications(10000))].

environment(App) ->
    Ignore = [default_pass, included_applications],
    lists:keysort(1, [P || P = {K, _} <- application:get_all_env(App),
                           not lists:member(K, Ignore)]).

-spec rotate_logs() -> rabbit_types:ok_or_error(any()).

rotate_logs() ->
    ?LOG_ERROR(
       "Forcing log rotation is currently unsupported",
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    {error, unsupported}.

%%--------------------------------------------------------------------

-spec start('normal',[]) ->
          {'error',
           {'erlang_version_too_old',
            {'found',string(),string()},
            {'required',string(),string()}}} |
          {'ok',pid()}.

start(normal, []) ->
    %% Reset boot state and clear the stop reason again (it was already
    %% made in rabbitmq_prelaunch).
    %%
    %% This is important if the previous startup attempt failed after
    %% rabbitmq_prelaunch was started and the application is still
    %% running.
    rabbit_boot_state:set(booting),
    rabbit_prelaunch:clear_stop_reason(),

    try
        run_prelaunch_second_phase(),

        ProductInfo = product_info(),
        case ProductInfo of
            #{product_overridden := true,
              product_base_name := BaseName,
              product_base_version := BaseVersion} ->
                ?LOG_INFO(
                   "~n Starting ~ts ~ts on Erlang ~ts [~ts]~n Based on ~ts ~ts~n ~ts~n ~ts",
                   [product_name(), product_version(), rabbit_misc:otp_release(),
                    emu_flavor(),
                    BaseName, BaseVersion,
                    ?COPYRIGHT_MESSAGE, product_license_line()],
                   #{domain => ?RMQLOG_DOMAIN_PRELAUNCH});
            _ ->
                ?LOG_INFO(
                   "~n Starting ~ts ~ts on Erlang ~ts [~ts]~n ~ts~n ~ts",
                   [product_name(), product_version(), rabbit_misc:otp_release(),
                    emu_flavor(),
                    ?COPYRIGHT_MESSAGE, product_license_line()],
                   #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})
        end,
        log_motd(),
        {ok, SupPid} = rabbit_sup:start_link(),

        %% When we load plugins later in this function, we refresh feature
        %% flags. If `feature_flags_v2' is enabled, `rabbit_ff_controller'
        %% will be used. We start it now because we can't wait for boot steps
        %% to do this (feature flags are refreshed before boot steps run).
        ok = rabbit_sup:start_child(rabbit_ff_controller),

        %% Compatibility with older RabbitMQ versions + required by
        %% rabbit_node_monitor:notify_node_up/0:
        %%
        %% We register the app process under the name `rabbit`. This is
        %% checked by `is_running(Node)` on a remote node. The process
        %% is also monitord by rabbit_node_monitor.
        %%
        %% The process name must be registered *before* running the boot
        %% steps: that's when rabbit_node_monitor will set the process
        %% monitor up.
        %%
        %% Note that plugins were not taken care of at this point
        %% either.
        ?LOG_DEBUG(
          "Register `rabbit` process (~tp) for rabbit_node_monitor",
          [self()]),
        true = register(rabbit, self()),

        print_banner(),
        log_banner(),
        warn_if_kernel_config_dubious(),

        ?LOG_DEBUG(""),
        ?LOG_DEBUG("== Plugins (prelaunch phase) =="),

        ?LOG_DEBUG("Setting plugins up"),
        %% `Plugins` contains all the enabled plugins, plus their
        %% dependencies. The order is important: dependencies appear
        %% before plugin which depend on them.
        Plugins = rabbit_plugins:setup(),
        ?LOG_DEBUG(
          "Loading the following plugins: ~tp", [Plugins]),
        %% We can load all plugins and refresh their feature flags at
        %% once, because it does not involve running code from the
        %% plugins.
        ok = app_utils:load_applications(Plugins),
        case rabbit_feature_flags:refresh_feature_flags_after_app_load() of
            ok     -> ok;
            Error1 -> throw(Error1)
        end,

        persist_static_configuration(),

        ?LOG_DEBUG(""),
        ?LOG_DEBUG("== Boot steps =="),

        ok = rabbit_boot_steps:run_boot_steps([rabbit | Plugins]),
        rabbit_boot_state:set(core_started),
        run_postlaunch_phase(Plugins),
        {ok, SupPid}
    catch
        throw:{error, _} = Error ->
            _ = mnesia:stop(),
            rabbit_prelaunch_errors:log_error(Error),
            rabbit_prelaunch:set_stop_reason(Error),
            rabbit_boot_state:set(stopped),
            Error;
        Class:Exception:Stacktrace ->
            _ = mnesia:stop(),
            rabbit_prelaunch_errors:log_exception(
              Class, Exception, Stacktrace),
            Error = {error, Exception},
            rabbit_prelaunch:set_stop_reason(Error),
            rabbit_boot_state:set(stopped),
            Error
    end.

run_postlaunch_phase(Plugins) ->
    spawn(fun() -> do_run_postlaunch_phase(Plugins) end).

do_run_postlaunch_phase(Plugins) ->
    %% Once RabbitMQ itself is started, we need to run a few more steps,
    %% in particular start plugins.
    ?LOG_DEBUG(""),
    ?LOG_DEBUG("== Postlaunch phase =="),

    try
        %% Successful boot resets node maintenance state.
        ?LOG_DEBUG(""),
        ?LOG_INFO("Resetting node maintenance status"),
        _ = rabbit_maintenance:unmark_as_being_drained(),

        ?LOG_DEBUG(""),
        ?LOG_DEBUG("== Plugins (postlaunch phase) =="),

        %% Before loading plugins, set the prometheus collectors and
        %% instrumenters to the empty list. By default, prometheus will attempt
        %% to find all implementers of its collector and instrumenter
        %% behaviours by scanning all available modules during application
        %% start. This can take significant time (on the order of seconds) due
        %% to the large number of modules available.
        %%
        %% * Collectors: the `rabbitmq_prometheus' plugin explicitly registers
        %%   all collectors.
        %% * Instrumenters: no instrumenters are used.
        _ = application:load(prometheus),
        ok = application:set_env(prometheus, collectors, [default]),
        ok = application:set_env(prometheus, instrumenters, []),

        %% However, we want to run their boot steps and actually start
        %% them one by one, to ensure a dependency is fully started
        %% before a plugin which depends on it gets a chance to start.
        ?LOG_DEBUG("Starting the following plugins: ~tp", [Plugins]),
        lists:foreach(
          fun(Plugin) ->
                  case application:ensure_all_started(Plugin) of
                      {ok, _} -> ok;
                      Error   -> throw(Error)
                  end
          end, Plugins),

        %% Export definitions after all plugins have been enabled,
        %% see rabbitmq/rabbitmq-server#2384.
        case rabbit_definitions:maybe_load_definitions() of
            ok           -> ok;
            DefLoadError -> throw(DefLoadError)
        end,

        %% Start listeners after all plugins have been enabled,
        %% see rabbitmq/rabbitmq-server#2405.
        ?LOG_INFO("Ready to start client connection listeners"),
        ok = rabbit_networking:boot(),

        %% The node is ready: mark it as such and log it.
        %% NOTE: PLEASE DO NOT ADD CRITICAL NODE STARTUP CODE AFTER THIS.
        ActivePlugins = rabbit_plugins:active(),
        StrictlyPlugins = rabbit_plugins:strictly_plugins(ActivePlugins),
        ok = log_broker_started(StrictlyPlugins),

        ?LOG_DEBUG("Marking ~ts as running", [product_name()]),
        rabbit_boot_state:set(ready),

        %% Now that everything is ready, trigger the garbage collector. With
        %% Khepri enabled, it seems to be more important than before; see #5515
        %% for context.
        _ = rabbit_runtime:gc_all_processes(),
        ok
    catch
        throw:{error, _} = Error ->
            rabbit_prelaunch_errors:log_error(Error),
            rabbit_prelaunch:set_stop_reason(Error),
            do_stop();
        Class:Exception:Stacktrace ->
            rabbit_prelaunch_errors:log_exception(
              Class, Exception, Stacktrace),
            Error = {error, Exception},
            rabbit_prelaunch:set_stop_reason(Error),
            do_stop()
    end.

prep_stop(State) ->
    rabbit_boot_state:set(stopping),
    %% Wait for any in-flight feature flag changes to finish. This way, we
    %% increase the chance of letting an `enable' operation to finish if the
    %% controller waits for anything in-flight before is actually exits.
    ok = rabbit_ff_controller:wait_for_task_and_stop(),
    rabbit_peer_discovery:maybe_unregister(),
    State.

-spec stop(_) -> 'ok'.

stop(State) ->
    ok = rabbit_alarm:stop(),
    ok = rabbit_table:maybe_clear_ram_only_tables(),
    case State of
        [] -> rabbit_prelaunch:set_stop_reason(normal);
        _  -> rabbit_prelaunch:set_stop_reason(State)
    end,
    rabbit_db:clear_init_finished(),
    rabbit_boot_state:set(stopped),
    ok.

%%---------------------------------------------------------------------------
%% boot step functions

-spec boot_delegate() -> 'ok'.

boot_delegate() ->
    {ok, Count} = application:get_env(rabbit, delegate_count),
    rabbit_sup:start_supervisor_child(delegate_sup, [Count]).

-spec recover() -> 'ok'.

recover() ->
    ok = rabbit_vhost:recover().

pg_local_amqp_session() ->
    PgScope = pg_local_scope(amqp_session),
    rabbit_sup:start_child(pg_amqp_session, pg, [PgScope]).

pg_local_amqp_connection() ->
    PgScope = pg_local_scope(amqp_connection),
    rabbit_sup:start_child(pg_amqp_connection, pg, [PgScope]).

pg_local_scope(Prefix) ->
    list_to_atom(io_lib:format("~s_~s", [Prefix, node()])).


-spec update_cluster_tags() -> 'ok'.

update_cluster_tags() ->
    Tags = application:get_env(rabbit, cluster_tags, []),
    ?LOG_DEBUG("Seeding cluster tags from application environment key...",
                       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    rabbit_runtime_parameters:set_global(cluster_tags, Tags, <<"internal_user">>).


-spec prevent_startup_if_node_was_reset() -> 'ok' | no_return().

prevent_startup_if_node_was_reset() ->
    case application:get_env(rabbit, prevent_startup_if_node_was_reset, false) of
        false ->
            %% Feature is disabled, skip the check
            ?LOG_DEBUG("prevent_startup_if_node_was_reset is disabled",
                       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        true ->
            %% Feature is enabled, perform the check
            DataDir = data_dir(),
            MarkerFile = filename:join(DataDir, "node_initialized.marker"),
            case filelib:is_file(MarkerFile) of
                true ->
                    %% Not the first run, check if tables need default data
                    case rabbit_table:needs_default_data() of
                        true ->
                            ?LOG_ERROR("Node has already been initialized, but database appears empty. "
                                       "This could indicate data loss or a split-brain scenario.",
                                      #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
                            throw({error, cluster_already_initialized_but_tables_empty});
                        false ->
                            ?LOG_INFO("Node has already been initialized, proceeding with normal startup",
                                      #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
                            ok
                    end;
                false ->
                    %% First time starting, create the marker file
                    ?LOG_INFO("First node startup detected, creating initialization marker",
                             #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
                    ok = filelib:ensure_dir(MarkerFile),
                    ok = file:write_file(MarkerFile, <<>>, [exclusive]), % Empty file.
                    ok
            end
    end.

-spec maybe_insert_default_data() -> 'ok'.

maybe_insert_default_data() ->
    NoDefsToImport = not rabbit_definitions:has_configured_definitions_to_load(),
    case rabbit_table:needs_default_data() andalso NoDefsToImport of
        true  ->
            ?LOG_INFO("Will seed default virtual host and user...",
                      #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            insert_default_data();
        false ->
            ?LOG_INFO("Will not seed default virtual host and user: have definitions to load...",
                      #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok
    end.

insert_default_data() ->
    DefaultUser = get_default_data_param(default_user),
    DefaultPass = get_default_data_param(default_pass),
    {ok, DefaultTags} = application:get_env(default_user_tags),
    DefaultVHost = get_default_data_param(default_vhost),
    {ok, [DefaultConfigurePerm, DefaultWritePerm, DefaultReadPerm]} =
        application:get_env(default_permissions),

    DefaultUserBin = rabbit_data_coercion:to_binary(DefaultUser),
    DefaultPassBin = rabbit_data_coercion:to_binary(DefaultPass),
    DefaultVHostBin = rabbit_data_coercion:to_binary(DefaultVHost),
    DefaultConfigurePermBin = rabbit_data_coercion:to_binary(DefaultConfigurePerm),
    DefaultWritePermBin = rabbit_data_coercion:to_binary(DefaultWritePerm),
    DefaultReadPermBin = rabbit_data_coercion:to_binary(DefaultReadPerm),

    ok = rabbit_vhost:add(DefaultVHostBin, <<"Default virtual host">>, [], ?INTERNAL_USER),
    ok = rabbit_auth_backend_internal:add_user(
        DefaultUserBin,
        DefaultPassBin,
        ?INTERNAL_USER
    ),
    ok = rabbit_auth_backend_internal:set_tags(DefaultUserBin, DefaultTags,
                                               ?INTERNAL_USER),
    ok = rabbit_auth_backend_internal:set_permissions(DefaultUserBin,
                                                      DefaultVHostBin,
                                                      DefaultConfigurePermBin,
                                                      DefaultWritePermBin,
                                                      DefaultReadPermBin,
                                                      ?INTERNAL_USER),
    ok.

get_default_data_param(Param) ->
    #{var_origins := Origins} = Context = rabbit_prelaunch:get_context(),
    case maps:get(Param, Origins, default) of
        environment ->
            Value = maps:get(Param, Context),
            ?assert(is_binary(Value)),
            Value;
        default ->
            {ok, Value} = application:get_env(Param),
            Value
    end.

%%---------------------------------------------------------------------------
%% Data directory.

-spec data_dir() -> DataDir when
      DataDir :: file:filename().
%% @doc Returns the data directory.
%%
%% This directory is used by many subsystems to store their files, either
%% directly underneath or in subdirectories.
%%
%% Here are a few examples:
%% <ul>
%% <li>Mnesia</li>
%% <li>Feature flags state</li>
%% <li>Ra systems's WAL and segment files</li>
%% <li>Classic queues' messages</li>
%% </ul>

data_dir() ->
    {ok, DataDir} = application:get_env(rabbit, data_dir),
    DataDir.

%%---------------------------------------------------------------------------
%% logging

-spec set_log_level(logger:level()) -> ok.
set_log_level(Level) ->
    rabbit_prelaunch_logging:set_log_level(Level).

-spec log_locations() -> [rabbit_prelaunch_logging:log_location()].
log_locations() ->
    rabbit_prelaunch_logging:log_locations().

-spec config_locations() -> [rabbit_config:config_location()].
config_locations() ->
    rabbit_config:config_files().

-spec force_event_refresh(reference()) -> 'ok'.

% Note: https://www.pivotaltracker.com/story/show/166962656
% This event is necessary for the stats timer to be initialized with
% the correct values once the management agent has started
force_event_refresh(Ref) ->
    % direct connections, e.g. STOMP
    ok = rabbit_direct:force_event_refresh(Ref),
    % AMQP connections
    ok = rabbit_networking:force_connection_event_refresh(Ref),
    % non-AMQP connections, which are not handled by the "AMQP core",
    % e.g. connections to the stream and MQTT plugins
    ok = rabbit_networking:force_non_amqp_connection_event_refresh(Ref),
    ok = rabbit_channel:force_event_refresh(Ref),
    ok = rabbit_amqqueue:force_event_refresh(Ref).

%%---------------------------------------------------------------------------
%% misc

log_broker_started(Plugins) ->
    PluginList = iolist_to_binary([rabbit_misc:format(" * ~ts~n", [P])
                                   || P <- Plugins]),
    Message = string:strip(rabbit_misc:format(
        "Server startup complete; ~b plugins started.~n~ts",
        [length(Plugins), PluginList]), right, $\n),
    ?LOG_INFO(Message,
              #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    io:format(" completed with ~tp plugins.~n", [length(Plugins)]).

-define(RABBIT_TEXT_LOGO,
        "~n  ##  ##      ~ts ~ts"
        "~n  ##  ##"
        "~n  ##########  ~ts"
        "~n  ######  ##"
        "~n  ##########  ~ts").
-define(FG8_START,  "\033[38;5;202m").
-define(BG8_START,  "\033[48;5;202m").
-define(FG32_START, "\033[38;2;255;102;0m").
-define(BG32_START, "\033[48;2;255;102;0m").
-define(C_END,      "\033[0m").
-define(RABBIT_8BITCOLOR_LOGO,
        "~n  " ?BG8_START "  " ?C_END "  " ?BG8_START "  " ?C_END "      \033[1m" ?FG8_START "~ts" ?C_END " ~ts"
        "~n  " ?BG8_START "  " ?C_END "  " ?BG8_START "  " ?C_END
        "~n  " ?BG8_START "          " ?C_END "  ~ts"
        "~n  " ?BG8_START "      " ?C_END "  " ?BG8_START "  " ?C_END
        "~n  " ?BG8_START "          " ?C_END "  ~ts").
-define(RABBIT_32BITCOLOR_LOGO,
        "~n  " ?BG32_START "  " ?C_END "  " ?BG32_START "  " ?C_END "      \033[1m" ?FG32_START "~ts" ?C_END " ~ts"
        "~n  " ?BG32_START "  " ?C_END "  " ?BG32_START "  " ?C_END
        "~n  " ?BG32_START "          " ?C_END "  ~ts"
        "~n  " ?BG32_START "      " ?C_END "  " ?BG32_START "  " ?C_END
        "~n  " ?BG32_START "          " ?C_END "  ~ts").

print_banner() ->
    Product = product_name(),
    Version = product_version(),
    LineListFormatter = fun (Placeholder, [_ | Tail] = LL) ->
                              LF = lists:flatten([Placeholder || _ <- lists:seq(1, length(Tail))]),
                              {LF, LL};
                            (_, []) ->
                              {"", ["(none)"]}
    end,
    Logo = case rabbit_prelaunch:get_context() of
               %% We use the colored logo only when running the
               %% interactive shell and when colors are supported.
               %%
               %% Basically it means it will be used on Unix when
               %% running "make run-broker" and that's about it.
               #{os_type := {unix, darwin},
                 interactive_shell := true,
                 output_supports_colors := true} -> ?RABBIT_8BITCOLOR_LOGO;
               #{interactive_shell := true,
                 output_supports_colors := true} -> ?RABBIT_32BITCOLOR_LOGO;
               _                                 -> ?RABBIT_TEXT_LOGO
           end,
    %% padded list lines
    {LogFmt, LogLocations} = LineListFormatter("~n        ~ts", log_locations()),
    {CfgFmt, CfgLocations} = LineListFormatter("~n                  ~ts", config_locations()),
    {MOTDFormat, MOTDArgs} = case motd() of
                                 undefined ->
                                     {"", []};
                                 MOTD ->
                                     Lines = string:split(MOTD, "\n", all),
                                     Padded = [case Line of
                                                   <<>> -> "\n";
                                                   _    -> ["  ", Line, "\n"]
                                               end
                                               || Line <- Lines],
                                     {"~n~ts", [Padded]}
                             end,
    io:format(Logo ++
              "~n" ++
              MOTDFormat ++
              "~n  Erlang:      ~ts [~ts]"
              "~n  TLS Library: ~ts"
              "~n  Release series support status: see https://www.rabbitmq.com/release-information"
              "~n"
              "~n  Doc guides:  https://www.rabbitmq.com/docs"
              "~n  Support:     https://www.rabbitmq.com/docs/contact"
              "~n  Tutorials:   https://www.rabbitmq.com/tutorials"
              "~n  Monitoring:  https://www.rabbitmq.com/docs/monitoring"
              "~n  Upgrading:   https://www.rabbitmq.com/docs/upgrade"
              "~n"
              "~n  Logs: ~ts" ++ LogFmt ++ "~n"
              "~n  Config file(s): ~ts" ++ CfgFmt ++ "~n"
              "~n  Starting broker...",
              [Product, Version, ?COPYRIGHT_MESSAGE, product_license_line()] ++
              [rabbit_misc:otp_release(), emu_flavor(), crypto_version()] ++
              MOTDArgs ++
              LogLocations ++
              CfgLocations).

emu_flavor() ->
    %% emu_flavor was introduced in Erlang 24 so we need to catch the error on Erlang 23
    case catch(erlang:system_info(emu_flavor)) of
        {'EXIT', _} -> "emu";
        EmuFlavor -> EmuFlavor
    end.

crypto_version() ->
    [{CryptoLibName, _, CryptoLibVersion}] = crypto:info_lib(),
    [CryptoLibName, " - ", CryptoLibVersion].

log_motd() ->
    case motd() of
        undefined ->
            ok;
        MOTD ->
            Lines = string:split(MOTD, "\n", all),
            Padded = [case Line of
                          <<>> -> "\n";
                          _    -> [" ", Line, "\n"]
                      end
                      || Line <- Lines],
            ?LOG_INFO("~n~ts", [string:trim(Padded, trailing, [$\r, $\n])],
                      #{domain => ?RMQLOG_DOMAIN_GLOBAL})
    end.

log_banner() ->
    {FirstLog, OtherLogs} = case log_locations() of
        [Head | Tail] ->
            {Head, [{"", F} || F <- Tail]};
        [] ->
            {"(none)", []}
    end,
    Settings = [{"node",           node()},
                {"home dir",       home_dir()},
                {"config file(s)", config_files()},
                {"cookie hash",    rabbit_nodes:cookie_hash()},
                {"log(s)",         FirstLog}] ++
               OtherLogs ++
               [{"data dir",       data_dir()}],
    DescrLen = 1 + lists:max([length(K) || {K, _V} <- Settings]),
    Format = fun (K, V) ->
                     rabbit_misc:format(
                       " ~-" ++ integer_to_list(DescrLen) ++ "s: ~ts~n", [K, V])
             end,
    Banner = string:strip(lists:flatten(
               [case S of
                    {"config file(s)" = K, []} ->
                        Format(K, "(none)");
                    {"config file(s)" = K, [V0 | Vs]} ->
                        [Format(K, V0) | [Format("", V) || V <- Vs]];
                    {K, V} ->
                        Format(K, V)
                end || S <- Settings]), right, $\n),
    ?LOG_INFO("~n~ts", [Banner],
              #{domain => ?RMQLOG_DOMAIN_GLOBAL}).

warn_if_kernel_config_dubious() ->
    case os:type() of
        {win32, _} ->
            ok;
        _ ->
            case erlang:system_info(kernel_poll) of
                true  -> ok;
                false -> ?LOG_WARNING(
                           "Kernel poll (epoll, kqueue, etc) is disabled. "
                           "Throughput and CPU utilization may worsen.",
                           #{domain => ?RMQLOG_DOMAIN_GLOBAL})
            end
    end,
    IDCOpts = case application:get_env(kernel, inet_default_connect_options) of
                  undefined -> [];
                  {ok, Val} -> Val
              end,
    case proplists:get_value(nodelay, IDCOpts, false) of
        false -> ?LOG_WARNING("Nagle's algorithm is enabled for sockets, "
                              "network I/O latency will be higher",
                              #{domain => ?RMQLOG_DOMAIN_GLOBAL});
        true  -> ok
    end.

-spec product_name() -> string().

product_name() ->
    case product_info() of
        #{product_name := ProductName}   -> ProductName;
        #{product_base_name := BaseName} -> BaseName
    end.

-spec product_license_line() -> string().
product_license_line() ->
    application:get_env(rabbit, license_line, ?INFORMATION_MESSAGE).

-spec product_version() -> string().

product_version() ->
    case product_info() of
        #{product_version := ProductVersion}   -> ProductVersion;
        #{product_base_version := BaseVersion} -> BaseVersion
    end.

-spec product_info() -> #{product_base_name := string(),
                          product_base_version := string(),
                          product_overridden := boolean(),
                          product_name => string(),
                          product_version => string(),
                          otp_release := string()}.

product_info() ->
    PTKey = {?MODULE, product},
    try
        %% The value is cached the first time to avoid calling the
        %% application master many times just for that.
        persistent_term:get(PTKey)
    catch
        error:badarg ->
            BaseName = base_product_name(),
            BaseVersion = base_product_version(),
            Info0 = #{product_base_name => BaseName,
                      product_base_version => BaseVersion,
                      otp_release => rabbit_misc:otp_release()},

            {NameFromEnv, VersionFromEnv} =
            case rabbit_prelaunch:get_context() of
                #{product_name := NFE,
                  product_version := VFE} -> {NFE, VFE};
                _                         -> {undefined, undefined}
            end,

            Info1 = case NameFromEnv of
                        undefined ->
                            NameFromApp = string_from_app_env(
                                            product_name,
                                            undefined),
                            case NameFromApp of
                                undefined ->
                                    Info0;
                                _ ->
                                    Info0#{product_name => NameFromApp,
                                           product_overridden => true}
                            end;
                        _ ->
                            Info0#{product_name => NameFromEnv,
                                   product_overridden => true}
                    end,

            Info2 = case VersionFromEnv of
                        undefined ->
                            VersionFromApp = string_from_app_env(
                                               product_version,
                                               undefined),
                            case VersionFromApp of
                                undefined ->
                                    Info1;
                                _ ->
                                    Info1#{product_version => VersionFromApp,
                                           product_overridden => true}
                            end;
                        _ ->
                            Info1#{product_version => VersionFromEnv,
                                   product_overridden => true}
                    end,
            persistent_term:put(PTKey, Info2),
            Info2
    end.

string_from_app_env(Key, Default) ->
    case application:get_env(rabbit, Key) of
        {ok, Val} ->
            case io_lib:deep_char_list(Val) of
                true ->
                    case lists:flatten(Val) of
                        ""     -> Default;
                        String -> String
                    end;
                false ->
                    Default
            end;
        undefined ->
            Default
    end.

base_product_name() ->
    %% This function assumes the `rabbit` application was loaded in
    %% product_info().
    {ok, Product} = application:get_key(rabbit, description),
    Product.

base_product_version() ->
    %% This function assumes the `rabbit` application was loaded in
    %% product_info().
    rabbit_misc:version().

motd_file() ->
    %% Precendence is:
    %%   1. The environment variable;
    %%   2. The `motd_file` configuration parameter;
    %%   3. The default value.
    Context = rabbit_prelaunch:get_context(),
    case Context of
        #{motd_file := File,
          var_origins := #{motd_file := environment}}
          when File =/= undefined ->
            File;
        _ ->
            Default = case Context of
                          #{motd_file := File} -> File;
                          _                    -> undefined
                      end,
            string_from_app_env(motd_file, Default)
    end.

motd() ->
    case motd_file() of
        undefined ->
            undefined;
        File ->
            case rabbit_misc:raw_read_file(File) of
                {ok, MOTD} -> string:trim(MOTD, trailing, [$\r,$\n]);
                {error, _} -> undefined
            end
    end.

home_dir() ->
    case init:get_argument(home) of
        {ok, [[Home]]} -> filename:absname(Home);
        Other          -> Other
    end.

config_files() ->
    rabbit_config:config_files().

%% We don't want this in fhc since it references rabbit stuff. And we can't put
%% this in the bootstep directly.
start_fhc() ->
    ok = rabbit_sup:start_restartable_child(
      file_handle_cache,
      [fun(_) -> ok end, fun(_) -> ok end]),
    ensure_working_fhc(),
    maybe_warn_low_fd_limit().

ensure_working_fhc() ->
    %% To test the file handle cache, we simply read a file we know it
    %% exists (Erlang kernel's .app file).
    %%
    %% To avoid any pollution of the application process' dictionary by
    %% file_handle_cache, we spawn a separate process.
    Parent = self(),
    TestFun = fun() ->
        ReadBuf = case application:get_env(rabbit, fhc_read_buffering) of
            {ok, true}  -> "ON";
            {ok, false} -> "OFF"
        end,
        WriteBuf = case application:get_env(rabbit, fhc_write_buffering) of
            {ok, true}  -> "ON";
            {ok, false} -> "OFF"
        end,
        ?LOG_INFO("FHC read buffering: ~ts", [ReadBuf],
                  #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
        ?LOG_INFO("FHC write buffering: ~ts", [WriteBuf],
                  #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
        Filename = filename:join(code:lib_dir(kernel), "ebin/kernel.app"),
        {ok, Fd} = file_handle_cache:open(Filename, [raw, binary, read], []),
        {ok, _} = file_handle_cache:read(Fd, 1),
        ok = file_handle_cache:close(Fd),
        Parent ! fhc_ok
    end,
    TestPid = spawn_link(TestFun),
    %% Because we are waiting for the test fun, abuse the
    %% 'mnesia_table_loading_retry_timeout' parameter to find a sane timeout
    %% value.
    Timeout = rabbit_table:retry_timeout(),
    receive
        fhc_ok                       -> ok;
        {'EXIT', TestPid, Exception} -> throw({ensure_working_fhc, Exception})
    after Timeout ->
            throw({ensure_working_fhc, {timeout, TestPid}})
    end.

maybe_warn_low_fd_limit() ->
    case file_handle_cache:ulimit() of
        %% unknown is included as atom() > integer().
        L when L > 1024 ->
            ok;
        L ->
            rabbit_log:warning("Available file handles: ~tp. "
                "Please consider increasing system limits", [L])
    end.

%% Any configuration that
%% 1. is not allowed to change while RabbitMQ is running, and
%% 2. is read often
%% should be placed into persistent_term for efficiency.
persist_static_configuration() ->
    persist_static_configuration(
      [classic_queue_index_v2_segment_entry_count,
       classic_queue_store_v2_max_cache_size,
       classic_queue_store_v2_check_crc32
      ]),

    Interceptors = application:get_env(?MODULE, message_interceptors, []),
    ok = rabbit_msg_interceptor:add(Interceptors),

    %% Disallow the following two cases:
    %% 1. Negative values
    %% 2. MoreCreditAfter greater than InitialCredit
    CreditFlowDefaultCredit = case application:get_env(?MODULE, credit_flow_default_credit) of
                     {ok, {InitialCredit, MoreCreditAfter}}
                       when is_integer(InitialCredit) andalso
                            is_integer(MoreCreditAfter) andalso
                            InitialCredit >= 0 andalso
                            MoreCreditAfter >= 0 andalso
                            MoreCreditAfter =< InitialCredit ->
                         {InitialCredit, MoreCreditAfter};
                     Other ->
                       rabbit_log:error("Refusing to boot due to an invalid value of 'rabbit.credit_flow_default_credit'"),
                       throw({error, {invalid_credit_flow_default_credit_value, Other}})
               end,
    ok = persistent_term:put(credit_flow_default_credit, CreditFlowDefaultCredit),

    %% Disallow 0 as it means unlimited:
    %% "If this field is zero or unset, there is no maximum
    %% size imposed by the link endpoint." [AMQP 1.0 §2.7.3]
    MaxMsgSize = case application:get_env(?MODULE, max_message_size) of
                     {ok, Size}
                       when is_integer(Size) andalso Size > 0 ->
                         erlang:min(Size, ?MAX_MSG_SIZE);
                     _ ->
                         ?MAX_MSG_SIZE
                 end,
    ok = persistent_term:put(max_message_size, MaxMsgSize).

persist_static_configuration(Params) ->
    lists:foreach(
      fun(Param) ->
              case application:get_env(?MODULE, Param) of
                  {ok, Value} ->
                      ok = persistent_term:put(Param, Value);
                  undefined ->
                      ok
              end
      end, Params).
