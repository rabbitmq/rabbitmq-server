%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit).

-ignore_xref({rabbit_direct, force_event_refresh, 1}).
-ignore_xref({rabbit_networking, force_connection_event_refresh, 1}).
%% Transitional step until we can require Erlang/OTP 21 and
%% use the now recommended try/catch syntax for obtaining the stack trace.
-compile(nowarn_deprecated_function).

-behaviour(application).

-export([start/0, boot/0, stop/0,
         stop_and_halt/0, await_startup/0, await_startup/1, await_startup/3,
         status/0, is_running/0, alarms/0,
         is_running/1, environment/0, rotate_logs/0, force_event_refresh/1,
         start_fhc/0]).

-export([start/2, stop/1, prep_stop/1]).
-export([start_apps/1, start_apps/2, stop_apps/1]).
-export([product_info/0,
         product_name/0,
         product_version/0,
         base_product_name/0,
         base_product_version/0,
         motd_file/0,
         motd/0]).
-export([log_locations/0, config_files/0]). %% for testing and mgmt-agent
-export([is_booted/1, is_booted/0, is_booting/1, is_booting/0]).

%%---------------------------------------------------------------------------
%% Boot steps.
-export([maybe_insert_default_data/0, boot_delegate/0, recover/0]).

%% for tests
-export([validate_msg_store_io_batch_size_and_credit_disc_bound/2]).

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

-rabbit_boot_step({database,
                   [{mfa,         {rabbit_mnesia, init, []}},
                    {requires,    file_handle_cache},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step({database_sync,
                   [{description, "database sync"},
                    {mfa,         {rabbit_sup, start_child, [mnesia_sync]}},
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

-rabbit_boot_step({rabbit_registry,
                   [{description, "plugin registry"},
                    {mfa,         {rabbit_sup, start_child,
                                   [rabbit_registry]}},
                    {requires,    external_infrastructure},
                    {enables,     kernel_ready}]}).

-rabbit_boot_step({rabbit_core_metrics,
                   [{description, "core metrics storage"},
                    {mfa,         {rabbit_sup, start_child,
                                   [rabbit_metrics]}},
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

-rabbit_boot_step({rabbit_memory_monitor,
                   [{description, "memory monitor"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_memory_monitor]}},
                    {requires,    rabbit_alarm},
                    {enables,     core_initialized}]}).

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

-rabbit_boot_step({upgrade_queues,
                   [{description, "per-vhost message store migration"},
                    {mfa,         {rabbit_upgrade,
                                   maybe_migrate_queues_to_per_vhost_storage,
                                   []}},
                    {requires,    [core_initialized]},
                    {enables,     recovery}]}).

-rabbit_boot_step({recovery,
                   [{description, "exchange, queue and binding recovery"},
                    {mfa,         {rabbit, recover, []}},
                    {requires,    [core_initialized]},
                    {enables,     routing_ready}]}).

-rabbit_boot_step({empty_db_check,
                   [{description, "empty DB check"},
                    {mfa,         {?MODULE, maybe_insert_default_data, []}},
                    {requires,    recovery},
                    {enables,     routing_ready}]}).

-rabbit_boot_step({routing_ready,
                   [{description, "message delivery logic ready"},
                    {requires,    [core_initialized, recovery]}]}).

-rabbit_boot_step({connection_tracking,
                   [{description, "connection tracking infrastructure"},
                    {mfa,         {rabbit_connection_tracking, boot, []}},
                    {enables,     routing_ready}]}).

-rabbit_boot_step({channel_tracking,
                   [{description, "channel tracking infrastructure"},
                    {mfa,         {rabbit_channel_tracking, boot, []}},
                    {enables,     routing_ready}]}).

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

-rabbit_boot_step({rabbit_looking_glass,
                   [{description, "Looking Glass tracer and profiler"},
                    {mfa,         {rabbit_looking_glass, boot, []}},
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
                    {mfa,         {rabbit_log, debug, ["'networking' boot step skipped and moved to end of startup", []]}},
                    {requires,    notify_cluster}]}).

%%---------------------------------------------------------------------------

-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-define(APPS, [os_mon, mnesia, rabbit_common, rabbitmq_prelaunch, ra, sysmon_handler, rabbit]).

-define(DIRTY_IO_SCHEDULERS_WARNING_THRESHOLD, 10).

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
            _ = rabbit_log_prelaunch:debug(""),
            _ = rabbit_log_prelaunch:debug(
              "== Prelaunch phase [2/2] (initial pass) ==");
        false ->
            _ = rabbit_log_prelaunch:debug(""),
            _ = rabbit_log_prelaunch:debug("== Prelaunch phase [2/2] =="),
            ok
    end,

    %% 1. Enabled plugins file.
    ok = rabbit_prelaunch_enabled_plugins_file:setup(Context),

    %% 2. Feature flags registry.
    ok = rabbit_prelaunch_feature_flags:setup(Context),

    %% 3. Logging.
    ok = rabbit_prelaunch_logging:setup(Context),

    %% 4. Clustering.
    ok = rabbit_prelaunch_cluster:setup(Context),

    %% Start Mnesia now that everything is ready.
    _ = rabbit_log_prelaunch:debug("Starting Mnesia"),
    ok = mnesia:start(),

    _ = rabbit_log_prelaunch:debug(""),
    _ = rabbit_log_prelaunch:debug("== Prelaunch DONE =="),

    case IsInitialPass of
        true  -> rabbit_prelaunch:initial_pass_finished();
        false -> ok
    end,
    ok.

start_it(StartType) ->
    case spawn_boot_marker() of
        {ok, Marker} ->
            T0 = erlang:timestamp(),
            _ = rabbit_log:info("RabbitMQ is asked to start...", []),
            try
                {ok, _} = application:ensure_all_started(rabbitmq_prelaunch,
                                                         StartType),
                {ok, _} = application:ensure_all_started(rabbit,
                                                         StartType),
                ok = wait_for_ready_or_stopped(),

                T1 = erlang:timestamp(),
                _ = rabbit_log_prelaunch:debug(
                  "Time to start RabbitMQ: ~p µs",
                  [timer:now_diff(T1, T0)]),
                stop_boot_marker(Marker),
                ok
            catch
                error:{badmatch, Error}:_ ->
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
                    _ = rabbit_log:info("~s is asked to stop...", [Product]),
                    do_stop(),
                    _ = rabbit_log:info(
                      "Successfully stopped ~s and its dependencies",
                      [Product]),
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
        _ = rabbit_log:error(
          "Error trying to stop ~s: ~p:~p",
          [product_name(), Type, Reason]),
        error({Type, Reason})
    after
        %% Enclose all the logging in the try block.
        %% init:stop() will be called regardless of any errors.
        try
            AppsLeft = [ A || {A, _, _} <- application:which_applications() ],
            _ = rabbit_log:info(
                lists:flatten(["Halting Erlang VM with the following applications:~n",
                               ["    ~p~n" || _ <- AppsLeft]]),
                AppsLeft),
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
%% should be moved over `rabbit_plugins`, along with stop_apps/1, once
%% the latter stops using app_utils as well.

start_apps(Apps, RestartTypes) ->
    false = lists:member(rabbit, Apps), %% Assertion.
    %% We need to load all applications involved in order to be able to
    %% find new feature flags.
    app_utils:load_applications(Apps),
    ok = rabbit_feature_flags:refresh_feature_flags_after_app_load(Apps),
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
    _ = rabbit_log:info(
        lists:flatten(["Stopping ~s applications and their dependencies in the following order:~n",
                       ["    ~p~n" || _ <- Apps]]),
        [product_name() | lists:reverse(Apps)]),
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
                false -> wait_for_boot_to_start(Node),
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
                false -> wait_for_boot_to_start(Node, Timeout),
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
    S1 = [{pid,                  list_to_integer(os:getpid())},
          %% The timeout value used is twice that of gen_server:call/2.
          {running_applications, rabbit_misc:which_applications()},
          {os,                   os:type()},
          {rabbitmq_version,     Version},
          {erlang_version,       erlang:system_info(system_version)},
          {memory,               rabbit_vm:memory()},
          {alarms,               alarms()},
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
           {data_directory, rabbit_mnesia:dir()},
           {raft_data_directory, ra_env:data_dir()}],
    Totals = case is_running() of
                 true ->
                     [{virtual_host_count, rabbit_vhost:count()},
                      {connection_count,
                       length(rabbit_networking:connections_local())},
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

-spec is_running() -> boolean().

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
    rabbit_lager:fold_sinks(
      fun
          (_, [], Acc) ->
              Acc;
          (SinkName, FileNames, Acc) ->
              lager:log(SinkName, info, self(),
                        "Log file rotation forced", []),
              %% FIXME: We use an internal message, understood by
              %% lager_file_backend. We should use a proper API, when
              %% it's added to Lager.
              %%
              %% FIXME: This call is effectively asynchronous: at the
              %% end of this function, we can't guaranty the rotation
              %% is completed.
              [ok = gen_event:call(SinkName,
                                   {lager_file_backend, FileName},
                                   rotate,
                                   infinity) || FileName <- FileNames],
              lager:log(SinkName, info, self(),
                        "Log file re-opened after forced rotation", []),
              Acc
      end, ok).

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
                _ = rabbit_log:info("~n Starting ~s ~s on Erlang ~s [~s]~n Based on ~s ~s~n ~s~n ~s~n",
                                [product_name(), product_version(), rabbit_misc:otp_release(),
                                 emu_flavor(),
                                 BaseName, BaseVersion,
                                 ?COPYRIGHT_MESSAGE, ?INFORMATION_MESSAGE]);
            _ ->
                _ = rabbit_log:info("~n Starting ~s ~s on Erlang ~s [~s]~n ~s~n ~s",
                                [product_name(), product_version(), rabbit_misc:otp_release(),
                                 emu_flavor(),
                                 ?COPYRIGHT_MESSAGE, ?INFORMATION_MESSAGE])
        end,
        log_motd(),
        {ok, SupPid} = rabbit_sup:start_link(),

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
        _ = rabbit_log_prelaunch:debug(
          "Register `rabbit` process (~p) for rabbit_node_monitor",
          [self()]),
        true = register(rabbit, self()),

        print_banner(),
        log_banner(),
        warn_if_kernel_config_dubious(),
        warn_if_disc_io_options_dubious(),

        _ = rabbit_log_prelaunch:debug(""),
        _ = rabbit_log_prelaunch:debug("== Plugins (prelaunch phase) =="),

        _ = rabbit_log_prelaunch:debug("Setting plugins up"),
        %% `Plugins` contains all the enabled plugins, plus their
        %% dependencies. The order is important: dependencies appear
        %% before plugin which depend on them.
        Plugins = rabbit_plugins:setup(),
        _ = rabbit_log_prelaunch:debug(
          "Loading the following plugins: ~p", [Plugins]),
        %% We can load all plugins and refresh their feature flags at
        %% once, because it does not involve running code from the
        %% plugins.
        ok = app_utils:load_applications(Plugins),
        ok = rabbit_feature_flags:refresh_feature_flags_after_app_load(
               Plugins),

        _ = rabbit_log_prelaunch:debug(""),
        _ = rabbit_log_prelaunch:debug("== Boot steps =="),

        ok = rabbit_boot_steps:run_boot_steps([rabbit | Plugins]),
        run_postlaunch_phase(Plugins),
        rabbit_boot_state:set(core_started),
        {ok, SupPid}
    catch
        throw:{error, _} = Error ->
            mnesia:stop(),
            rabbit_prelaunch_errors:log_error(Error),
            rabbit_prelaunch:set_stop_reason(Error),
            rabbit_boot_state:set(stopped),
            Error;
        Class:Exception:Stacktrace ->
            mnesia:stop(),
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
    _ = rabbit_log_prelaunch:debug(""),
    _ = rabbit_log_prelaunch:debug("== Postlaunch phase =="),

    try
        %% Successful boot resets node maintenance state.
        _ = rabbit_log_prelaunch:debug(""),
        _ = rabbit_log_prelaunch:info("Resetting node maintenance status"),
        _ = rabbit_maintenance:unmark_as_being_drained(),

        _ = rabbit_log_prelaunch:debug(""),
        _ = rabbit_log_prelaunch:debug("== Plugins (postlaunch phase) =="),

        %% However, we want to run their boot steps and actually start
        %% them one by one, to ensure a dependency is fully started
        %% before a plugin which depends on it gets a chance to start.
        _ = rabbit_log_prelaunch:debug(
          "Starting the following plugins: ~p", [Plugins]),
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
        _ = rabbit_log_prelaunch:info(
          "Ready to start client connection listeners"),
        ok = rabbit_networking:boot(),

        %% The node is ready: mark it as such and log it.
        %% NOTE: PLEASE DO NOT ADD CRITICAL NODE STARTUP CODE AFTER THIS.
        ok = rabbit_lager:broker_is_started(),
        ActivePlugins = rabbit_plugins:active(),
        StrictlyPlugins = rabbit_plugins:strictly_plugins(ActivePlugins),
        ok = log_broker_started(StrictlyPlugins),

        _ = rabbit_log_prelaunch:info("Resetting node maintenance status"),
        %% successful boot resets node maintenance state
        rabbit_maintenance:unmark_as_being_drained(),
        _ = rabbit_log_prelaunch:debug("Marking ~s as running", [product_name()]),
        rabbit_boot_state:set(ready)
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
    rabbit_peer_discovery:maybe_unregister(),
    State.

-spec stop(_) -> 'ok'.

stop(State) ->
    ok = rabbit_alarm:stop(),
    ok = case rabbit_mnesia:is_clustered() of
             true  -> ok;
             false -> rabbit_table:clear_ram_only_tables()
         end,
    case State of
        [] -> rabbit_prelaunch:set_stop_reason(normal);
        _  -> rabbit_prelaunch:set_stop_reason(State)
    end,
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
    ok = rabbit_policy:recover(),
    ok = rabbit_vhost:recover(),
    ok = lager_exchange_backend:maybe_init_exchange().

-spec maybe_insert_default_data() -> 'ok'.

maybe_insert_default_data() ->
    NoDefsToImport = not rabbit_definitions:has_configured_definitions_to_load(),
    case rabbit_table:needs_default_data() andalso NoDefsToImport of
        true  ->
            _ = rabbit_log:info("Will seed default virtual host and user..."),
            insert_default_data();
        false ->
            _ = rabbit_log:info("Will not seed default virtual host and user: have definitions to load..."),
            ok
    end.

insert_default_data() ->
    {ok, DefaultUser} = application:get_env(default_user),
    {ok, DefaultPass} = application:get_env(default_pass),
    {ok, DefaultTags} = application:get_env(default_user_tags),
    {ok, DefaultVHost} = application:get_env(default_vhost),
    {ok, [DefaultConfigurePerm, DefaultWritePerm, DefaultReadPerm]} =
        application:get_env(default_permissions),

    DefaultUserBin = rabbit_data_coercion:to_binary(DefaultUser),
    DefaultPassBin = rabbit_data_coercion:to_binary(DefaultPass),
    DefaultVHostBin = rabbit_data_coercion:to_binary(DefaultVHost),
    DefaultConfigurePermBin = rabbit_data_coercion:to_binary(DefaultConfigurePerm),
    DefaultWritePermBin = rabbit_data_coercion:to_binary(DefaultWritePerm),
    DefaultReadPermBin = rabbit_data_coercion:to_binary(DefaultReadPerm),

    ok = rabbit_vhost:add(DefaultVHostBin, <<"Default virtual host">>, [], ?INTERNAL_USER),
    ok = lager_exchange_backend:maybe_init_exchange(),
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

%%---------------------------------------------------------------------------
%% logging

-spec log_locations() -> [rabbit_lager:log_location()].
log_locations() ->
    rabbit_lager:log_locations().

-spec config_locations() -> [rabbit_config:config_location()].
config_locations() ->
    rabbit_config:config_files().

-spec force_event_refresh(reference()) -> 'ok'.

% Note: https://www.pivotaltracker.com/story/show/166962656
% This event is necessary for the stats timer to be initialized with
% the correct values once the management agent has started
force_event_refresh(Ref) ->
    ok = rabbit_direct:force_event_refresh(Ref),
    ok = rabbit_networking:force_connection_event_refresh(Ref),
    ok = rabbit_channel:force_event_refresh(Ref),
    ok = rabbit_amqqueue:force_event_refresh(Ref).

%%---------------------------------------------------------------------------
%% misc

log_broker_started(Plugins) ->
    PluginList = iolist_to_binary([rabbit_misc:format(" * ~s~n", [P])
                                   || P <- Plugins]),
    Message = string:strip(rabbit_misc:format(
        "Server startup complete; ~b plugins started.~n~s",
        [length(Plugins), PluginList]), right, $\n),
    _ = rabbit_log:info(Message),
    io:format(" completed with ~p plugins.~n", [length(Plugins)]).

-define(RABBIT_TEXT_LOGO,
        "~n  ##  ##      ~s ~s"
        "~n  ##  ##"
        "~n  ##########  ~s"
        "~n  ######  ##"
        "~n  ##########  ~s").
-define(FG8_START,  "\033[38;5;202m").
-define(BG8_START,  "\033[48;5;202m").
-define(FG32_START, "\033[38;2;255;102;0m").
-define(BG32_START, "\033[48;2;255;102;0m").
-define(C_END,      "\033[0m").
-define(RABBIT_8BITCOLOR_LOGO,
        "~n  " ?BG8_START "  " ?C_END "  " ?BG8_START "  " ?C_END "      \033[1m" ?FG8_START "~s" ?C_END " ~s"
        "~n  " ?BG8_START "  " ?C_END "  " ?BG8_START "  " ?C_END
        "~n  " ?BG8_START "          " ?C_END "  ~s"
        "~n  " ?BG8_START "      " ?C_END "  " ?BG8_START "  " ?C_END
        "~n  " ?BG8_START "          " ?C_END "  ~s").
-define(RABBIT_32BITCOLOR_LOGO,
        "~n  " ?BG32_START "  " ?C_END "  " ?BG32_START "  " ?C_END "      \033[1m" ?FG32_START "~s" ?C_END " ~s"
        "~n  " ?BG32_START "  " ?C_END "  " ?BG32_START "  " ?C_END
        "~n  " ?BG32_START "          " ?C_END "  ~s"
        "~n  " ?BG32_START "      " ?C_END "  " ?BG32_START "  " ?C_END
        "~n  " ?BG32_START "          " ?C_END "  ~s").

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
              "~n"
              "~n  Doc guides:  https://rabbitmq.com/documentation.html"
              "~n  Support:     https://rabbitmq.com/contact.html"
              "~n  Tutorials:   https://rabbitmq.com/getstarted.html"
              "~n  Monitoring:  https://rabbitmq.com/monitoring.html"
              "~n"
              "~n  Logs: ~ts" ++ LogFmt ++ "~n"
              "~n  Config file(s): ~ts" ++ CfgFmt ++ "~n"
              "~n  Starting broker...",
              [Product, Version, ?COPYRIGHT_MESSAGE, ?INFORMATION_MESSAGE] ++
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
            _ = rabbit_log:info("~n~ts", [string:trim(Padded, trailing, [$\r, $\n])])
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
               [{"database dir",   rabbit_mnesia:dir()}],
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
    _ = rabbit_log:info("~n~ts", [Banner]).

warn_if_kernel_config_dubious() ->
    case os:type() of
        {win32, _} ->
            ok;
        _ ->
            case erlang:system_info(kernel_poll) of
                true  -> ok;
                false -> _ = rabbit_log:warning(
                           "Kernel poll (epoll, kqueue, etc) is disabled. Throughput "
                           "and CPU utilization may worsen.~n")
            end
    end,
    DirtyIOSchedulers = erlang:system_info(dirty_io_schedulers),
    case DirtyIOSchedulers < ?DIRTY_IO_SCHEDULERS_WARNING_THRESHOLD of
        true  -> _ = rabbit_log:warning(
                   "Erlang VM is running with ~b dirty I/O schedulers, "
                   "file I/O performance may worsen~n", [DirtyIOSchedulers]);
        false -> ok
    end,
    IDCOpts = case application:get_env(kernel, inet_default_connect_options) of
                  undefined -> [];
                  {ok, Val} -> Val
              end,
    case proplists:get_value(nodelay, IDCOpts, false) of
        false -> _ = rabbit_log:warning("Nagle's algorithm is enabled for sockets, "
                                    "network I/O latency will be higher~n");
        true  -> ok
    end.

warn_if_disc_io_options_dubious() ->
    %% if these values are not set, it doesn't matter since
    %% rabbit_variable_queue will pick up the values defined in the
    %% IO_BATCH_SIZE and CREDIT_DISC_BOUND constants.
    CreditDiscBound = rabbit_misc:get_env(rabbit, msg_store_credit_disc_bound,
                                          undefined),
    IoBatchSize = rabbit_misc:get_env(rabbit, msg_store_io_batch_size,
                                      undefined),
    case catch validate_msg_store_io_batch_size_and_credit_disc_bound(
                 CreditDiscBound, IoBatchSize) of
        ok -> ok;
        {error, {Reason, Vars}} ->
            _ = rabbit_log:warning(Reason, Vars)
    end.

validate_msg_store_io_batch_size_and_credit_disc_bound(CreditDiscBound,
                                                       IoBatchSize) ->
    case IoBatchSize of
        undefined ->
            ok;
        IoBatchSize when is_integer(IoBatchSize) ->
            if IoBatchSize < ?IO_BATCH_SIZE ->
                    throw({error,
                     {"io_batch_size of ~b lower than recommended value ~b, "
                      "paging performance may worsen~n",
                      [IoBatchSize, ?IO_BATCH_SIZE]}});
               true ->
                    ok
            end;
        IoBatchSize ->
            throw({error,
             {"io_batch_size should be an integer, but ~b given",
              [IoBatchSize]}})
    end,

    %% CreditDiscBound = {InitialCredit, MoreCreditAfter}
    {RIC, RMCA} = ?CREDIT_DISC_BOUND,
    case CreditDiscBound of
        undefined ->
            ok;
        {IC, MCA} when is_integer(IC), is_integer(MCA) ->
            if IC < RIC; MCA < RMCA ->
                    throw({error,
                     {"msg_store_credit_disc_bound {~b, ~b} lower than"
                      "recommended value {~b, ~b},"
                      " paging performance may worsen~n",
                      [IC, MCA, RIC, RMCA]}});
               true ->
                    ok
            end;
        {IC, MCA} ->
            throw({error,
             {"both msg_store_credit_disc_bound values should be integers, but ~p given",
              [{IC, MCA}]}});
        CreditDiscBound ->
            throw({error,
             {"invalid msg_store_credit_disc_bound value given: ~p",
              [CreditDiscBound]}})
    end,

    case {CreditDiscBound, IoBatchSize} of
        {undefined, undefined} ->
            ok;
        {_CDB, undefined} ->
            ok;
        {undefined, _IBS} ->
            ok;
        {{InitialCredit, _MCA}, IoBatchSize} ->
            if IoBatchSize < InitialCredit ->
                    throw(
                      {error,
                       {"msg_store_io_batch_size ~b should be bigger than the initial "
                        "credit value from msg_store_credit_disc_bound ~b,"
                        " paging performance may worsen~n",
                        [IoBatchSize, InitialCredit]}});
               true ->
                    ok
            end
    end.

-spec product_name() -> string().

product_name() ->
    case product_info() of
        #{product_name := ProductName}   -> ProductName;
        #{product_base_name := BaseName} -> BaseName
    end.

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
      [fun rabbit_alarm:set_alarm/1, fun rabbit_alarm:clear_alarm/1]),
    ensure_working_fhc().

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
        _ = rabbit_log:info("FHC read buffering:  ~s~n", [ReadBuf]),
        _ = rabbit_log:info("FHC write buffering: ~s~n", [WriteBuf]),
        Filename = filename:join(code:lib_dir(kernel, ebin), "kernel.app"),
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
