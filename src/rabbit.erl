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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit).

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
-export([log_locations/0, config_files/0, decrypt_config/2]). %% for testing and mgmt-agent
-export([is_booted/1, is_booted/0, is_booting/1, is_booting/0]).

-deprecated([{force_event_refresh, 1, eventually}]).

-ifdef(TEST).

-export([start_logger/0]).

-endif.

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
                   [{description, "worker pool"},
                    {mfa,         {rabbit_sup, start_supervisor_child,
                                   [worker_pool_sup]}},
                    {requires,    pre_boot},
                    {enables,     external_infrastructure}]}).

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
                    {requires,    core_initialized}]}).

-rabbit_boot_step({direct_client,
                   [{description, "direct client"},
                    {mfa,         {rabbit_direct, boot, []}},
                    {requires,    routing_ready}]}).

-rabbit_boot_step({connection_tracking,
                   [{description, "sets up internal storage for node-local connections"},
                    {mfa,         {rabbit_connection_tracking, boot, []}},
                    {requires,    routing_ready}]}).

-rabbit_boot_step({networking,
                   [{mfa,         {rabbit_networking, boot, []}},
                    {requires,    routing_ready}]}).

-rabbit_boot_step({notify_cluster,
                   [{description, "notify cluster nodes"},
                    {mfa,         {rabbit_node_monitor, notify_node_up, []}},
                    {requires,    networking}]}).

-rabbit_boot_step({background_gc,
                   [{description, "background garbage collection"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [background_gc]}},
                    {enables,     networking}]}).

-rabbit_boot_step({rabbit_core_metrics_gc,
                   [{description, "background core metrics garbage collection"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_core_metrics_gc]}},
                    {enables,     networking}]}).

-rabbit_boot_step({rabbit_looking_glass,
                   [{description, "Looking Glass tracer and profiler"},
                    {mfa,         {rabbit_looking_glass, boot, []}},
                    {requires,    networking}]}).

%%---------------------------------------------------------------------------

-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-define(APPS, [os_mon, mnesia, rabbit_common, ra, sysmon_handler, rabbit]).

-define(ASYNC_THREADS_WARNING_THRESHOLD, 8).

%% 1 minute
-define(BOOT_START_TIMEOUT,     1 * 60 * 1000).
%% 12 hours
-define(BOOT_FINISH_TIMEOUT,    12 * 60 * 60 * 1000).
%% 100 ms
-define(BOOT_STATUS_CHECK_INTERVAL, 100).

%%----------------------------------------------------------------------------

-type restart_type() :: 'permanent' | 'transient' | 'temporary'.
%% this really should be an abstract type
-type log_location() :: string().
-type param() :: atom().
-type app_name() :: atom().

%%----------------------------------------------------------------------------

ensure_application_loaded() ->
    %% We end up looking at the rabbit app's env for HiPE and log
    %% handling, so it needs to be loaded. But during the tests, it
    %% may end up getting loaded twice, so guard against that.
    case application:load(rabbit) of
        ok                                -> ok;
        {error, {already_loaded, rabbit}} -> ok
    end.

-spec start() -> 'ok'.

start() ->
    start_it(fun() ->
                     %% We do not want to upgrade mnesia after just
                     %% restarting the app.
                     ok = ensure_application_loaded(),
                     HipeResult = rabbit_hipe:maybe_hipe_compile(),
                     ok = start_logger(),
                     rabbit_hipe:log_hipe_result(HipeResult),
                     Apps = load_all_apps(),
                     rabbit_feature_flags:initialize_registry(),
                     rabbit_node_monitor:prepare_cluster_status_files(),
                     rabbit_mnesia:check_cluster_consistency(),
                     broker_start(Apps)
             end).

-spec boot() -> 'ok'.

boot() ->
    start_it(fun() ->
                     ensure_config(),
                     ok = ensure_application_loaded(),
                     HipeResult = rabbit_hipe:maybe_hipe_compile(),
                     ok = start_logger(),
                     rabbit_hipe:log_hipe_result(HipeResult),
                     Apps = load_all_apps(),
                     rabbit_feature_flags:initialize_registry(),
                     rabbit_node_monitor:prepare_cluster_status_files(),
                     ok = rabbit_upgrade:maybe_upgrade_mnesia(),
                     %% It's important that the consistency check happens after
                     %% the upgrade, since if we are a secondary node the
                     %% primary node will have forgotten us
                     rabbit_mnesia:check_cluster_consistency(),
                     broker_start(Apps)
             end).

ensure_config() ->
    case rabbit_config:validate_config_files() of
        ok -> ok;
        {error, {ErrFmt, ErrArgs}} ->
            log_boot_error_and_exit(check_config_file, ErrFmt, ErrArgs)
    end,
    case rabbit_config:prepare_and_use_config() of
        {error, {generation_error, Error}} ->
            log_boot_error_and_exit(generate_config_file,
                                    "~nConfig file generation failed ~s",
                                    Error);
        ok -> ok
    end.

load_all_apps() ->
    Plugins = rabbit_plugins:setup(),
    ToBeLoaded = Plugins ++ ?APPS,
    app_utils:load_applications(ToBeLoaded),
    ToBeLoaded.

broker_start(Apps) ->
    start_loaded_apps(Apps),
    maybe_sd_notify(),
    ok = rabbit_lager:broker_is_started(),
    ok = log_broker_started(rabbit_plugins:strictly_plugins(rabbit_plugins:active())).

%% Try to send systemd ready notification if it makes sense in the
%% current environment. standard_error is used intentionally in all
%% logging statements, so all this messages will end in systemd
%% journal.
maybe_sd_notify() ->
    case sd_notify_ready() of
        false ->
            io:format(standard_error, "systemd READY notification failed, beware of timeouts~n", []);
        _ ->
            ok
    end.

sd_notify_ready() ->
    case {os:type(), os:getenv("NOTIFY_SOCKET")} of
        {{win32, _}, _} ->
            true;
        {_, [_|_]} -> %% Non-empty NOTIFY_SOCKET, give it a try
            sd_notify_legacy() orelse sd_notify_socat();
        _ ->
            true
    end.

sd_notify_data() ->
    "READY=1\nSTATUS=Initialized\nMAINPID=" ++ os:getpid() ++ "\n".

sd_notify_legacy() ->
    case code:load_file(sd_notify) of
        {module, sd_notify} ->
            SDNotify = sd_notify,
            SDNotify:sd_notify(0, sd_notify_data()),
            true;
        {error, _} ->
            false
    end.

%% socat(1) is the most portable way the sd_notify could be
%% implemented in erlang, without introducing some NIF. Currently the
%% following issues prevent us from implementing it in a more
%% reasonable way:
%% - systemd-notify(1) is unstable for non-root users
%% - erlang doesn't support unix domain sockets.
%%
%% Some details on how we ended with such a solution:
%%   https://github.com/rabbitmq/rabbitmq-server/issues/664
sd_notify_socat() ->
    case sd_current_unit() of
        {ok, Unit} ->
            io:format(standard_error, "systemd unit for activation check: \"~s\"~n", [Unit]),
            sd_notify_socat(Unit);
        _ ->
            false
    end.

socat_socket_arg("@" ++ AbstractUnixSocket) ->
    "abstract-sendto:" ++ AbstractUnixSocket;
socat_socket_arg(UnixSocket) ->
    "unix-sendto:" ++ UnixSocket.

sd_open_port() ->
    open_port(
      {spawn_executable, os:find_executable("socat")},
      [{args, [socat_socket_arg(os:getenv("NOTIFY_SOCKET")), "STDIO"]},
       use_stdio, out]).

sd_notify_socat(Unit) ->
    try sd_open_port() of
        Port ->
            Port ! {self(), {command, sd_notify_data()}},
            Result = sd_wait_activation(Port, Unit),
            port_close(Port),
            Result
    catch
        Class:Reason ->
            io:format(standard_error, "Failed to start socat ~p:~p~n", [Class, Reason]),
            false
    end.

sd_current_unit() ->
    CmdOut = os:cmd("ps -o unit= -p " ++ os:getpid()),
    case catch re:run(CmdOut, "([-.@0-9a-zA-Z]+)", [unicode, {capture, all_but_first, list}]) of
        {'EXIT', _} ->
            error;
        {match, [Unit]} ->
            {ok, Unit};
        _ ->
            error
    end.

sd_wait_activation(Port, Unit) ->
    case os:find_executable("systemctl") of
        false ->
            io:format(standard_error, "'systemctl' unavailable, falling back to sleep~n", []),
            timer:sleep(5000),
            true;
        _ ->
            sd_wait_activation(Port, Unit, 10)
    end.

sd_wait_activation(_, _, 0) ->
    io:format(standard_error, "Service still in 'activating' state, bailing out~n", []),
    false;
sd_wait_activation(Port, Unit, AttemptsLeft) ->
    case os:cmd("systemctl show --property=ActiveState -- '" ++ Unit ++ "'") of
        "ActiveState=activating\n" ->
            timer:sleep(1000),
            sd_wait_activation(Port, Unit, AttemptsLeft - 1);
        "ActiveState=" ++ _ ->
            true;
        _ = Err->
            io:format(standard_error, "Unexpected status from systemd ~p~n", [Err]),
            false
    end.

start_it(StartFun) ->
    Marker = spawn_link(fun() -> receive stop -> ok end end),
    case catch register(rabbit_boot, Marker) of
        true -> try
                    case is_running() of
                        true  -> ok;
                        false -> StartFun()
                    end
                catch
                    Class:Reason ->
                        boot_error(Class, Reason)
                after
                    unlink(Marker),
                    Marker ! stop,
                    %% give the error loggers some time to catch up
                    timer:sleep(100)
                end;
        _    -> unlink(Marker),
                Marker ! stop
    end.

-spec stop() -> 'ok'.

stop() ->
    case whereis(rabbit_boot) of
        undefined -> ok;
        _         ->
            rabbit_log:info("RabbitMQ hasn't finished starting yet. Waiting for startup to finish before stopping..."),
            ok = wait_for_boot_to_finish(node())
    end,
    rabbit_log:info("RabbitMQ is asked to stop...~n", []),
    Apps = ?APPS ++ rabbit_plugins:active(),
    %% this will also perform unregistration with the peer discovery backend
    %% as needed
    stop_apps(app_utils:app_dependency_order(Apps, true)),
    rabbit_log:info("Successfully stopped RabbitMQ and its dependencies~n", []).

-spec stop_and_halt() -> no_return().

stop_and_halt() ->
    try
        stop()
    catch Type:Reason ->
        rabbit_log:error("Error trying to stop RabbitMQ: ~p:~p", [Type, Reason]),
        error({Type, Reason})
    after
        %% Enclose all the logging in the try block.
        %% init:stop() will be called regardless of any errors.
        try
            AppsLeft = [ A || {A, _, _} <- application:which_applications() ],
            rabbit_log:info(
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

start_apps(Apps, RestartTypes) ->
    app_utils:load_applications(Apps),
    ok = rabbit_feature_flags:refresh_feature_flags_after_app_load(Apps),
    start_loaded_apps(Apps, RestartTypes).

start_loaded_apps(Apps) ->
    start_loaded_apps(Apps, #{}).

start_loaded_apps(Apps, RestartTypes) ->
    ensure_sysmon_handler_app_config(),
    %% make Ra use a custom logger that dispatches to lager instead of the
    %% default OTP logger
    application:set_env(ra, logger_module, rabbit_log_ra_shim),
    ConfigEntryDecoder = case application:get_env(rabbit, config_entry_decoder) of
        undefined ->
            [];
        {ok, Val} ->
            Val
    end,
    PassPhrase = case proplists:get_value(passphrase, ConfigEntryDecoder) of
        prompt ->
            IoDevice = get_input_iodevice(),
            io:setopts(IoDevice, [{echo, false}]),
            PP = lists:droplast(io:get_line(IoDevice,
                "\nPlease enter the passphrase to unlock encrypted "
                "configuration entries.\n\nPassphrase: ")),
            io:setopts(IoDevice, [{echo, true}]),
            io:format(IoDevice, "~n", []),
            PP;
        {file, Filename} ->
            {ok, File} = file:read_file(Filename),
            [PP|_] = binary:split(File, [<<"\r\n">>, <<"\n">>]),
            PP;
        PP ->
            PP
    end,
    Algo = {
        proplists:get_value(cipher, ConfigEntryDecoder, rabbit_pbe:default_cipher()),
        proplists:get_value(hash, ConfigEntryDecoder, rabbit_pbe:default_hash()),
        proplists:get_value(iterations, ConfigEntryDecoder, rabbit_pbe:default_iterations()),
        PassPhrase
    },
    decrypt_config(Apps, Algo),
    OrderedApps = app_utils:app_dependency_order(Apps, false),
    case lists:member(rabbit, Apps) of
        false -> rabbit_boot_steps:run_boot_steps(Apps); %% plugin activation
        true  -> ok                    %% will run during start of rabbit app
    end,
    ok = app_utils:start_applications(OrderedApps,
                                      handle_app_error(could_not_start),
                                      RestartTypes).

%% rabbitmq/rabbitmq-server#952
%% This function is to be called after configuration has been optionally generated
%% and the sysmon_handler application loaded, but not started. It will ensure that
%% sane defaults are used for configuration settings that haven't been set by the
%% user
ensure_sysmon_handler_app_config() ->
    Defaults = [
                {process_limit, 100},
                {port_limit, 100},
                {gc_ms_limit, 0},
                {schedule_ms_limit, 0},
                {heap_word_limit, 0},
                {busy_port, false},
                {busy_dist_port, true}
               ],
    lists:foreach(fun({K, V}) ->
                          case application:get_env(sysmon_handler, K) of
                              undefined ->
                                  application:set_env(sysmon_handler, K, V);
                              _ ->
                                  ok
                          end
                  end, Defaults).

%% This function retrieves the correct IoDevice for requesting
%% input. The problem with using the default IoDevice is that
%% the Erlang shell prevents us from getting the input.
%%
%% Instead we therefore look for the io process used by the
%% shell and if it can't be found (because the shell is not
%% started e.g with -noshell) we use the 'user' process.
%%
%% This function will not work when either -oldshell or -noinput
%% options are passed to erl.
get_input_iodevice() ->
    case whereis(user) of
        undefined -> user;
        User ->
            case group:interfaces(User) of
                [] ->
                    user;
                [{user_drv, Drv}] ->
                    case user_drv:interfaces(Drv) of
                        [] ->
                            user;
                        [{current_group, IoDevice}] ->
                            IoDevice
                    end
            end
    end.

decrypt_config([], _) ->
    ok;
decrypt_config([App|Apps], Algo) ->
    decrypt_app(App, application:get_all_env(App), Algo),
    decrypt_config(Apps, Algo).

decrypt_app(_, [], _) ->
    ok;
decrypt_app(App, [{Key, Value}|Tail], Algo) ->
    try begin
            case decrypt(Value, Algo) of
                Value ->
                    ok;
                NewValue ->
                    application:set_env(App, Key, NewValue)
            end
        end
    catch
        exit:{bad_configuration, config_entry_decoder} ->
            exit({bad_configuration, config_entry_decoder});
        _:Msg ->
            rabbit_log:info("Error while decrypting key '~p'. Please check encrypted value, passphrase, and encryption configuration~n", [Key]),
            exit({decryption_error, {key, Key}, Msg})
    end,
    decrypt_app(App, Tail, Algo).

decrypt({encrypted, _}, {_, _, _, undefined}) ->
    exit({bad_configuration, config_entry_decoder});
decrypt({encrypted, EncValue}, {Cipher, Hash, Iterations, Password}) ->
    rabbit_pbe:decrypt_term(Cipher, Hash, Iterations, Password, EncValue);
decrypt(List, Algo) when is_list(List) ->
    decrypt_list(List, Algo, []);
decrypt(Value, _) ->
    Value.

%% We make no distinction between strings and other lists.
%% When we receive a string, we loop through each element
%% and ultimately return the string unmodified, as intended.
decrypt_list([], _, Acc) ->
    lists:reverse(Acc);
decrypt_list([{Key, Value}|Tail], Algo, Acc) when Key =/= encrypted ->
    decrypt_list(Tail, Algo, [{Key, decrypt(Value, Algo)}|Acc]);
decrypt_list([Value|Tail], Algo, Acc) ->
    decrypt_list(Tail, Algo, [decrypt(Value, Algo)|Acc]).

-spec stop_apps([app_name()]) -> 'ok'.

stop_apps([]) ->
    ok;
stop_apps(Apps) ->
    rabbit_log:info(
        lists:flatten(["Stopping RabbitMQ applications and their dependencies in the following order:~n",
                       ["    ~p~n" || _ <- Apps]]),
        lists:reverse(Apps)),
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

is_booting(Node) ->
    case rpc:call(Node, erlang, whereis, [rabbit_boot]) of
        {badrpc, _} = Err -> Err;
        undefined         -> false;
        P when is_pid(P)  -> true
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

wait_for_boot_to_finish(Node) ->
    wait_for_boot_to_finish(Node, false, ?BOOT_FINISH_TIMEOUT).

wait_for_boot_to_finish(Node, PrintProgressReports) ->
    wait_for_boot_to_finish(Node, PrintProgressReports, ?BOOT_FINISH_TIMEOUT).

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
    S1 = [{pid,                  list_to_integer(os:getpid())},
          %% The timeout value used is twice that of gen_server:call/2.
          {running_applications, rabbit_misc:which_applications()},
          {os,                   os:type()},
          {erlang_version,       erlang:system_info(system_version)},
          {memory,               rabbit_vm:memory()},
          {alarms,               alarms()},
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
    S1 ++ S2 ++ S3 ++ S4.

alarms() ->
    Alarms = rabbit_misc:with_exit_handler(rabbit_misc:const([]),
                                           fun rabbit_alarm:get_alarms/0),
    N = node(),
    %% [{{resource_limit,memory,rabbit@mercurio},[]}]
    [Limit || {{resource_limit, Limit, Node}, _} <- Alarms, Node =:= N].

listeners() ->
    Listeners = try
                    rabbit_networking:active_listeners()
                catch
                    exit:{aborted, _} -> []
                end,
    [{Protocol, Port, rabbit_misc:ntoa(IP)} ||
        #listener{node       = Node,
                  protocol   = Protocol,
                  ip_address = IP,
                  port       = Port} <- Listeners, Node =:= node()].

%% TODO this only determines if the rabbit application has started,
%% not if it is running, never mind plugins. It would be nice to have
%% more nuance here.

-spec is_running() -> boolean().

is_running() -> is_running(node()).

-spec is_running(node()) -> boolean().

is_running(Node) -> rabbit_nodes:is_process_running(Node, rabbit).

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
              %% FIXME: This message is asynchronous, therefore this
              %% entire call is asynchronous: at the end of this
              %% function, we can't guaranty the rotation is completed.
              [SinkName ! {rotate, FileName} || FileName <- FileNames],
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
    case erts_version_check() of
        ok ->
            rabbit_log:info("~n Starting RabbitMQ ~s on Erlang ~s~n ~s~n ~s~n",
                            [rabbit_misc:version(), rabbit_misc:otp_release(),
                             ?COPYRIGHT_MESSAGE, ?INFORMATION_MESSAGE]),
            {ok, SupPid} = rabbit_sup:start_link(),
            true = register(rabbit, self()),
            print_banner(),
            log_banner(),
            warn_if_kernel_config_dubious(),
            warn_if_disc_io_options_dubious(),
            rabbit_boot_steps:run_boot_steps(),
            {ok, SupPid};
        {error, {erlang_version_too_old,
                 {found, OTPRel, ERTSVer},
                 {required, ?OTP_MINIMUM, ?ERTS_MINIMUM}}} ->
            Msg = "This RabbitMQ version cannot run on Erlang ~s (erts ~s): "
                  "minimum required version is ~s (erts ~s)",
            Args = [OTPRel, ERTSVer, ?OTP_MINIMUM, ?ERTS_MINIMUM],
            rabbit_log:error(Msg, Args),
            %% also print to stderr to make this more visible
            io:format(standard_error, "Error: " ++ Msg ++ "~n", Args),
            {error, {erlang_version_too_old, rabbit_misc:format("Erlang ~s or later is required, started on ~s", [?OTP_MINIMUM, OTPRel])}};
        Error ->
            Error
    end.

prep_stop(State) ->
  rabbit_peer_discovery:maybe_unregister(),
  State.

-spec stop(_) -> 'ok'.

stop(_State) ->
    ok = rabbit_alarm:stop(),
    ok = case rabbit_mnesia:is_clustered() of
             true  -> ok;
             false -> rabbit_table:clear_ram_only_tables()
         end,
    ok.

-spec boot_error(term(), not_available | [tuple()]) -> no_return().

boot_error(_, {could_not_start, rabbit, {{timeout_waiting_for_tables, _}, _}}) ->
    AllNodes = rabbit_mnesia:cluster_nodes(all),
    Suffix = "~nBACKGROUND~n==========~n~n"
        "This cluster node was shut down while other nodes were still running.~n"
        "To avoid losing data, you should start the other nodes first, then~n"
        "start this one. To force this node to start, first invoke~n"
        "\"rabbitmqctl force_boot\". If you do so, any changes made on other~n"
        "cluster nodes after this one was shut down may be lost.~n",
    {Err, Nodes} =
        case AllNodes -- [node()] of
            [] -> {"Timeout contacting cluster nodes. Since RabbitMQ was"
                   " shut down forcefully~nit cannot determine which nodes"
                   " are timing out.~n" ++ Suffix, []};
            Ns -> {rabbit_misc:format(
                     "Timeout contacting cluster nodes: ~p.~n" ++ Suffix, [Ns]),
                   Ns}
        end,
    log_boot_error_and_exit(
      timeout_waiting_for_tables,
      "~n" ++ Err ++ rabbit_nodes:diagnostics(Nodes), []);
boot_error(Class, {error, {cannot_log_to_file, _, _}} = Reason) ->
    log_boot_error_and_exit(
      Reason,
      "~nError description:~s",
      [lager:pr_stacktrace(erlang:get_stacktrace(), {Class, Reason})]);
boot_error(Class, Reason) ->
    LogLocations = log_locations(),
    log_boot_error_and_exit(
      Reason,
      "~nError description:~s"
      "~nLog file(s) (may contain more information):~n" ++
      lists:flatten(["   ~s~n" || _ <- lists:seq(1, length(LogLocations))]),
      [lager:pr_stacktrace(erlang:get_stacktrace(), {Class, Reason})] ++
      LogLocations).

-spec log_boot_error_and_exit(_, _, _) -> no_return().
log_boot_error_and_exit(Reason, Format, Args) ->
    rabbit_log:error(Format, Args),
    io:format(standard_error, "~nBOOT FAILED~n===========~n" ++ Format ++ "~n", Args),
    timer:sleep(1000),
    exit(Reason).

%%---------------------------------------------------------------------------
%% boot step functions

-spec boot_delegate() -> 'ok'.

boot_delegate() ->
    {ok, Count} = application:get_env(rabbit, delegate_count),
    rabbit_sup:start_supervisor_child(delegate_sup, [Count]).

-spec recover() -> 'ok'.

recover() ->
    rabbit_policy:recover(),
    rabbit_vhost:recover().

-spec maybe_insert_default_data() -> 'ok'.

maybe_insert_default_data() ->
    case rabbit_table:needs_default_data() of
        true  -> insert_default_data();
        false -> ok
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

    ok = rabbit_vhost:add(DefaultVHostBin, ?INTERNAL_USER),
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

start_logger() ->
    rabbit_lager:start_logger(),
    ok.

-spec log_locations() -> [log_location()].

log_locations() ->
    rabbit_lager:log_locations().

%% This feature was used by the management API up-to and including
%% RabbitMQ 3.7.x. It is unused in 3.8.x and thus deprecated. We keep it
%% to support in-place upgrades to 3.8.x (i.e. mixed-version clusters).

-spec force_event_refresh(reference()) -> 'ok'.

force_event_refresh(Ref) ->
    rabbit_direct:force_event_refresh(Ref),
    rabbit_networking:force_connection_event_refresh(Ref),
    rabbit_channel:force_event_refresh(Ref),
    rabbit_amqqueue:force_event_refresh(Ref).

%%---------------------------------------------------------------------------
%% misc

log_broker_started(Plugins) ->
    PluginList = iolist_to_binary([rabbit_misc:format(" * ~s~n", [P])
                                   || P <- Plugins]),
    Message = string:strip(rabbit_misc:format(
        "Server startup complete; ~b plugins started.~n~s",
        [length(Plugins), PluginList]), right, $\n),
    rabbit_log:info(Message),
    io:format(" completed with ~p plugins.~n", [length(Plugins)]).

erts_version_check() ->
    ERTSVer = erlang:system_info(version),
    OTPRel = rabbit_misc:otp_release(),
    case rabbit_misc:version_compare(?ERTS_MINIMUM, ERTSVer, lte) of
        true when ?ERTS_MINIMUM =/= ERTSVer ->
            ok;
        true when ?ERTS_MINIMUM =:= ERTSVer andalso ?OTP_MINIMUM =< OTPRel ->
            %% When a critical regression or bug is found, a new OTP
            %% release can be published without changing the ERTS
            %% version. For instance, this is the case with R16B03 and
            %% R16B03-1.
            %%
            %% In this case, we compare the release versions
            %% alphabetically.
            ok;
        _ -> {error, {erlang_version_too_old,
                      {found, OTPRel, ERTSVer},
                      {required, ?OTP_MINIMUM, ?ERTS_MINIMUM}}}
    end.

print_banner() ->
    {ok, Product} = application:get_key(description),
    {ok, Version} = application:get_key(vsn),
    {LogFmt, LogLocations} = case log_locations() of
        [_ | Tail] = LL ->
            LF = lists:flatten(["~n                    ~s"
                                || _ <- lists:seq(1, length(Tail))]),
            {LF, LL};
        [] ->
            {"", ["(none)"]}
    end,
    io:format("~n  ##  ##"
              "~n  ##  ##      ~s ~s. ~s"
              "~n  ##########  ~s"
              "~n  ######  ##"
              "~n  ##########  Logs: ~s" ++
              LogFmt ++
              "~n~n              Starting broker..."
              "~n",
              [Product, Version, ?COPYRIGHT_MESSAGE, ?INFORMATION_MESSAGE] ++
              LogLocations).

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
                       " ~-" ++ integer_to_list(DescrLen) ++ "s: ~s~n", [K, V])
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
    rabbit_log:info("~n~s", [Banner]).

warn_if_kernel_config_dubious() ->
    case os:type() of
        {win32, _} ->
            ok;
        _ ->
            case erlang:system_info(kernel_poll) of
                true  -> ok;
                false -> rabbit_log:warning(
                           "Kernel poll (epoll, kqueue, etc) is disabled. Throughput "
                           "and CPU utilization may worsen.~n")
            end
    end,
    AsyncThreads = erlang:system_info(thread_pool_size),
    case AsyncThreads < ?ASYNC_THREADS_WARNING_THRESHOLD of
        true  -> rabbit_log:warning(
                   "Erlang VM is running with ~b I/O threads, "
                   "file I/O performance may worsen~n", [AsyncThreads]);
        false -> ok
    end,
    IDCOpts = case application:get_env(kernel, inet_default_connect_options) of
                  undefined -> [];
                  {ok, Val} -> Val
              end,
    case proplists:get_value(nodelay, IDCOpts, false) of
        false -> rabbit_log:warning("Nagle's algorithm is enabled for sockets, "
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
            rabbit_log:warning(Reason, Vars)
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

home_dir() ->
    case init:get_argument(home) of
        {ok, [[Home]]} -> Home;
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
        rabbit_log:info("FHC read buffering:  ~s~n", [ReadBuf]),
        rabbit_log:info("FHC write buffering: ~s~n", [WriteBuf]),
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
