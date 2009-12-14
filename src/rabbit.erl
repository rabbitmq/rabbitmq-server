%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit).

-behaviour(application).

-export([prepare/0, start/0, finish_boot/1, stop/0, stop_and_halt/0, status/0, rotate_logs/1]).

-export([start/2, stop/1]).

-export([log_location/1]).

%%---------------------------------------------------------------------------
%% Boot steps.
-export([boot_database/0, boot_core_processes/0, boot_recovery/0,
         boot_persister/0, boot_guid_generator/0,
         boot_builtin_applications/0, boot_tcp_listeners/0,
         boot_ssl_listeners/0]).

-rabbit_boot_step({boot_database/0, [{description, "database"}]}).
-rabbit_boot_step({boot_core_processes/0, [{description, "core processes"},
                                           {post, {?MODULE, boot_database/0}}]}).
-rabbit_boot_step({boot_recovery/0, [{description, "recovery"},
                                     {post, {?MODULE, boot_core_processes/0}}]}).
-rabbit_boot_step({boot_persister/0, [{description, "persister"},
                                      {post, {?MODULE, boot_recovery/0}}]}).
-rabbit_boot_step({boot_guid_generator/0, [{description, "guid generator"},
                                           {post, {?MODULE, boot_persister/0}}]}).
-rabbit_boot_step({boot_builtin_applications/0, [{description, "builtin applications"},
                                                 {post, {?MODULE, boot_guid_generator/0}}]}).
-rabbit_boot_step({boot_tcp_listeners/0, [{description, "TCP listeners"},
                                          {post, {?MODULE, boot_builtin_applications/0}}]}).
-rabbit_boot_step({boot_ssl_listeners/0, [{description, "SSL listeners"},
                                          {post, {?MODULE, boot_tcp_listeners/0}}]}).
%%---------------------------------------------------------------------------

-import(application).
-import(mnesia).
-import(lists).
-import(inet).
-import(gen_tcp).

-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-define(APPS, [os_mon, mnesia, rabbit]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(log_location() :: 'tty' | 'undefined' | string()).
-type(file_suffix() :: binary()).
-type(boot_step() :: {{atom(), {atom(), 0}}, [{atom(), any()}]}).

-spec(prepare/0 :: () -> 'ok').
-spec(start/0 :: () -> 'ok').
-spec(finish_boot/1 :: ([boot_step()]) -> 'ok').
-spec(stop/0 :: () -> 'ok').
-spec(stop_and_halt/0 :: () -> 'ok').
-spec(rotate_logs/1 :: (file_suffix()) -> 'ok' | {'error', any()}).
-spec(status/0 :: () ->
             [{running_applications, [{atom(), string(), string()}]} |
              {nodes, [erlang_node()]} |
              {running_nodes, [erlang_node()]}]).
-spec(log_location/1 :: ('sasl' | 'kernel') -> log_location()).

-endif.

%%----------------------------------------------------------------------------

prepare() ->
    ok = ensure_working_log_handlers(),
    ok = rabbit_mnesia:ensure_mnesia_dir().

start() ->
    try
        ok = prepare(),
        ok = rabbit_misc:start_applications(?APPS) 
    after
        %%give the error loggers some time to catch up
        timer:sleep(100)
    end.

stop() ->
    ok = rabbit_misc:stop_applications(?APPS).

stop_and_halt() ->
    spawn(fun () ->
                  SleepTime = 1000,
                  rabbit_log:info("Stop-and-halt request received; "
                                  "halting in ~p milliseconds~n",
                                  [SleepTime]),
                  timer:sleep(SleepTime),
                  init:stop()
          end),
    case catch stop() of _ -> ok end.

status() ->
    [{running_applications, application:which_applications()}] ++
        rabbit_mnesia:status().

rotate_logs(BinarySuffix) ->
    Suffix = binary_to_list(BinarySuffix),
    log_rotation_result(rotate_logs(log_location(kernel),
                                    Suffix,
                                    rabbit_error_logger_file_h),
                        rotate_logs(log_location(sasl),
                                    Suffix,
                                    rabbit_sasl_report_file_h)).

%%--------------------------------------------------------------------

start(normal, []) ->
    {ok, _SupPid} = rabbit_sup:start_link().

finish_boot(BootSteps) ->
    %% We set our group_leader so we appear to OTP to be part of the
    %% rabbit application.
    case application_controller:get_master(rabbit) of
        undefined ->
            exit({?MODULE, finish_boot, could_not_find_rabbit});
        MasterPid when is_pid(MasterPid) ->
            group_leader(MasterPid, self())
    end,

    print_banner(),
    [ok = run_boot_step(Step) || Step <- BootSteps],
    io:format("~nbroker running~n"),
    ok.

run_boot_step({ModFunSpec = {Module, {Fun, 0}}, Attributes}) ->
    Description = case lists:keysearch(description, 1, Attributes) of
                      {value, {_, D}} -> D;
                      false -> lists:flatten(io_lib:format("~w:~w", [Module, Fun]))
                  end,
    io:format("starting ~-20s ...", [Description]),
    case catch Module:Fun() of
        {'EXIT', Reason} ->
            io:format("FAILED~nReason: ~p~n", [Reason]),
            ChainedReason = {?MODULE, finish_boot, failed, ModFunSpec, Reason},
            error_logger:error_report(ChainedReason),
            timer:sleep(1000),
            exit(ChainedReason);
        ok ->
            io:format("done~n"),
            ok
    end.

boot_database() ->
    ok = rabbit_mnesia:init().

boot_core_processes() ->
    ok = start_child(rabbit_log),
    ok = rabbit_hooks:start(),

    ok = rabbit_binary_generator:check_empty_content_body_frame_size(),

    ok = rabbit_alarm:start(),

    {ok, MemoryWatermark} = application:get_env(vm_memory_high_watermark),
    ok = case MemoryWatermark == 0 of
             true ->
                 ok;
             false ->
                 start_child(vm_memory_monitor, [MemoryWatermark])
         end,

    ok = rabbit_amqqueue:start(),

    ok = start_child(rabbit_router),
    ok = start_child(rabbit_node_monitor).

boot_recovery() ->
    ok = maybe_insert_default_data(),
    ok = rabbit_exchange:recover(),
    ok = rabbit_amqqueue:recover().

boot_persister() ->
    ok = start_child(rabbit_persister).

boot_guid_generator() ->
    ok = start_child(rabbit_guid).

boot_builtin_applications() ->
    {ok, DefaultVHost} = application:get_env(default_vhost),
    ok = error_logger:add_report_handler(rabbit_error_logger, [DefaultVHost]),
    ok = start_builtin_amq_applications().

boot_tcp_listeners() ->
    ok = rabbit_networking:start(),
    {ok, TcpListeners} = application:get_env(tcp_listeners),
    [ok = rabbit_networking:start_tcp_listener(Host, Port)
     || {Host, Port} <- TcpListeners],
    ok.

boot_ssl_listeners() ->
    case application:get_env(ssl_listeners) of
        {ok, []} ->
            ok;
        {ok, SslListeners} ->
            ok = rabbit_misc:start_applications([crypto, ssl]),
            {ok, SslOpts} = application:get_env(ssl_options),
            [rabbit_networking:start_ssl_listener(Host, Port, SslOpts)
             || {Host, Port} <- SslListeners],
            ok
    end.

stop(_State) ->
    terminated_ok = error_logger:delete_report_handler(rabbit_error_logger),
    ok = rabbit_alarm:stop(),
    ok = case rabbit_mnesia:is_clustered() of
             true  -> rabbit_amqqueue:on_node_down(node());
             false -> rabbit_mnesia:empty_ram_only_tables()
         end,
    ok.

%---------------------------------------------------------------------------

log_location(Type) ->
    case application:get_env(Type, case Type of 
                                       kernel -> error_logger;
                                       sasl   -> sasl_error_logger
                                   end) of
        {ok, {file, File}} -> File;
        {ok, false}        -> undefined;
        {ok, tty}          -> tty;
        {ok, silent}       -> undefined;
        {ok, Bad}          -> throw({error, {cannot_log_to_file, Bad}});
        _                  -> undefined
    end.

app_location() ->
    {ok, Application} = application:get_application(),
    filename:absname(code:where_is_file(atom_to_list(Application) ++ ".app")).

home_dir() ->
    case init:get_argument(home) of
        {ok, [[Home]]} -> Home;
        Other          -> Other
    end.

%---------------------------------------------------------------------------

print_banner() ->
    {ok, Product} = application:get_key(id),
    {ok, Version} = application:get_key(vsn),
    ProductLen = string:len(Product),
    io:format("~n"
              "+---+   +---+~n"
              "|   |   |   |~n"
              "|   |   |   |~n"
              "|   |   |   |~n"
              "|   +---+   +-------+~n"
              "|                   |~n"
              "| ~s  +---+   |~n"
              "|           |   |   |~n"
              "| ~s  +---+   |~n"
              "|                   |~n"
              "+-------------------+~n"
              "AMQP ~p-~p~n~s~n~s~n~n",
              [Product, string:right([$v|Version], ProductLen),
               ?PROTOCOL_VERSION_MAJOR, ?PROTOCOL_VERSION_MINOR,
               ?COPYRIGHT_MESSAGE, ?INFORMATION_MESSAGE]),
    Settings = [{"node",           node()},
                {"app descriptor", app_location()},
                {"home dir",       home_dir()},
                {"cookie hash",    rabbit_misc:cookie_hash()},
                {"log",            log_location(kernel)},
                {"sasl log",       log_location(sasl)},
                {"database dir",   rabbit_mnesia:dir()}],
    DescrLen = lists:max([length(K) || {K, _V} <- Settings]),
    Format = "~-" ++ integer_to_list(DescrLen) ++ "s: ~s~n",
    lists:foreach(fun ({K, V}) -> io:format(Format, [K, V]) end, Settings),
    io:nl().

start_child(Mod) ->
    start_child(Mod, []).

start_child(Mod, Args) ->
    {ok,_} = supervisor:start_child(rabbit_sup,
                                    {Mod, {Mod, start_link, Args},
                                     transient, 100, worker, [Mod]}),
    ok.

ensure_working_log_handlers() ->
    Handlers = gen_event:which_handlers(error_logger),
    ok = ensure_working_log_handler(error_logger_file_h,
                                    rabbit_error_logger_file_h,
                                    error_logger_tty_h,
                                    log_location(kernel),
                                    Handlers),

    ok = ensure_working_log_handler(sasl_report_file_h,
                                    rabbit_sasl_report_file_h,
                                    sasl_report_tty_h,
                                    log_location(sasl),
                                    Handlers),
    ok.

ensure_working_log_handler(OldFHandler, NewFHandler, TTYHandler,
                           LogLocation, Handlers) ->
    case LogLocation of
        undefined -> ok;
        tty       -> case lists:member(TTYHandler, Handlers) of
                         true  -> ok;
                         false ->
                             throw({error, {cannot_log_to_tty,
                                            TTYHandler, not_installed}})
                     end;
        _         -> case lists:member(NewFHandler, Handlers) of 
                         true  -> ok;
                         false -> case rotate_logs(LogLocation, "",
                                                   OldFHandler, NewFHandler) of
                                      ok -> ok;
                                      {error, Reason} ->
                                          throw({error, {cannot_log_to_file,
                                                         LogLocation, Reason}})
                                  end
                     end
    end.

maybe_insert_default_data() ->
    case rabbit_mnesia:is_db_empty() of
        true -> insert_default_data();
        false -> ok
    end.

insert_default_data() ->
    {ok, DefaultUser} = application:get_env(default_user),
    {ok, DefaultPass} = application:get_env(default_pass),
    {ok, DefaultVHost} = application:get_env(default_vhost),
    {ok, [DefaultConfigurePerm, DefaultWritePerm, DefaultReadPerm]} =
        application:get_env(default_permissions),
    ok = rabbit_access_control:add_vhost(DefaultVHost),
    ok = rabbit_access_control:add_user(DefaultUser, DefaultPass),
    ok = rabbit_access_control:set_permissions(DefaultUser, DefaultVHost,
                                               DefaultConfigurePerm,
                                               DefaultWritePerm,
                                               DefaultReadPerm),
    ok.

start_builtin_amq_applications() ->
    %%TODO: we may want to create a separate supervisor for these so
    %%they don't bring down the entire app when they die and fail to
    %%restart
    ok.

rotate_logs(File, Suffix, Handler) ->
    rotate_logs(File, Suffix, Handler, Handler).

rotate_logs(File, Suffix, OldHandler, NewHandler) ->
    case File of
        undefined -> ok;
        tty       -> ok;
        _         -> gen_event:swap_handler(
                       error_logger,
                       {OldHandler, swap},
                       {NewHandler, {File, Suffix}})
    end.

log_rotation_result({error, MainLogError}, {error, SaslLogError}) ->
    {error, {{cannot_rotate_main_logs, MainLogError},
             {cannot_rotate_sasl_logs, SaslLogError}}};
log_rotation_result({error, MainLogError}, ok) ->
    {error, {cannot_rotate_main_logs, MainLogError}};
log_rotation_result(ok, {error, SaslLogError}) ->
    {error, {cannot_rotate_sasl_logs, SaslLogError}};
log_rotation_result(ok, ok) ->
    ok.
