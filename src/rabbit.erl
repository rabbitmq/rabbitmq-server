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

-export([start/0, stop/0, stop_and_halt/0, status/0, rotate_logs/1]).

-export([start/2, stop/1]).

-export([log_location/1]).

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

-spec(start/0 :: () -> 'ok').
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

start() ->
    try
        ok = ensure_working_log_handlers(),
        ok = rabbit_mnesia:ensure_mnesia_dir(),
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

    {ok, SupPid} = rabbit_sup:start_link(),

    print_banner(),

    {ok, ExtraSteps} = application:get_env(extra_startup_steps),

    lists:foreach(
      fun ({Msg, Thunk}) ->
              io:format("starting ~-20s ...", [Msg]),
              Thunk(),
              io:format("done~n");
          ({Msg, M, F, A}) ->
              io:format("starting ~-20s ...", [Msg]),
              apply(M, F, A),
              io:format("done~n")
      end,
      [{"database",
        fun () -> ok = rabbit_mnesia:init() end},
       {"core processes",
        fun () ->
                ok = start_child(rabbit_log),

                ok = rabbit_amqqueue:start(),

		{ok, MemoryAlarms} = application:get_env(memory_alarms),
                ok = rabbit_alarm:start(MemoryAlarms),
                
                ok = rabbit_binary_generator:
                    check_empty_content_body_frame_size(),

                ok = start_child(rabbit_router),
                ok = start_child(rabbit_node_monitor)
        end},
       {"recovery",
        fun () ->
                ok = maybe_insert_default_data(),
                ok = rabbit_exchange:recover(),
                ok = rabbit_amqqueue:recover()
        end},
       {"persister",
        fun () ->
                ok = start_child(rabbit_persister)
        end},
       {"guid generator",
        fun () ->
                ok = start_child(rabbit_guid)
        end},
       {"builtin applications",
        fun () ->
                {ok, DefaultVHost} = application:get_env(default_vhost),
                ok = error_logger:add_report_handler(
                       rabbit_error_logger, [DefaultVHost]),
                ok = start_builtin_amq_applications()
        end},
       {"TCP listeners",
        fun () ->
                ok = rabbit_networking:start(),
                {ok, TCPListeners} = application:get_env(tcp_listeners),
                lists:foreach(
                  fun ({Host, Port}) ->
                          ok = rabbit_networking:start_tcp_listener(Host, Port)
                  end,
                  TCPListeners)
        end}]
      ++ ExtraSteps),

    io:format("~nbroker running~n"),

    {ok, SupPid}.

stop(_State) ->
    terminated_ok = error_logger:delete_report_handler(rabbit_error_logger),
    ok = rabbit_alarm:stop(),
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

%---------------------------------------------------------------------------

print_banner() ->
    {ok, Product} = application:get_key(id),
    {ok, Version} = application:get_key(vsn),
    io:format("~s ~s (AMQP ~p-~p)~n~s~n~s~n~n",
              [Product, Version,
               ?PROTOCOL_VERSION_MAJOR, ?PROTOCOL_VERSION_MINOR,
               ?COPYRIGHT_MESSAGE, ?INFORMATION_MESSAGE]),
    Settings = [{"node",         node()},
                {"log",          log_location(kernel)},
                {"sasl log",     log_location(sasl)},
                {"database dir", rabbit_mnesia:dir()}],
    DescrLen = lists:max([length(K) || {K, _V} <- Settings]),
    Format = "~-" ++ integer_to_list(DescrLen) ++ "s: ~s~n",
    lists:foreach(fun ({K, V}) -> io:format(Format, [K, V]) end, Settings),
    io:nl().

start_child(Mod) ->
    {ok,_} = supervisor:start_child(rabbit_sup,
                                    {Mod, {Mod, start_link, []},
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
