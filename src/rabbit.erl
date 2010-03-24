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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit).

-behaviour(application).

-export([prepare/0, start/0, stop/0, stop_and_halt/0, status/0, rotate_logs/1]).

-export([start/2, stop/1]).

-export([log_location/1]).

%%---------------------------------------------------------------------------
%% Boot steps.
-export([maybe_insert_default_data/0]).

-rabbit_boot_step({codec_correctness_check,
                   [{description, "codec correctness check"},
                    {mfa,         {rabbit_binary_generator,
                                   check_empty_content_body_frame_size,
                                   []}}]}).

-rabbit_boot_step({database,
                   [{mfa,         {rabbit_mnesia, init, []}},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step({worker_pool,
                   [{description, "worker pool"},
                    {mfa,         {rabbit_sup, start_child, [worker_pool_sup]}},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step({external_infrastructure,
                   [{description, "external infrastructure ready"}]}).

-rabbit_boot_step({rabbit_exchange_type_registry,
                   [{description, "exchange type registry"},
                    {mfa,         {rabbit_sup, start_child,
                                   [rabbit_exchange_type_registry]}},
                    {enables,     kernel_ready},
                    {requires,    external_infrastructure}]}).

-rabbit_boot_step({rabbit_log,
                   [{description, "logging server"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_log]}},
                    {enables,     kernel_ready},
                    {requires,    external_infrastructure}]}).

-rabbit_boot_step({rabbit_hooks,
                   [{description, "internal event notification system"},
                    {mfa,         {rabbit_hooks, start, []}},
                    {enables,     kernel_ready},
                    {requires,    external_infrastructure}]}).

-rabbit_boot_step({kernel_ready,
                   [{description, "kernel ready"},
                    {requires,    external_infrastructure}]}).

-rabbit_boot_step({rabbit_alarm,
                   [{description, "alarm handler"},
                    {mfa,         {rabbit_alarm, start, []}},
                    {requires,    kernel_ready},
                    {enables,     core_initialized}]}).

-rabbit_boot_step({rabbit_amqqueue_sup,
                   [{description, "queue supervisor"},
                    {mfa,         {rabbit_amqqueue, start, []}},
                    {requires,    kernel_ready},
                    {enables,     core_initialized}]}).

-rabbit_boot_step({rabbit_router,
                   [{description, "cluster router"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_router]}},
                    {requires,    kernel_ready},
                    {enables,     core_initialized}]}).

-rabbit_boot_step({rabbit_node_monitor,
                   [{description, "node monitor"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_node_monitor]}},
                    {requires,    kernel_ready},
                    {requires,    rabbit_amqqueue_sup},
                    {enables,     core_initialized}]}).

-rabbit_boot_step({core_initialized,
                   [{description, "core initialized"}]}).

-rabbit_boot_step({empty_db_check,
                   [{description, "empty DB check"},
                    {mfa,         {?MODULE, maybe_insert_default_data, []}},
                    {requires,    core_initialized}]}).

-rabbit_boot_step({exchange_recovery,
                   [{description, "exchange recovery"},
                    {mfa,         {rabbit_exchange, recover, []}},
                    {requires,    empty_db_check}]}).

-rabbit_boot_step({queue_recovery,
                   [{description, "queue recovery"},
                    {mfa,         {rabbit_amqqueue, recover, []}},
                    {requires,    exchange_recovery}]}).

-rabbit_boot_step({persister,
                   [{mfa,         {rabbit_sup, start_child, [rabbit_persister]}},
                    {requires,    queue_recovery}]}).

-rabbit_boot_step({guid_generator,
                   [{description, "guid generator"},
                    {mfa,         {rabbit_sup, start_restartable_child,
                                   [rabbit_guid]}},
                    {requires,    persister},
                    {enables,     routing_ready}]}).

-rabbit_boot_step({routing_ready,
                   [{description, "message delivery logic ready"}]}).

-rabbit_boot_step({log_relay,
                   [{description, "error log relay"},
                    {mfa,         {rabbit_error_logger, boot, []}},
                    {requires,    routing_ready}]}).

-rabbit_boot_step({networking,
                   [{mfa,         {rabbit_networking, boot, []}},
                    {requires,    log_relay},
                    {enables,     networking_listening}]}).

-rabbit_boot_step({networking_listening,
                   [{description, "network listeners available"}]}).

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

-spec(prepare/0 :: () -> 'ok').
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
    try
        stop()
    after
        init:stop()
    end,
    ok.

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
    [ok = run_boot_step(Step) || Step <- boot_steps()],
    io:format("~nbroker running~n"),

    {ok, SupPid}.


stop(_State) ->
    terminated_ok = error_logger:delete_report_handler(rabbit_error_logger),
    ok = rabbit_alarm:stop(),
    ok = case rabbit_mnesia:is_clustered() of
             true  -> rabbit_amqqueue:on_node_down(node());
             false -> rabbit_mnesia:empty_ram_only_tables()
         end,
    ok.

%%---------------------------------------------------------------------------

boot_error(Format, Args) ->
    io:format("BOOT ERROR: " ++ Format, Args),
    error_logger:error_msg(Format, Args),
    timer:sleep(1000),
    exit({?MODULE, failure_during_boot}).

run_boot_step({StepName, Attributes}) ->
    Description = case lists:keysearch(description, 1, Attributes) of
                      {value, {_, D}} -> D;
                      false           -> StepName
                  end,
    case [MFA || {mfa, MFA} <- Attributes] of
        [] ->
            io:format("-- ~s~n", [Description]);
        MFAs ->
            io:format("starting ~-60s ...", [Description]),
            [case catch apply(M,F,A) of
                 {'EXIT', Reason} ->
                     boot_error("FAILED~nReason: ~p~n", [Reason]);
                 ok ->
                     ok
             end || {M,F,A} <- MFAs],
            io:format("done~n"),
            ok
    end.

boot_steps() ->
    AllApps = [App || {App, _, _} <- application:loaded_applications()],
    Modules = lists:usort(
                lists:append([Modules
                              || {ok, Modules} <-
                                     [application:get_key(App, modules)
                                      || App <- AllApps]])),
    UnsortedSteps =
        lists:flatmap(fun (Module) ->
                              [{StepName, Attributes}
                               || {rabbit_boot_step, [{StepName, Attributes}]}
                                      <- Module:module_info(attributes)]
                      end, Modules),
    sort_boot_steps(UnsortedSteps).

sort_boot_steps(UnsortedSteps) ->
    G = digraph:new([acyclic]),

    %% Add vertices, with duplicate checking.
    [case digraph:vertex(G, StepName) of
         false -> digraph:add_vertex(G, StepName, Step);
         _     -> boot_error("Duplicate boot step name: ~w~n", [StepName])
     end || Step = {StepName, _Attrs} <- UnsortedSteps],

    %% Add edges, detecting cycles and missing vertices.
    lists:foreach(fun ({StepName, Attributes}) ->
                          [add_boot_step_dep(G, StepName, PrecedingStepName)
                           || {requires, PrecedingStepName} <- Attributes],
                          [add_boot_step_dep(G, SucceedingStepName, StepName)
                           || {enables, SucceedingStepName} <- Attributes]
                  end, UnsortedSteps),

    %% Use topological sort to find a consistent ordering (if there is
    %% one, otherwise fail).
    SortedStepsRev = [begin
                          {StepName, Step} = digraph:vertex(G, StepName),
                          Step
                      end || StepName <- digraph_utils:topsort(G)],
    SortedSteps = lists:reverse(SortedStepsRev),

    digraph:delete(G),

    %% Check that all mentioned {M,F,A} triples are exported.
    case [{StepName, {M,F,A}}
          || {StepName, Attributes} <- SortedSteps,
             {mfa, {M,F,A}} <- Attributes,
             not erlang:function_exported(M, F, length(A))] of
        []               -> SortedSteps;
        MissingFunctions -> boot_error("Boot step functions not exported: ~p~n",
                                       [MissingFunctions])
    end.

add_boot_step_dep(G, RunsSecond, RunsFirst) ->
    case digraph:add_edge(G, RunsSecond, RunsFirst) of
        {error, Reason} ->
            boot_error("Could not add boot step dependency of ~w on ~w:~n~s",
              [RunsSecond, RunsFirst,
               case Reason of
                   {bad_vertex, V} ->
                       io_lib:format("Boot step not registered: ~w~n", [V]);
                   {bad_edge, [First | Rest]} ->
                       [io_lib:format("Cyclic dependency: ~w", [First]),
                        [io_lib:format(" depends on ~w", [Next])
                         || Next <- Rest],
                        io_lib:format(" depends on ~w~n", [First])]
               end]);
        _ ->
            ok
    end.

%%---------------------------------------------------------------------------

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
