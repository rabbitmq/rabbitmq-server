%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_external_stats).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([list_registry_plugins/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(METRICS_KEYS, [fd_used, sockets_used, mem_used, disk_free, proc_used, gc_num,
                       gc_bytes_reclaimed, context_switches]).

-define(PERSISTER_KEYS, [persister_stats]).

-define(OTHER_KEYS, [name, partitions, os_pid, fd_total, sockets_total, mem_limit,
                     mem_alarm, disk_free_limit, disk_free_alarm, proc_total,
                     rates_mode, uptime, run_queue, processors, exchange_types,
                     auth_mechanisms, applications, contexts, log_files,
                     db_dir, config_files, net_ticktime, enabled_plugins,
                     mem_calculation_strategy, ra_open_file_metrics]).

-define(TEN_MINUTES_AS_SECONDS, 600).

%%--------------------------------------------------------------------

-record(state, {
    fd_total,
    fhc_stats,
    node_owners,
    interval,
    error_logged_time,
    fd_warning_logged
}).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------

get_used_fd(State0) ->
    try
        get_used_fd(os:type(), State0)
    catch
        _:Error ->
            State2 = log_fd_error("Could not infer the number of file handles used: ~tp", [Error], State0),
            {State2, 0}
    end.

-spec get_used_fd({atom(), atom()}, #state{}) -> {#state{}, non_neg_integer()}.
get_used_fd({unix, linux}, State0) ->
    case file:list_dir("/proc/" ++ os:getpid() ++ "/fd") of
        {ok, Files} ->
            {State0, length(Files)};
        {error, _}  ->
            get_used_fd({unix, generic}, State0)
    end;

get_used_fd({unix, BSD}, State0)
  when BSD == openbsd; BSD == freebsd; BSD == netbsd ->
    IsDigit = fun (D) -> lists:member(D, "0123456789*") end,
    Output = os:cmd("fstat -p " ++ os:getpid()),
    try
        F = fun (Line) ->
                    lists:all(IsDigit, lists:nth(4, string:tokens(Line, " ")))
            end,
        UsedFd = length(lists:filter(F, string:tokens(Output, "\n"))),
        {State0, UsedFd}
    catch _:Error:Stacktrace ->
              State1 = log_fd_error("Could not parse fstat output:~n~ts~n~tp",
                                    [Output, {Error, Stacktrace}], State0),
              {State1, 0}
    end;

get_used_fd({unix, _}, State0) ->
    Cmd = rabbit_misc:format(
            "lsof -d \"0-9999999\" -lna -p ~ts || echo failed", [os:getpid()]),
    Res = os:cmd(Cmd),
    case string:right(Res, 7) of
        "failed\n" ->
            State1 = log_fd_error("Could not obtain lsof output", [], State0),
            {State1, 0};
        _ ->
            UsedFd = string:words(Res, $\n) - 1,
            {State0, UsedFd}
    end;

%% handle.exe can be obtained from
%% https://learn.microsoft.com/en-us/sysinternals/downloads/handle
%% Note that the "File" number appears to include network sockets too; I assume
%% that's the number we care about. Note also that if you omit "-s" you will
%% see a list of file handles *without* network sockets. If you then add "-a"
%% you will see a list of handles of various types, including network sockets
%% shown as file handles to \Device\Afd.
get_used_fd({win32, _}, State0) ->
    Pid = os:getpid(),
    case os:find_executable("handle.exe") of
        false ->
            State1 = log_fd_warning_once("Could not find handle.exe, using powershell to determine handle count", [], State0),
            UsedFd = get_used_fd_via_powershell(Pid),
            {State1, UsedFd};
        HandleExe ->
            Args = ["/accepteula", "-s", "-p", Pid],
            {ok, HandleExeOutput} = rabbit_misc:win32_cmd(HandleExe, Args),
            case HandleExeOutput of
                [] ->
                    State1 = log_fd_warning_once("Could not execute handle.exe, using powershell to determine handle count", [], State0),
                    UsedFd = get_used_fd_via_powershell(Pid),
                    {State1, UsedFd};
                _  ->
                    case find_files_line(HandleExeOutput) of
                        unknown ->
                            State1 = log_fd_warning_once("handle.exe output did not contain "
                                                         "a line beginning with 'File', using "
                                                         "powershell to determine used file descriptor "
                                                         "count: ~tp", [HandleExeOutput], State0),
                            UsedFd = get_used_fd_via_powershell(Pid),
                            {State1, UsedFd};
                        UsedFd ->
                            {State0, UsedFd}
                    end
            end
    end.

find_files_line([]) ->
    unknown;
% Note:
% rabbit_misc:win32_cmd trims the output, so there will be no
% leading/trailing whitespace
find_files_line(["File " ++ Rest | _T]) ->
    [Files] = string:tokens(Rest, ": "),
    list_to_integer(Files);
find_files_line([_H | T]) ->
    find_files_line(T).

get_used_fd_via_powershell(Pid) ->
    Cmd = "Get-Process -Id " ++ Pid ++ " | Select-Object -ExpandProperty HandleCount",
    {ok, [Result]} = rabbit_misc:pwsh_cmd(Cmd),
    list_to_integer(Result).

-define(SAFE_CALL(Fun, NoProcFailResult),
    try
        Fun
    catch exit:{noproc, _} -> NoProcFailResult
    end).

get_disk_free_limit() -> ?SAFE_CALL(rabbit_disk_monitor:get_disk_free_limit(),
                                    disk_free_monitoring_disabled).

get_disk_free() -> ?SAFE_CALL(rabbit_disk_monitor:get_disk_free(),
                              disk_free_monitoring_disabled).

log_fd_warning_once(Fmt, Args, #state{fd_warning_logged = undefined}=State) ->
    % no warning has been logged, so log it and make a note of when
    ok = rabbit_log:warning(Fmt, Args),
    State#state{fd_warning_logged = true};
log_fd_warning_once(_Fmt, _Args, #state{fd_warning_logged = true}=State) ->
    State.

log_fd_error(Fmt, Args, #state{error_logged_time = undefined}=State) ->
    % rabbitmq/rabbitmq-management#90
    % no errors have been logged, so log it and make a note of when
    Now = erlang:monotonic_time(second),
    ok = rabbit_log:error(Fmt, Args),
    State#state{error_logged_time = Now};
log_fd_error(Fmt, Args, #state{error_logged_time = Time}=State) ->
    Now = erlang:monotonic_time(second),
    case Now >= Time + ?TEN_MINUTES_AS_SECONDS of
        true ->
            % rabbitmq/rabbitmq-management#90
            % it has been longer than 10 minutes,
            % re-log the error
            ok = rabbit_log:error(Fmt, Args),
            State#state{error_logged_time = Now};
        _ ->
            % 10 minutes have not yet passed
            State
    end.
%%--------------------------------------------------------------------

infos([], Acc, State) ->
    {State, lists:reverse(Acc)};
infos([Item|T], Acc0, State0) ->
    {State1, Infos} = i(Item, State0),
    Acc1 = [{Item, Infos}|Acc0],
    infos(T, Acc1, State1).

i(name, State) ->
    {State, node()};
i(partitions, State) ->
    {State, rabbit_node_monitor:partitions()};
i(fd_used, State) ->
    get_used_fd(State);
i(fd_total, #state{fd_total = FdTotal}=State) ->
    {State, FdTotal};
i(sockets_used, State) ->
    {State, proplists:get_value(sockets_used, file_handle_cache:info([sockets_used]))};
i(sockets_total, State) ->
    {State, proplists:get_value(sockets_limit, file_handle_cache:info([sockets_limit]))};
i(os_pid, State) ->
    {State, rabbit_data_coercion:to_utf8_binary(os:getpid())};
i(mem_used, State) ->
    {State, vm_memory_monitor:get_process_memory()};
i(mem_calculation_strategy, State) ->
    {State, vm_memory_monitor:get_memory_calculation_strategy()};
i(mem_limit, State) ->
    {State, vm_memory_monitor:get_memory_limit()};
i(mem_alarm, State) ->
    {State, resource_alarm_set(memory)};
i(proc_used, State) ->
    {State, erlang:system_info(process_count)};
i(proc_total, State) ->
    {State, erlang:system_info(process_limit)};
i(run_queue, State) ->
    {State, erlang:statistics(run_queue)};
i(processors, State) ->
    {State, erlang:system_info(logical_processors)};
i(disk_free_limit, State) ->
    {State, get_disk_free_limit()};
i(disk_free, State) ->
    {State, get_disk_free()};
i(disk_free_alarm, State) ->
    {State, resource_alarm_set(disk)};
i(contexts, State) ->
    {State, rabbit_web_dispatch_contexts()};
i(uptime, State) ->
    {Total, _} = erlang:statistics(wall_clock),
    {State, Total};
i(rates_mode, State) ->
    {State, rabbit_mgmt_db_handler:rates_mode()};
i(exchange_types, State) ->
    {State, list_registry_plugins(exchange)};
i(log_files, State) ->
    {State, [rabbit_data_coercion:to_utf8_binary(F) || F <- rabbit:log_locations()]};
i(db_dir, State) ->
    {State, rabbit_data_coercion:to_utf8_binary(rabbit:data_dir())};
i(config_files, State) ->
    {State, [rabbit_data_coercion:to_utf8_binary(F) || F <- rabbit:config_files()]};
i(net_ticktime, State) ->
    {State, net_kernel:get_net_ticktime()};
i(persister_stats, State) ->
    {State, persister_stats(State)};
i(enabled_plugins, State) ->
    {ok, Dir} = application:get_env(rabbit, enabled_plugins_file),
    {State, rabbit_plugins:read_enabled(Dir)};
i(auth_mechanisms, State) ->
    {ok, Mechanisms} = application:get_env(rabbit, auth_mechanisms),
    F = fun (N) ->
                lists:member(list_to_atom(binary_to_list(N)), Mechanisms)
        end,
    {State, list_registry_plugins(auth_mechanism, F)};
i(applications, State) ->
    {State, [format_application(A) || A <- lists:keysort(1, rabbit_misc:which_applications())]};
i(gc_num, State) ->
    {GCs, _, _} = erlang:statistics(garbage_collection),
    {State, GCs};
i(gc_bytes_reclaimed, State) ->
    {_, Words, _} = erlang:statistics(garbage_collection),
    {State, Words * erlang:system_info(wordsize)};
i(context_switches, State) ->
    {Sw, 0} = erlang:statistics(context_switches),
    {State, Sw};
i(ra_open_file_metrics, State) ->
    {State, [{ra_log_wal, ra_metrics(ra_log_wal)},
             {ra_log_segment_writer, ra_metrics(ra_log_segment_writer)}]}.

ra_metrics(K) ->
    try
        case ets:lookup(ra_open_file_metrics, whereis(K)) of
            [] -> 0;
            [{_, C}] -> C
        end
    catch
        error:badarg ->
            %% On startup the mgmt might start before ra does
            0
    end.

resource_alarm_set(Source) ->
    lists:member({{resource_limit, Source, node()},[]},
                 rabbit_alarm:get_alarms()).

list_registry_plugins(Type) ->
    list_registry_plugins(Type, fun(_) -> true end).

list_registry_plugins(Type, Fun) ->
    [registry_plugin_enabled(set_plugin_name(Name, Module), Fun) ||
        {Name, Module} <- rabbit_registry:lookup_all(Type)].

registry_plugin_enabled(Desc, Fun) ->
    Desc ++ [{enabled, Fun(proplists:get_value(name, Desc))}].

format_application({Application, Description, Version}) ->
    [{name, Application},
     {description, rabbit_data_coercion:to_utf8_binary(Description)},
     {version, rabbit_data_coercion:to_utf8_binary(Version)}].

set_plugin_name(Name, Module) ->
    [{name, atom_to_binary(Name, utf8)} |
     proplists:delete(name, Module:description())].

persister_stats(#state{fhc_stats = FHC}) ->
    [{flatten_key(K), V} || {{_Op, _Type} = K, V} <- FHC].

flatten_key({A, B}) ->
    list_to_atom(atom_to_list(A) ++ "_" ++ atom_to_list(B)).

cluster_links() ->
    rabbit_net:dist_info().

%%--------------------------------------------------------------------

%% This is slightly icky in that we introduce knowledge of
%% rabbit_web_dispatch, which is not a dependency. But the last thing I
%% want to do is create a rabbitmq_mochiweb_management_agent plugin.
rabbit_web_dispatch_contexts() ->
    [format_context(C) || C <- rabbit_web_dispatch_registry_list_all()].

%% For similar reasons we don't declare a dependency on
%% rabbitmq_mochiweb - so at startup there's no guarantee it will be
%% running. So we have to catch this noproc.
rabbit_web_dispatch_registry_list_all() ->
    case code:is_loaded(rabbit_web_dispatch_registry) of
        false -> [];
        _     -> try
                     M = rabbit_web_dispatch_registry, %% Fool xref
                     M:list_all()
                 catch exit:{noproc, _} ->
                         []
                 end
    end.

format_context({Path, Description, Rest}) ->
    [{description, rabbit_data_coercion:to_utf8_binary(Description)},
     {path,        rabbit_data_coercion:to_utf8_binary("/" ++ Path)} |
     format_mochiweb_option_list(Rest)].

format_mochiweb_option_list(C) ->
    [{K, format_mochiweb_option(K, V)} || {K, V} <- C].

format_mochiweb_option(ssl_opts, V) ->
    format_mochiweb_option_list(V);
format_mochiweb_option(_K, V) ->
    case io_lib:printable_unicode_list(V) of
        true  -> rabbit_data_coercion:to_utf8_binary(V);
        false -> rabbit_data_coercion:to_utf8_binary(rabbit_misc:format("~w", [V]))
    end.

%%--------------------------------------------------------------------

init([]) ->
    {ok, Interval}   = application:get_env(rabbit, collect_statistics_interval),
    State = #state{fd_total    = file_handle_cache:ulimit(),
                   fhc_stats   = get_fhc_stats(),
                   node_owners = sets:new(),
                   interval    = Interval},
    %% We can update stats straight away as they need to be available
    %% when the mgmt plugin starts a collector
    {ok, emit_update(State)}.

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_C, State) ->
    {noreply, State}.

handle_info(emit_update, State) ->
    {noreply, emit_update(State)};

handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) -> ok.

code_change(_, State, _) -> {ok, State}.

%%--------------------------------------------------------------------

emit_update(State0) ->
    State1 = update_state(State0),
    {State2, MStats} = infos(?METRICS_KEYS, [], State1),
    {State3, PStats} = infos(?PERSISTER_KEYS, [], State2),
    {State4, OStats} = infos(?OTHER_KEYS, [], State3),
    [{persister_stats, PStats0}] = PStats,
    [{name, _Name} | OStats0] = OStats,
    rabbit_core_metrics:node_stats(persister_metrics, PStats0),
    rabbit_core_metrics:node_stats(coarse_metrics, MStats),
    rabbit_core_metrics:node_stats(node_metrics, OStats0),
    rabbit_event:notify(node_stats, PStats ++ MStats ++ OStats),
    erlang:send_after(State4#state.interval, self(), emit_update),
    emit_node_node_stats(State4).

emit_node_node_stats(State = #state{node_owners = Owners}) ->
    Links = cluster_links(),
    NewOwners = sets:from_list([{Node, Owner} || {Node, Owner, _} <- Links]),
    Dead = sets:to_list(sets:subtract(Owners, NewOwners)),
    [rabbit_event:notify(
       node_node_deleted, [{route, Route}]) || {Node, _Owner} <- Dead,
                                                Route <- [{node(), Node},
                                                          {Node,   node()}]],
    [begin
         rabbit_core_metrics:node_node_stats({node(), Node}, Stats),
         rabbit_event:notify(
           node_node_stats, [{route, {node(), Node}} | Stats])
     end || {Node, _Owner, Stats} <- Links],
    State#state{node_owners = NewOwners}.

update_state(State0) ->
    %% Store raw data, the average operation time is calculated during querying
    %% from the accumulated total
    FHC = get_fhc_stats(),
    State0#state{fhc_stats = FHC}.

get_fhc_stats() ->
    dict:to_list(dict:merge(fun(_, V1, V2) -> V1 + V2 end,
                            dict:from_list(file_handle_cache_stats:get()),
                            dict:from_list(get_ra_io_metrics()))).

get_ra_io_metrics() ->
    lists:sort(ets:tab2list(ra_io_metrics)).
