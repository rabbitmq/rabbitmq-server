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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_external_stats).

%% Transitional step until we can require Erlang/OTP 21 and
%% use the now recommended try/catch syntax for obtaining the stack trace.
-compile(nowarn_deprecated_function).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([list_registry_plugins/1]).

-import(rabbit_misc, [pget/2]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(METRICS_KEYS, [fd_used, sockets_used, mem_used, disk_free, proc_used, gc_num,
                       gc_bytes_reclaimed, context_switches]).

-define(PERSISTER_KEYS, [persister_stats]).

-define(OTHER_KEYS, [name, partitions, os_pid, fd_total, sockets_total, mem_limit,
                     mem_alarm, disk_free_limit, disk_free_alarm, proc_total,
                     rates_mode, uptime, run_queue, processors, exchange_types,
                     auth_mechanisms, applications, contexts, log_files,
                     db_dir, config_files, net_ticktime, enabled_plugins,
                     mem_calculation_strategy]).

%%--------------------------------------------------------------------

-record(state, {
    fd_total,
    fhc_stats,
    node_owners,
    last_ts,
    interval
}).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------

get_used_fd() ->
    case get_used_fd(os:type()) of
        Fd when is_number(Fd) ->
            Fd;
        _Other ->
            %% Defaults to 0 if data is not available
            0
    end.

get_used_fd({unix, linux}) ->
    case file:list_dir("/proc/" ++ os:getpid() ++ "/fd") of
        {ok, Files} -> length(Files);
        {error, _}  -> get_used_fd({unix, generic})
    end;

get_used_fd({unix, BSD})
  when BSD == openbsd; BSD == freebsd; BSD == netbsd ->
    Digit = fun (D) -> lists:member(D, "0123456789*") end,
    Output = os:cmd("fstat -p " ++ os:getpid()),
    try
        length(
          lists:filter(
            fun (Line) ->
                    lists:all(Digit, (lists:nth(4, string:tokens(Line, " "))))
            end, string:tokens(Output, "\n")))
    catch _:Error ->
            log_fd_error("Could not parse fstat output:~n~s~n~p~n",
                         [Output, {Error, erlang:get_stacktrace()}])
    end;

get_used_fd({unix, _}) ->
    Cmd = rabbit_misc:format(
            "lsof -d \"0-9999999\" -lna -p ~s || echo failed", [os:getpid()]),
    Res = os:cmd(Cmd),
    case string:right(Res, 7) of
        "failed\n" -> log_fd_error("Could not obtain lsof output~n", []);
        _          -> string:words(Res, $\n) - 1
    end;

%% handle.exe can be obtained from
%% http://technet.microsoft.com/en-us/sysinternals/bb896655.aspx

%% Output looks like:

%% Handle v3.42
%% Copyright (C) 1997-2008 Mark Russinovich
%% Sysinternals - www.sysinternals.com
%%
%% Handle type summary:
%%   ALPC Port       : 2
%%   Desktop         : 1
%%   Directory       : 1
%%   Event           : 108
%%   File            : 25
%%   IoCompletion    : 3
%%   Key             : 7
%%   KeyedEvent      : 1
%%   Mutant          : 1
%%   Process         : 3
%%   Process         : 38
%%   Thread          : 41
%%   Timer           : 3
%%   TpWorkerFactory : 2
%%   WindowStation   : 2
%% Total handles: 238

%% Note that the "File" number appears to include network sockets too; I assume
%% that's the number we care about. Note also that if you omit "-s" you will
%% see a list of file handles *without* network sockets. If you then add "-a"
%% you will see a list of handles of various types, including network sockets
%% shown as file handles to \Device\Afd.

get_used_fd({win32, _}) ->
    Handle = rabbit_misc:os_cmd(
               "handle.exe /accepteula -s -p " ++ os:getpid() ++ " 2> nul"),
    case Handle of
        [] -> log_fd_error("Could not find handle.exe, please install from "
                           "sysinternals~n", []);
        _  -> case find_files_line(string:tokens(Handle, "\r\n")) of
                  unknown ->
                      log_fd_error("handle.exe output did not contain "
                                   "a line beginning with '  File ', unable "
                                   "to determine used file descriptor "
                                   "count: ~p~n", [Handle]);
                  Any ->
                      Any
              end
    end.

find_files_line([]) ->
    unknown;
find_files_line(["  File " ++ Rest | _T]) ->
    [Files] = string:tokens(Rest, ": "),
    list_to_integer(Files);
find_files_line([_H | T]) ->
    find_files_line(T).

-define(SAFE_CALL(Fun, NoProcFailResult),
    try
        Fun
    catch exit:{noproc, _} -> NoProcFailResult
    end).

get_disk_free_limit() -> ?SAFE_CALL(rabbit_disk_monitor:get_disk_free_limit(),
                                    disk_free_monitoring_disabled).

get_disk_free() -> ?SAFE_CALL(rabbit_disk_monitor:get_disk_free(),
                              disk_free_monitoring_disabled).

log_fd_error(Fmt, Args) ->
    case get(logged_used_fd_error) of
        undefined -> rabbit_log:warning(Fmt, Args),
                     put(logged_used_fd_error, true);
        _         -> ok
    end.
%%--------------------------------------------------------------------

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(name,            _State) -> node();
i(partitions,      _State) -> rabbit_node_monitor:partitions();
i(fd_used,         _State) -> get_used_fd();
i(fd_total, #state{fd_total = FdTotal}) -> FdTotal;
i(sockets_used,    _State) ->
    proplists:get_value(sockets_used, file_handle_cache:info([sockets_used]));
i(sockets_total,   _State) ->
    proplists:get_value(sockets_limit, file_handle_cache:info([sockets_limit]));
i(os_pid,          _State) -> list_to_binary(os:getpid());

i(mem_used,        _State) -> vm_memory_monitor:get_process_memory();
i(mem_calculation_strategy, _State) -> vm_memory_monitor:get_memory_calculation_strategy();
i(mem_limit,       _State) -> vm_memory_monitor:get_memory_limit();
i(mem_alarm,       _State) -> resource_alarm_set(memory);
i(proc_used,       _State) -> erlang:system_info(process_count);
i(proc_total,      _State) -> erlang:system_info(process_limit);
i(run_queue,       _State) -> erlang:statistics(run_queue);
i(processors,      _State) -> erlang:system_info(logical_processors);
i(disk_free_limit, _State) -> get_disk_free_limit();
i(disk_free,       _State) -> get_disk_free();
i(disk_free_alarm, _State) -> resource_alarm_set(disk);
i(contexts,        _State) -> rabbit_web_dispatch_contexts();
i(uptime,          _State) -> {Total, _} = erlang:statistics(wall_clock),
                                Total;
i(rates_mode,      _State) -> rabbit_mgmt_db_handler:rates_mode();
i(exchange_types,  _State) -> list_registry_plugins(exchange);
i(log_files,       _State) -> [list_to_binary(F) || F <- rabbit:log_locations()];
i(db_dir,          _State) -> list_to_binary(rabbit_mnesia:dir());
i(config_files,    _State) -> [list_to_binary(F) || F <- rabbit:config_files()];
i(net_ticktime,    _State) -> net_kernel:get_net_ticktime();
i(persister_stats,  State) -> persister_stats(State);
i(enabled_plugins, _State) -> {ok, Dir} = application:get_env(
                                           rabbit, enabled_plugins_file),
                              rabbit_plugins:read_enabled(Dir);
i(auth_mechanisms, _State) ->
    {ok, Mechanisms} = application:get_env(rabbit, auth_mechanisms),
    list_registry_plugins(
      auth_mechanism,
      fun (N) -> lists:member(list_to_atom(binary_to_list(N)), Mechanisms) end);
i(applications,    _State) ->
    [format_application(A) ||
        A <- lists:keysort(1, rabbit_misc:which_applications())];
i(gc_num, _State) ->
    {GCs, _, _} = erlang:statistics(garbage_collection),
    GCs;
i(gc_bytes_reclaimed, _State) ->
    {_, Words, _} = erlang:statistics(garbage_collection),
    Words * erlang:system_info(wordsize);
i(context_switches, _State) ->
    {Sw, 0} = erlang:statistics(context_switches),
    Sw.

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
     {description, list_to_binary(Description)},
     {version, list_to_binary(Version)}].

set_plugin_name(Name, Module) ->
    [{name, list_to_binary(atom_to_list(Name))} |
     proplists:delete(name, Module:description())].

persister_stats(#state{fhc_stats = FHC}) ->
    [{flatten_key(K), V} || {{_Op, _Type} = K, V} <- FHC].

flatten_key({A, B}) ->
    list_to_atom(atom_to_list(A) ++ "_" ++ atom_to_list(B)).

cluster_links() ->
    {ok, Items} = net_kernel:nodes_info(),
    [Link || Item <- Items,
             Link <- [format_nodes_info(Item)], Link =/= undefined].

format_nodes_info({Node, Info}) ->
    Owner = proplists:get_value(owner, Info),
    case catch process_info(Owner, links) of
        {links, Links} ->
            case [Link || Link <- Links, is_port(Link)] of
                [Port] ->
                    {Node, Owner, format_nodes_info1(Port)};
                _ ->
                    undefined
            end;
        _ ->
            undefined
    end.

format_nodes_info1(Port) ->
    case {rabbit_net:socket_ends(Port, inbound),
          rabbit_net:getstat(Port, [recv_oct, send_oct])} of
        {{ok, {PeerAddr, PeerPort, SockAddr, SockPort}}, {ok, Stats}} ->
            [{peer_addr, maybe_ntoab(PeerAddr)},
             {peer_port, PeerPort},
             {sock_addr, maybe_ntoab(SockAddr)},
             {sock_port, SockPort},
             {recv_bytes, pget(recv_oct, Stats)},
             {send_bytes, pget(send_oct, Stats)}];
        _ ->
            []
    end.

maybe_ntoab(A) when is_tuple(A) -> list_to_binary(rabbit_misc:ntoab(A));
maybe_ntoab(H)                  -> H.

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
    [{description, list_to_binary(Description)},
     {path,        list_to_binary("/" ++ Path)} |
     format_mochiweb_option_list(Rest)].

format_mochiweb_option_list(C) ->
    [{K, format_mochiweb_option(K, V)} || {K, V} <- C].

format_mochiweb_option(ssl_opts, V) ->
    format_mochiweb_option_list(V);
format_mochiweb_option(_K, V) ->
    case io_lib:printable_list(V) of
        true  -> list_to_binary(V);
        false -> list_to_binary(rabbit_misc:format("~w", [V]))
    end.

%%--------------------------------------------------------------------

init([]) ->
    {ok, Interval}   = application:get_env(rabbit, collect_statistics_interval),
    State = #state{fd_total    = file_handle_cache:ulimit(),
                   fhc_stats   = file_handle_cache_stats:get(),
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
    State = update_state(State0),
    MStats = infos(?METRICS_KEYS, State),
    [{persister_stats, PStats0}] = PStats = infos(?PERSISTER_KEYS, State),
    [{name, _Name} | OStats0] = OStats = infos(?OTHER_KEYS, State),
    rabbit_core_metrics:node_stats(persister_metrics, PStats0),
    rabbit_core_metrics:node_stats(coarse_metrics, MStats),
    rabbit_core_metrics:node_stats(node_metrics, OStats0),
    rabbit_event:notify(node_stats, PStats ++ MStats ++ OStats),
    erlang:send_after(State#state.interval, self(), emit_update),
    emit_node_node_stats(State).

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
    FHC = file_handle_cache_stats:get(),
    State0#state{fhc_stats = FHC}.
