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
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_mgmt_external_stats).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([info/1, info/2]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(REFRESH_RATIO, 5000).
-define(KEYS, [os_pid, mem_ets, mem_binary, fd_used, fd_total,
               sockets_used, sockets_total, mem_used, mem_limit, mem_alarm,
               proc_used, proc_total, statistics_level,
               erlang_version, uptime, run_queue, processors, exchange_types,
               auth_mechanisms, applications, contexts]).

%%--------------------------------------------------------------------

-record(state, {time_ms, fd_used, fd_total}).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

info(Node) ->
    info(Node, ?KEYS).

info(Node, Keys) ->
    try
        gen_server2:call({?MODULE, Node}, {info, Keys}, infinity)
    catch
        exit:{noproc, _} -> [{external_stats_not_running, true}]
    end.

%%--------------------------------------------------------------------

get_used_fd_lsof() ->
    case os:find_executable("lsof") of
        false -> unknown;
        Path  -> Cmd = Path ++ " -d \"0-9999999\" -lna -p " ++ os:getpid(),
                 string:words(os:cmd(Cmd), $\n) - 1
    end.

get_used_fd() ->
    get_used_fd(os:type()).

get_used_fd({unix, linux}) ->
    {ok, Files} = file:list_dir("/proc/" ++ os:getpid() ++ "/fd"),
    length(Files);

get_used_fd({unix, _}) ->
    get_used_fd_lsof();

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
    Handle = os:cmd("handle.exe /accepteula -s -p " ++ os:getpid() ++
                        " 2> nul"),
    case Handle of
        [] -> install_handle_from_sysinternals;
        _  -> find_files_line(string:tokens(Handle, "\r\n"))
    end;

get_used_fd(_) ->
    unknown.

find_files_line([]) ->
    unknown;
find_files_line(["  File " ++ Rest | _T]) ->
    [Files] = string:tokens(Rest, ": "),
    list_to_integer(Files);
find_files_line([_H | T]) ->
    find_files_line(T).

get_memory_limit() ->
    try
        vm_memory_monitor:get_memory_limit()
    catch exit:{noproc, _} -> memory_monitoring_disabled
    end.

%%--------------------------------------------------------------------

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(fd_used,  #state{fd_used  = FdUsed})  -> FdUsed;
i(fd_total, #state{fd_total = FdTotal}) -> FdTotal;
i(sockets_used,   _State) ->
    proplists:get_value(obtain_count, file_handle_cache:info([obtain_count]));
i(sockets_total,  _State) ->
    proplists:get_value(obtain_limit, file_handle_cache:info([obtain_limit]));
i(os_pid,         _State) -> list_to_binary(os:getpid());
i(mem_ets,        _State) -> erlang:memory(ets);
i(mem_binary,     _State) -> erlang:memory(binary);
i(mem_used,       _State) -> erlang:memory(total);
i(mem_limit,      _State) -> get_memory_limit();
i(proc_used,      _State) -> erlang:system_info(process_count);
i(proc_total,     _State) -> erlang:system_info(process_limit);
i(erlang_version, _State) -> list_to_binary(erlang:system_info(otp_release));
i(run_queue,      _State) -> erlang:statistics(run_queue);
i(processors,     _State) -> erlang:system_info(logical_processors);
i(uptime, _State) ->
    {Total, _} = erlang:statistics(wall_clock),
    Total;
i(statistics_level, _State) ->
    {ok, StatsLevel} = application:get_env(rabbit, collect_statistics),
    StatsLevel;
i(exchange_types, _State) ->
    list_registry_plugins(exchange);
i(auth_mechanisms, _State) ->
    {ok, Mechanisms} = application:get_env(rabbit, auth_mechanisms),
    list_registry_plugins(
      auth_mechanism,
      fun (N) -> lists:member(list_to_atom(binary_to_list(N)), Mechanisms) end);
i(applications, _State) ->
    [format_application(A) ||
        A <- lists:keysort(1, application:which_applications())];
i(mem_alarm, _State) -> lists:member({{vm_memory_high_watermark, node()}, []},
                                     alarm_handler:get_alarms());
i(contexts, _State) ->
    [format_context(C) || C <- rabbit_mochiweb_registry:list_all()].

list_registry_plugins(Type) ->
    list_registry_plugins(Type, fun(_) -> true end).

list_registry_plugins(Type, Fun) ->
    [registry_plugin_enabled(Module:description(), Fun) ||
        {_, Module} <- rabbit_registry:lookup_all(Type)].

registry_plugin_enabled(Desc, Fun) ->
    Desc ++ [{enabled, Fun(proplists:get_value(name, Desc))}].

format_application({Application, Description, Version}) ->
    [{name, Application},
     {description, list_to_binary(Description)},
     {version, list_to_binary(Version)}].

format_context({Path, Description, Rest}) ->
    DescPart = case Description of
                   none -> [];
                   _    -> [{description, list_to_binary(Description)}]
               end,
    DescPart ++ [{path, list_to_binary("/" ++ Path)}] ++ Rest.

%%--------------------------------------------------------------------

init([]) ->
    State = #state{fd_total   = file_handle_cache:ulimit()},
    {ok, internal_update(State)}.


handle_call({info, Items}, _From, State0) ->
    State = case (rabbit_misc:now_ms() - State0#state.time_ms >
                      ?REFRESH_RATIO) of
                true  -> internal_update(State0);
                false -> State0
            end,
    {reply, infos(Items, State), State};

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_C, State) ->
    {noreply, State}.

handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) -> ok.

code_change(_, State, _) -> {ok, State}.

%%--------------------------------------------------------------------

internal_update(State) ->
    State#state{time_ms   = rabbit_misc:now_ms(),
                fd_used   = get_used_fd()}.
