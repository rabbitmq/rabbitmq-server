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

%% TODO this is vestigal from the status plugin. Do we need a caching
%% mechanism for the os-level info this returns?

%% TODO rename mem_total to mem_limit

-module(rabbit_mgmt_external_stats).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([update/0, info/0]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(REFRESH_RATIO, 5000).
-define(KEYS, [bound_to, fd_used, fd_total,
               mem_used, mem_total, proc_used, proc_total]).

%%--------------------------------------------------------------------

-record(state, {time_ms, bound_to, fd_used, fd_total,
                mem_used, mem_total, proc_used, proc_total}).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

info() ->
    gen_server2:call(?MODULE, {info, ?KEYS}, infinity).

update() ->
    gen_server2:cast(?MODULE, update).

%%--------------------------------------------------------------------

get_used_fd_lsof() ->
    Lsof = os:cmd("lsof -d \"0-9999999\" -lna -p " ++ os:getpid()),
    string:words(Lsof, $\n).

get_used_fd() ->
    get_used_fd(os:type()).

get_used_fd({unix, linux}) ->
    {ok, Files} = file:list_dir("/proc/" ++ os:getpid() ++ "/fd"),
    length(Files);

get_used_fd({unix, Os}) when Os =:= darwin
                      orelse Os =:= freebsd ->
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

get_total_memory() ->
    try
        vm_memory_monitor:get_memory_limit()
    catch exit:{noproc, _} -> memory_monitoring_disabled
    end.

%%--------------------------------------------------------------------

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(bound_to,    #state{bound_to   = BoundTo})   -> BoundTo;
i(fd_used,     #state{fd_used    = FdUsed})    -> FdUsed;
i(fd_total,    #state{fd_total   = FdTotal})   -> FdTotal;
i(mem_used,    #state{mem_used   = MemUsed})   -> MemUsed;
i(mem_total,   #state{mem_total  = MemTotal})  -> MemTotal;
i(proc_used,   #state{proc_used  = ProcUsed})  -> ProcUsed;
i(proc_total,  #state{proc_total = ProcTotal}) -> ProcTotal.

%%--------------------------------------------------------------------

init([]) ->
    %% TODO obtain this information dynamically from
    %% rabbit_networking:active_listeners(), or ditch it.
    {ok, Binds} = application:get_env(rabbit, tcp_listeners),
    BoundTo = lists:flatten(
                [rabbit_mgmt_format:print("~s:~p", [Addr,Port]) ||
                    {Addr, Port} <- Binds]),
    State = #state{fd_total   = file_handle_cache:ulimit(),
                   mem_total  = get_total_memory(),
                   proc_total = erlang:system_info(process_limit),
                   bound_to   = BoundTo},
    {ok, internal_update(State)}.


handle_call({info, Items}, _From, State0) ->
    State = case (rabbit_mgmt_util:now_ms() - State0#state.time_ms >
                      ?REFRESH_RATIO) of
                true  -> internal_update(State0);
                false -> State0
            end,
    {reply, infos(Items, State), State};

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.


handle_cast(update, State) ->
    {noreply, internal_update(State)};

handle_cast(_C, State) ->
    {noreply, State}.


handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) -> ok.

code_change(_, State, _) -> {ok, State}.

%%--------------------------------------------------------------------

internal_update(State) ->
    State#state{time_ms   = rabbit_mgmt_util:now_ms(),
                fd_used   = get_used_fd(),
                mem_used  = erlang:memory(total),
                proc_used = erlang:system_info(process_count)}.
