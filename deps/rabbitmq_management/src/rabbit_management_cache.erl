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
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2009 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_management_cache).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([update/0, info/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(REFRESH_RATIO, 5000).


%%--------------------------------------------------------------------

-record(state, {
        time_ms,
        datetime,
        bound_to,
        fd_used,
        fd_total,
        mem_used,
        mem_total,
        proc_used,
        proc_total
        }).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

info(Items, Timeout) ->
    gen_server2:call(?MODULE, {info, Items}, Timeout).

% By default, let's not wait too long. If that takes more than 1 second,
% it's better to quickly return 408 "request timeout" rather than hang.
info(Items) ->
    info(Items, 1000).

update() ->
    gen_server2:cast(?MODULE, update).

%%--------------------------------------------------------------------

%% TODO Windows?

get_total_fd_ulimit() ->
    {MaxFds, _} = string:to_integer(os:cmd("ulimit -n")),
    MaxFds.

get_total_fd() ->
    get_total_fd(os:type()).

get_total_fd({unix, Os}) when Os =:= linux
                       orelse Os =:= darwin
                       orelse Os =:= freebsd
                       orelse Os =:= sunos ->
    get_total_fd_ulimit();

get_total_fd(_) ->
    unknown.


get_used_fd_lsof() ->
    Lsof = os:cmd("lsof -d \"0-9999999\" -lna -p " ++ os:getpid()),
    string:words(Lsof, $\n).

get_used_fd() ->
    get_used_fd(os:type()).

get_used_fd({unix, Os}) when Os =:= linux
                      orelse Os =:= darwin
                      orelse Os =:= freebsd ->
    get_used_fd_lsof();


get_used_fd(_) ->
    unknown.


get_total_memory() ->
    vm_memory_monitor:get_vm_memory_high_watermark() *
	vm_memory_monitor:get_total_memory().


%%--------------------------------------------------------------------

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(datetime,    #state{datetime = DateTime})       -> DateTime;
i(bound_to,    #state{bound_to = BoundTo})        -> BoundTo;
i(fd_used,     #state{fd_used = FdUsed})          -> FdUsed;
i(fd_total,    #state{fd_total = FdTotal})        -> FdTotal;
i(mem_used,    #state{mem_used = MemUsed})        -> MemUsed;
i(mem_total,   #state{mem_total = MemTotal})      -> MemTotal;
i(proc_used,   #state{proc_used = ProcUsed})      -> ProcUsed;
i(proc_total,  #state{proc_total = ProcTotal})    -> ProcTotal.

%%--------------------------------------------------------------------

init([]) ->
    {ok, Binds} = application:get_env(rabbit, tcp_listeners),
    BoundTo = lists:flatten( [ status_render:print("~s:~p ", [Addr,Port])
                                                || {Addr, Port} <- Binds ] ),
    State = #state{
      fd_total = get_total_fd(),
      mem_total = get_total_memory(),
      proc_total = erlang:system_info(process_limit),
      bound_to = BoundTo
     },
    {ok, internal_update(State)}.


handle_call({info, Items}, _From, State0) ->
    State = case rabbit_management_util:now_ms() - State0#state.time_ms >
                ?REFRESH_RATIO of
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
    State#state{
        time_ms = rabbit_management_util:now_ms(),
        datetime = rabbit_management_util:http_date(),
        fd_used = get_used_fd(),
        mem_used = erlang:memory(total),
        proc_used = erlang:system_info(process_count)
    }.


