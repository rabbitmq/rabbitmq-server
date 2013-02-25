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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_alarm).

-behaviour(gen_event).

-export([start_link/0, start/0, stop/0, register/2, set_alarm/1,
         clear_alarm/1, get_alarms/0, on_node_up/1, on_node_down/1]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-export([remote_conserve_resources/3]). %% Internal use only

-define(SERVER, ?MODULE).

-record(alarms, {alertees, alarmed_nodes, alarms}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(start/0 :: () -> 'ok').
-spec(stop/0 :: () -> 'ok').
-spec(register/2 :: (pid(), rabbit_types:mfargs()) -> boolean()).
-spec(set_alarm/1 :: (any()) -> 'ok').
-spec(clear_alarm/1 :: (any()) -> 'ok').
-spec(on_node_up/1 :: (node()) -> 'ok').
-spec(on_node_down/1 :: (node()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_event:start_link({local, ?SERVER}).

start() ->
    ok = rabbit_sup:start_restartable_child(?MODULE),
    ok = gen_event:add_handler(?SERVER, ?MODULE, []),
    {ok, MemoryWatermark} = application:get_env(vm_memory_high_watermark),
    rabbit_sup:start_restartable_child(
      vm_memory_monitor, [MemoryWatermark,
                          fun (Alarm) ->
                                  background_gc:run(),
                                  set_alarm(Alarm)
                          end,
                          fun clear_alarm/1]),
    {ok, DiskLimit} = application:get_env(disk_free_limit),
    rabbit_sup:start_restartable_child(rabbit_disk_monitor, [DiskLimit]),
    ok.

stop() -> ok.

register(Pid, AlertMFA) ->
    gen_event:call(?SERVER, ?MODULE, {register, Pid, AlertMFA}, infinity).

set_alarm(Alarm)   -> gen_event:notify(?SERVER, {set_alarm,   Alarm}).
clear_alarm(Alarm) -> gen_event:notify(?SERVER, {clear_alarm, Alarm}).

get_alarms() -> gen_event:call(?SERVER, ?MODULE, get_alarms, infinity).

on_node_up(Node)   -> gen_event:notify(?SERVER, {node_up,   Node}).
on_node_down(Node) -> gen_event:notify(?SERVER, {node_down, Node}).

remote_conserve_resources(Pid, Source, true) ->
    gen_event:notify({?SERVER, node(Pid)},
                     {set_alarm, {{resource_limit, Source, node()}, []}});
remote_conserve_resources(Pid, Source, false) ->
    gen_event:notify({?SERVER, node(Pid)},
                     {clear_alarm, {resource_limit, Source, node()}}).


%%----------------------------------------------------------------------------

init([]) ->
    {ok, #alarms{alertees      = dict:new(),
                 alarmed_nodes = dict:new(),
                 alarms        = []}}.

handle_call({register, Pid, AlertMFA}, State) ->
    {ok, 0 < dict:size(State#alarms.alarmed_nodes),
     internal_register(Pid, AlertMFA, State)};

handle_call(get_alarms, State = #alarms{alarms = Alarms}) ->
    {ok, Alarms, State};

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event({set_alarm, Alarm}, State = #alarms{alarms = Alarms}) ->
    handle_set_alarm(Alarm, State#alarms{alarms = [Alarm|Alarms]});

handle_event({clear_alarm, Alarm}, State = #alarms{alarms = Alarms}) ->
    handle_clear_alarm(Alarm, State#alarms{alarms = lists:keydelete(Alarm, 1,
                                                                    Alarms)});

handle_event({node_up, Node}, State) ->
    %% Must do this via notify and not call to avoid possible deadlock.
    ok = gen_event:notify(
           {?SERVER, Node},
           {register, self(), {?MODULE, remote_conserve_resources, []}}),
    {ok, State};

handle_event({node_down, Node}, State) ->
    {ok, maybe_alert(fun dict_unappend_all/3, Node, [], State)};

handle_event({register, Pid, AlertMFA}, State) ->
    {ok, internal_register(Pid, AlertMFA, State)};

handle_event(_Event, State) ->
    {ok, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #alarms{alertees = Alertees}) ->
    {ok, State#alarms{alertees = dict:erase(Pid, Alertees)}};

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

dict_unappend_all(Key, _Val, Dict) ->
    dict:erase(Key, Dict).

dict_unappend(Key, Val, Dict) ->
    case lists:delete(Val, dict:fetch(Key, Dict)) of
        [] -> dict:erase(Key, Dict);
        X  -> dict:store(Key, X, Dict)
    end.

count_dict_values(Val, Dict) ->
    dict:fold(fun (_Node, List, Count) ->
                  Count + case lists:member(Val, List) of
                              true  -> 1;
                              false -> 0
                          end
              end, 0, Dict).

maybe_alert(UpdateFun, Node, Source,
            State = #alarms{alarmed_nodes = AN,
                            alertees      = Alertees}) ->
    AN1 = UpdateFun(Node, Source, AN),
    BeforeSz = count_dict_values(Source, AN),
    AfterSz  = count_dict_values(Source, AN1),

    %% If we have changed our alarm state, inform the remotes.
    IsLocal = Node =:= node(),
    if IsLocal andalso BeforeSz < AfterSz ->
           ok = alert_remote(true,  Alertees, Source);
       IsLocal andalso BeforeSz > AfterSz ->
           ok = alert_remote(false, Alertees, Source);
       true                               ->
           ok
    end,
    %% If the overall alarm state has changed, inform the locals.
    case {dict:size(AN), dict:size(AN1)} of
        {0, 1} -> ok = alert_local(true,  Alertees, Source);
        {1, 0} -> ok = alert_local(false, Alertees, Source);
        {_, _} -> ok
    end,
    State#alarms{alarmed_nodes = AN1}.

alert_local(Alert, Alertees, Source) ->
    alert(Alertees, Source, Alert, fun erlang:'=:='/2).

alert_remote(Alert, Alertees, Source) ->
    alert(Alertees, Source, Alert, fun erlang:'=/='/2).

alert(Alertees, Source, Alert, NodeComparator) ->
    Node = node(),
    dict:fold(fun (Pid, {M, F, A}, ok) ->
                      case NodeComparator(Node, node(Pid)) of
                          true  -> apply(M, F, A ++ [Pid, Source, Alert]);
                          false -> ok
                      end
              end, ok, Alertees).

internal_register(Pid, {M, F, A} = AlertMFA,
                  State = #alarms{alertees = Alertees}) ->
    _MRef = erlang:monitor(process, Pid),
    case dict:find(node(), State#alarms.alarmed_nodes) of
        {ok, Sources} -> [apply(M, F, A ++ [Pid, R, true]) || R <- Sources];
        error          -> ok
    end,
    NewAlertees = dict:store(Pid, AlertMFA, Alertees),
    State#alarms{alertees = NewAlertees}.

handle_set_alarm({{resource_limit, Source, Node}, []}, State) ->
    rabbit_log:warning("~s resource limit alarm set on node ~p~n",
                       [Source, Node]),
    {ok, maybe_alert(fun dict:append/3, Node, Source, State)};
handle_set_alarm({file_descriptor_limit, []}, State) ->
    rabbit_log:warning("file descriptor limit alarm set~n"),
    {ok, State};
handle_set_alarm(Alarm, State) ->
    rabbit_log:warning("alarm '~p' set~n", [Alarm]),
    {ok, State}.

handle_clear_alarm({resource_limit, Source, Node}, State) ->
    rabbit_log:warning("~s resource limit alarm cleared on node ~p~n",
                       [Source, Node]),
    {ok, maybe_alert(fun dict_unappend/3, Node, Source, State)};
handle_clear_alarm(file_descriptor_limit, State) ->
    rabbit_log:warning("file descriptor limit alarm cleared~n"),
    {ok, State};
handle_clear_alarm(Alarm, State) ->
    rabbit_log:warning("alarm '~p' cleared~n", [Alarm]),
    {ok, State}.
