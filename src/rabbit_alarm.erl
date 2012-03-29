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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_alarm).

-behaviour(gen_event).

-export([start/0, stop/0, register/2, on_node_up/1, on_node_down/1]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-export([remote_conserve_resources/3]). %% Internal use only

-record(alarms, {alertees, alarmed_nodes}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> 'ok').
-spec(stop/0 :: () -> 'ok').
-spec(register/2 :: (pid(), rabbit_types:mfargs()) -> boolean()).
-spec(on_node_up/1 :: (node()) -> 'ok').
-spec(on_node_down/1 :: (node()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start() ->
    ok = alarm_handler:add_alarm_handler(?MODULE, []),
    {ok, MemoryWatermark} = application:get_env(vm_memory_high_watermark),
    rabbit_sup:start_restartable_child(vm_memory_monitor, [MemoryWatermark]),

    {ok, DiskLimit} = application:get_env(disk_free_limit),
    rabbit_sup:start_restartable_child(rabbit_disk_monitor, [DiskLimit]),
    ok.

stop() ->
    ok = alarm_handler:delete_alarm_handler(?MODULE).

register(Pid, HighMemMFA) ->
    gen_event:call(alarm_handler, ?MODULE,
                   {register, Pid, HighMemMFA},
                   infinity).

on_node_up(Node) -> gen_event:notify(alarm_handler, {node_up, Node}).

on_node_down(Node) -> gen_event:notify(alarm_handler, {node_down, Node}).

%% Can't use alarm_handler:{set,clear}_alarm because that doesn't
%% permit notifying a remote node.
remote_conserve_resources(Pid, Source, true) ->
    gen_event:notify({alarm_handler, node(Pid)},
                     {set_alarm, {{resource_limit, Source, node()}, []}});
remote_conserve_resources(Pid, Source, false) ->
    gen_event:notify({alarm_handler, node(Pid)},
                     {clear_alarm, {resource_limit, Source, node()}}).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #alarms{alertees      = dict:new(),
                 alarmed_nodes = dict:new()}}.

handle_call({register, Pid, HighMemMFA}, State) ->
    {ok, 0 < dict:size(State#alarms.alarmed_nodes),
     internal_register(Pid, HighMemMFA, State)};

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event({set_alarm, {{resource_limit, Source, Node}, []}}, State) ->
    {ok, maybe_alert(fun dict:append/3, Node, Source, State)};

handle_event({clear_alarm, {resource_limit, Source, Node}}, State) ->
    {ok, maybe_alert(fun dict_unappend/3, Node, Source, State)};

handle_event({node_up, Node}, State) ->
    %% Must do this via notify and not call to avoid possible deadlock.
    ok = gen_event:notify(
           {alarm_handler, Node},
           {register, self(), {?MODULE, remote_conserve_resources, []}}),
    {ok, State};

handle_event({node_down, Node}, State) ->
    {ok, maybe_alert(fun dict_unappend_all/3, Node, [], State)};

handle_event({register, Pid, HighMemMFA}, State) ->
    {ok, internal_register(Pid, HighMemMFA, State)};

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

alert_local(Alert, Alertees, _Source) ->
    alert(Alertees, [Alert], fun erlang:'=:='/2).

alert_remote(Alert, Alertees, Source) ->
    alert(Alertees, [Source, Alert], fun erlang:'=/='/2).

alert(Alertees, AlertArg, NodeComparator) ->
    Node = node(),
    dict:fold(fun (Pid, {M, F, A}, ok) ->
                      case NodeComparator(Node, node(Pid)) of
                          true  -> apply(M, F, A ++ [Pid] ++ AlertArg);
                          false -> ok
                      end
              end, ok, Alertees).

internal_register(Pid, {M, F, A} = HighMemMFA,
                  State = #alarms{alertees = Alertees}) ->
    _MRef = erlang:monitor(process, Pid),
    case dict:find(node(), State#alarms.alarmed_nodes) of
        {ok, _Sources} -> apply(M, F, A ++ [Pid, true]);
        error          -> ok
    end,
    NewAlertees = dict:store(Pid, HighMemMFA, Alertees),
    State#alarms{alertees = NewAlertees}.
