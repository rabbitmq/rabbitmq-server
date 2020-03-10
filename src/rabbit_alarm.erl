%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% There are two types of alarms handled by this module:
%%
%% * per-node resource (disk, memory) alarms for the whole cluster. If any node
%%   has an alarm, then all publishing should be disabled across the
%%   cluster until all alarms clear. When a node sets such an alarm,
%%   this information is automatically propagated throughout the cluster.
%%   `#alarms.alarmed_nodes' is being used to track this type of alarms.
%% * limits local to this node (file_descriptor_limit). Used for information
%%   purposes only: logging and getting node status. This information is not propagated
%%   throughout the cluster. `#alarms.alarms' is being used to track this type of alarms.
%% @end

-module(rabbit_alarm).

-behaviour(gen_event).

-export([start_link/0, start/0, stop/0, register/2, set_alarm/1,
         clear_alarm/1, get_alarms/0, on_node_up/1, on_node_down/1]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-export([remote_conserve_resources/3]). %% Internal use only

-define(SERVER, ?MODULE).



%%----------------------------------------------------------------------------

-record(alarms, {alertees :: dict:dict(pid(), rabbit_types:mfargs()),
                 alarmed_nodes :: dict:dict(node(), [resource_alarm_source()]),
                 alarms :: [alarm()]}).

-type local_alarm() :: 'file_descriptor_limit'.
-type resource_alarm_source() :: 'disk' | 'memory'.
-type resource_alarm() :: {resource_limit, resource_alarm_source(), node()}.
-type alarm() :: local_alarm() | resource_alarm().

%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().

start_link() ->
    gen_event:start_link({local, ?SERVER}).

-spec start() -> 'ok'.

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
    rabbit_sup:start_delayed_restartable_child(
      rabbit_disk_monitor, [DiskLimit]),
    ok.

-spec stop() -> 'ok'.

stop() -> ok.

%% Registers a handler that should be called on every resource alarm change.
%% Given a call rabbit_alarm:register(Pid, {M, F, A}), the handler would be
%% called like this: `apply(M, F, A ++ [Pid, Source, Alert])', where `Source'
%% has the type of resource_alarm_source() and `Alert' has the type of resource_alert().

-spec register(pid(), rabbit_types:mfargs()) -> [atom()].

register(Pid, AlertMFA) ->
    gen_event:call(?SERVER, ?MODULE, {register, Pid, AlertMFA}, infinity).

-spec set_alarm({alarm(), []}) -> 'ok'.

set_alarm(Alarm)   -> gen_event:notify(?SERVER, {set_alarm,   Alarm}).

-spec clear_alarm(alarm()) -> 'ok'.

clear_alarm(Alarm) -> gen_event:notify(?SERVER, {clear_alarm, Alarm}).

-spec get_alarms() -> [{alarm(), []}].

get_alarms() -> gen_event:call(?SERVER, ?MODULE, get_alarms, infinity).

-spec on_node_up(node()) -> 'ok'.

on_node_up(Node)   -> gen_event:notify(?SERVER, {node_up,   Node}).

-spec on_node_down(node()) -> 'ok'.

on_node_down(Node) -> gen_event:notify(?SERVER, {node_down, Node}).

remote_conserve_resources(Pid, Source, {true, _, _}) ->
    gen_event:notify({?SERVER, node(Pid)},
                     {set_alarm, {{resource_limit, Source, node()}, []}});
remote_conserve_resources(Pid, Source, {false, _, _}) ->
    gen_event:notify({?SERVER, node(Pid)},
                     {clear_alarm, {resource_limit, Source, node()}}).


%%----------------------------------------------------------------------------

init([]) ->
    {ok, #alarms{alertees      = dict:new(),
                 alarmed_nodes = dict:new(),
                 alarms        = []}}.

handle_call({register, Pid, AlertMFA}, State = #alarms{alarmed_nodes = AN}) ->
    {ok, lists:usort(lists:append([V || {_, V} <- dict:to_list(AN)])),
     internal_register(Pid, AlertMFA, State)};

handle_call(get_alarms, State) ->
    {ok, get_alarms(State), State};

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event({set_alarm, {{resource_limit, Source, Node}, []}}, State) ->
    case is_node_alarmed(Source, Node, State) of
        true ->
            {ok, State};
        false ->
            rabbit_event:notify(alarm_set, [{source, Source},
                                            {node, Node}]),
            handle_set_resource_alarm(Source, Node, State)
    end;
handle_event({set_alarm, Alarm}, State = #alarms{alarms = Alarms}) ->
    case lists:member(Alarm, Alarms) of
        true  -> {ok, State};
        false -> UpdatedAlarms = lists:usort([Alarm|Alarms]),
                 handle_set_alarm(Alarm, State#alarms{alarms = UpdatedAlarms})
    end;

handle_event({clear_alarm, {resource_limit, Source, Node}}, State) ->
    case is_node_alarmed(Source, Node, State) of
        true  ->
            rabbit_event:notify(alarm_cleared, [{source, Source},
                                                {node, Node}]),
            handle_clear_resource_alarm(Source, Node, State);
        false ->
            {ok, State}
    end;
handle_event({clear_alarm, Alarm}, State = #alarms{alarms = Alarms}) ->
    case lists:keymember(Alarm, 1, Alarms) of
        true  -> handle_clear_alarm(
                   Alarm, State#alarms{alarms = lists:keydelete(
                                                  Alarm, 1, Alarms)});
        false -> {ok, State}

    end;

handle_event({node_up, Node}, State) ->
    %% Must do this via notify and not call to avoid possible deadlock.
    ok = gen_event:notify(
           {?SERVER, Node},
           {register, self(), {?MODULE, remote_conserve_resources, []}}),
    {ok, State};

handle_event({node_down, Node}, #alarms{alarmed_nodes = AN} = State) ->
    AlarmsForDeadNode = case dict:find(Node, AN) of
                            {ok, V} -> V;
                            error   -> []
                        end,
    {ok, lists:foldr(fun(Source, AccState) ->
                             rabbit_log:warning("~s resource limit alarm cleared for dead node ~p~n",
                                                [Source, Node]),
                             maybe_alert(fun dict_unappend/3, Node, Source, false, AccState)
                     end, State, AlarmsForDeadNode)};

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

dict_append(Key, Val, Dict) ->
    L = case dict:find(Key, Dict) of
            {ok, V} -> V;
            error   -> []
        end,
    dict:store(Key, lists:usort([Val|L]), Dict).

dict_unappend(Key, Val, Dict) ->
    L = case dict:find(Key, Dict) of
            {ok, V} -> V;
            error   -> []
        end,

    case lists:delete(Val, L) of
        [] -> dict:erase(Key, Dict);
        X  -> dict:store(Key, X, Dict)
    end.

maybe_alert(UpdateFun, Node, Source, WasAlertAdded,
            State = #alarms{alarmed_nodes = AN,
                            alertees      = Alertees}) ->
    AN1 = UpdateFun(Node, Source, AN),
    %% Is alarm for Source still set on any node?
    StillHasAlerts = lists:any(fun ({_Node, NodeAlerts}) -> lists:member(Source, NodeAlerts) end, dict:to_list(AN1)),
    case StillHasAlerts of
        true -> ok;
        false -> rabbit_log:warning("~s resource limit alarm cleared across the cluster~n", [Source])
    end,
    Alert = {WasAlertAdded, StillHasAlerts, Node},
    case node() of
        Node -> ok = alert_remote(Alert,  Alertees, Source);
        _    -> ok
    end,
    ok = alert_local(Alert, Alertees, Source),
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
        {ok, Sources} -> [apply(M, F, A ++ [Pid, R, {true, true, node()}]) || R <- Sources];
        error          -> ok
    end,
    NewAlertees = dict:store(Pid, AlertMFA, Alertees),
    State#alarms{alertees = NewAlertees}.

handle_set_resource_alarm(Source, Node, State) ->
    rabbit_log:warning(
      "~s resource limit alarm set on node ~p.~n~n"
      "**********************************************************~n"
      "*** Publishers will be blocked until this alarm clears ***~n"
      "**********************************************************~n",
      [Source, Node]),
    {ok, maybe_alert(fun dict_append/3, Node, Source, true, State)}.

handle_set_alarm({file_descriptor_limit, []}, State) ->
    rabbit_log:warning(
      "file descriptor limit alarm set.~n~n"
      "********************************************************************~n"
      "*** New connections will not be accepted until this alarm clears ***~n"
      "********************************************************************~n"),
    {ok, State};
handle_set_alarm(Alarm, State) ->
    rabbit_log:warning("alarm '~p' set~n", [Alarm]),
    {ok, State}.

handle_clear_resource_alarm(Source, Node, State) ->
    rabbit_log:warning("~s resource limit alarm cleared on node ~p~n",
                       [Source, Node]),
    {ok, maybe_alert(fun dict_unappend/3, Node, Source, false, State)}.

handle_clear_alarm(file_descriptor_limit, State) ->
    rabbit_log:warning("file descriptor limit alarm cleared~n"),
    {ok, State};
handle_clear_alarm(Alarm, State) ->
    rabbit_log:warning("alarm '~p' cleared~n", [Alarm]),
    {ok, State}.

is_node_alarmed(Source, Node, #alarms{alarmed_nodes = AN}) ->
    case dict:find(Node, AN) of
        {ok, Sources} ->
            lists:member(Source, Sources);
        error ->
            false
    end.

get_alarms(#alarms{alarms = Alarms,
                   alarmed_nodes = AN}) ->
    Alarms ++ [ {{resource_limit, Source, Node}, []}
                || {Node, Sources} <- dict:to_list(AN), Source <- Sources ].
