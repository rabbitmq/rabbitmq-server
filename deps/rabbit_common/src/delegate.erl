%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(delegate).

%% delegate is an alternative way of doing remote calls. Compared to
%% the rpc module, it reduces inter-node communication. For example,
%% if a message is routed to 1,000 queues on node A and needs to be
%% propagated to nodes B and C, it would be nice to avoid doing 2,000
%% remote casts to queue processes.
%%
%% An important issue here is preserving order - we need to make sure
%% that messages from a certain channel to a certain queue take a
%% consistent route, to prevent them being reordered. In fact all
%% AMQP-ish things (such as queue declaration results and basic.get)
%% must take the same route as well, to ensure that clients see causal
%% ordering correctly. Therefore we have a rather generic mechanism
%% here rather than just a message-reflector. That's also why we pick
%% the delegate process to use based on a hash of the source pid.
%%
%% When a function is invoked using delegate:invoke/2,
%% or delegate:invoke_no_result/2 on a group of pids, the pids are first split
%% into local and remote ones. Remote processes are then grouped by
%% node. The function is then invoked locally and on every node (using
%% gen_server2:multi/4) as many times as there are processes on that
%% node, sequentially.
%%
%% Errors returned when executing functions on remote nodes are re-raised
%% in the caller.
%%
%% RabbitMQ starts a pool of delegate processes on boot. The size of
%% the pool is configurable, the aim is to make sure we don't have too
%% few delegates and thus limit performance on many-CPU machines.

%% There are some optimisations applied.
%% If a message is sent to only one queue (a common scenario),
%% sending them over the delegate mechanism is redundant.
%% This optimization is applied to gen_server2 module calls when
%% delegate prefix matches the default, ?DEFAULT_NAME.
%%
%% Coonsider two examples:
%%
%%  1. "delegate:invoke(Pids, {erlang, process_info, [memory]})", "erlang:process_info/1"
%%      should be called inside the target node.
%%  2. "{Results, Errors} = delegate:invoke(MemberPids, ?DELEGATE_PREFIX, FunOrMFA)",
%%      Since this operation specifically specifies a delegate name rather than
%%      relying on ?DEFAULT_NAME, it will be invoked using the delegate mechanism.

-behaviour(gen_server2).

-export([start_link/1, start_link/2, invoke_no_result/2,
         invoke/2, invoke/3, monitor/2, monitor/3, demonitor/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {node, monitors, name}).

%%----------------------------------------------------------------------------

-export_type([monitor_ref/0]).

-type monitor_ref() :: reference() | {atom(), pid()}.
-type fun_or_mfa(A) :: fun ((pid()) -> A) | {atom(), atom(), [any()]}.

-spec start_link
        (non_neg_integer()) -> {'ok', pid()} | ignore | {'error', any()}.
-spec invoke
        ( pid(),  fun_or_mfa(A)) -> A;
        ([pid()], fun_or_mfa(A)) -> {[{pid(), A}], [{pid(), term()}]}.
-spec invoke_no_result(pid() | [pid()], fun_or_mfa(any())) -> 'ok'.
-spec monitor('process', pid()) -> monitor_ref().
-spec demonitor(monitor_ref()) -> 'true'.

%%----------------------------------------------------------------------------

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE,   10000).
-define(DEFAULT_NAME,        "delegate_").

%%----------------------------------------------------------------------------

start_link(Num) ->
    start_link(?DEFAULT_NAME, Num).

start_link(Name, Num) ->
    Name1 = delegate_name(Name, Num),
    gen_server2:start_link({local, Name1}, ?MODULE, [Name1], []).

invoke(Pid, FunOrMFA = {gen_server2, _F, _A}) when is_pid(Pid) ->  %% optimisation
    case safe_invoke(Pid, FunOrMFA) of
        {ok,    _, Result} -> Result;
        {error, _, {Class, Reason, StackTrace}}  -> erlang:raise(Class, Reason, StackTrace)
    end;
invoke(Pid, FunOrMFA) ->
    invoke(Pid, ?DEFAULT_NAME, FunOrMFA).

invoke(Pid, _Name, FunOrMFA) when is_pid(Pid) andalso node(Pid) =:= node() ->
    apply1(FunOrMFA, Pid);
invoke(Pid, ?DEFAULT_NAME, FunOrMFA = {gen_server2, _F, _A}) when is_pid(Pid) ->  %% optimisation
    case safe_invoke(Pid, FunOrMFA) of
        {ok,    _, Result} -> Result;
        {error, _, {Class, Reason, StackTrace}}  -> erlang:raise(Class, Reason, StackTrace)
    end;
invoke(Pid, Name, FunOrMFA) when is_pid(Pid) ->
    case invoke([Pid], Name, FunOrMFA) of
        {[{Pid, Result}], []} ->
            Result;
        {[], [{Pid, {Class, Reason, StackTrace}}]} ->
            erlang:raise(Class, Reason, StackTrace)
    end;

invoke([], _Name, _FunOrMFA) -> %% optimisation
    {[], []};
invoke([Pid], ?DEFAULT_NAME, FunOrMFA = {gen_server2, _F, _A}) when is_pid(Pid) -> %% optimisation
    case safe_invoke(Pid, FunOrMFA) of
        {ok,    _, Result} -> {[{Pid, Result}], []};
        {error, _, Error}  -> {[], [{Pid, Error}]}
    end;
invoke([Pid], _Name, FunOrMFA) when node(Pid) =:= node() -> %% optimisation
    case safe_invoke(Pid, FunOrMFA) of
        {ok,    _, Result} -> {[{Pid, Result}], []};
        {error, _, Error}  -> {[], [{Pid, Error}]}
    end;
invoke(Pids, Name = ?DEFAULT_NAME, FunOrMFA = {gen_server2, _F, _A}) when is_list(Pids) ->
    {LocalCallPids, Grouped} = group_local_call_pids_by_node(Pids),
    invoke(Pids, Name, FunOrMFA, LocalCallPids, Grouped);
invoke(Pids, Name, FunOrMFA) when is_list(Pids) ->
    {LocalPids, Grouped} = group_pids_by_node(Pids),
    invoke(Pids, Name, FunOrMFA, LocalPids, Grouped).

invoke(Pids, Name, FunOrMFA, LocalCallPids, Grouped) when is_list(Pids) ->
    %% The use of multi_call is only safe because the timeout is
    %% infinity, and thus there is no process spawned in order to do
    %% the sending. Thus calls can't overtake preceding calls/casts.
    {Replies, BadNodes} =
        case maps:keys(Grouped) of
            []          -> {[], []};
            RemoteNodes -> gen_server2:multi_call(
                             RemoteNodes, delegate(self(), Name, RemoteNodes),
                             {invoke, FunOrMFA, Grouped}, infinity)
        end,
    BadPids = [{Pid, {exit, {nodedown, BadNode}, []}} ||
                  BadNode <- BadNodes,
                  Pid     <- maps:get(BadNode, Grouped)],
    ResultsNoNode = lists:append([safe_invoke(LocalCallPids, FunOrMFA) |
                                  [Results || {_Node, Results} <- Replies]]),
    lists:foldl(
      fun ({ok,    Pid, Result}, {Good, Bad}) -> {[{Pid, Result} | Good], Bad};
          ({error, Pid, Error},  {Good, Bad}) -> {Good, [{Pid, Error} | Bad]}
      end, {[], BadPids}, ResultsNoNode).

monitor(process, Pid) ->
    ?MODULE:monitor(process, Pid, ?DEFAULT_NAME).

monitor(process, Pid, _Prefix) when node(Pid) =:= node() ->
    erlang:monitor(process, Pid);
monitor(process, Pid, Prefix) ->
    Name = delegate(Pid, Prefix, [node(Pid)]),
    gen_server2:cast(Name, {monitor, self(), Pid}),
    {Name, Pid}.

demonitor(Ref) when is_reference(Ref) ->
    erlang:demonitor(Ref);
demonitor({Name, Pid}) ->
    gen_server2:cast(Name, {demonitor, self(), Pid}).

invoke_no_result(Pid, FunOrMFA = {gen_server2, _F, _A}) when is_pid(Pid) ->
    _ = safe_invoke(Pid, FunOrMFA), %% we don't care about any error
    ok;
invoke_no_result(Pid, FunOrMFA) when is_pid(Pid) andalso node(Pid) =:= node() ->
    %% Optimization, avoids calling invoke_no_result/3.
    %%
    %% This may seem like a cosmetic change at first but it actually massively reduces the memory usage in mirrored
    %% queues when ack/nack are sent to the node that hosts a mirror.
    %% This way binary references are not kept around unnecessarily.
    %%
    %% See https://github.com/rabbitmq/rabbitmq-common/issues/208#issuecomment-311308583 for a before/after
    %% comparison.
    _ = safe_invoke(Pid, FunOrMFA), %% we don't care about any error
    ok;
invoke_no_result(Pid, FunOrMFA) when is_pid(Pid) ->
    %% Optimization, avoids calling invoke_no_result/3
    RemoteNode  = node(Pid),
    gen_server2:abcast([RemoteNode], delegate(self(), ?DEFAULT_NAME, [RemoteNode]),
                       {invoke, FunOrMFA,
                        maps:from_list([{RemoteNode, [Pid]}])}),
    ok;
invoke_no_result([], _FunOrMFA) -> %% optimisation
    ok;
invoke_no_result([Pid], FunOrMFA = {gen_server2, _F, _A}) when is_pid(Pid) -> %% optimisation
    _ = safe_invoke(Pid, FunOrMFA), %% must not die
    ok;
invoke_no_result([Pid], FunOrMFA) when node(Pid) =:= node() -> %% optimisation
    _ = safe_invoke(Pid, FunOrMFA), %% must not die
    ok;
invoke_no_result([Pid], FunOrMFA) ->
    RemoteNode  = node(Pid),
    gen_server2:abcast([RemoteNode], delegate(self(), ?DEFAULT_NAME, [RemoteNode]),
                       {invoke, FunOrMFA,
                        maps:from_list([{RemoteNode, [Pid]}])}),
    ok;
invoke_no_result(Pids, FunOrMFA = {gen_server2, _F, _A}) when is_list(Pids) ->
    {LocalCallPids, Grouped} = group_local_call_pids_by_node(Pids),
    invoke_no_result(Pids, FunOrMFA, LocalCallPids, Grouped);
invoke_no_result(Pids, FunOrMFA) when is_list(Pids) ->
    {LocalPids, Grouped} = group_pids_by_node(Pids),
    invoke_no_result(Pids, FunOrMFA, LocalPids, Grouped).

invoke_no_result(Pids, FunOrMFA, LocalCallPids, Grouped) when is_list(Pids) ->
    case maps:keys(Grouped) of
        []          -> ok;
        RemoteNodes -> gen_server2:abcast(
                         RemoteNodes, delegate(self(), ?DEFAULT_NAME, RemoteNodes),
                         {invoke, FunOrMFA, Grouped})
    end,
    _ = safe_invoke(LocalCallPids, FunOrMFA), %% must not die
    ok.

%%----------------------------------------------------------------------------

group_pids_by_node(Pids) ->
    LocalNode = node(),
    lists:foldl(
      fun (Pid, {Local, Remote}) when node(Pid) =:= LocalNode ->
              {[Pid | Local], Remote};
          (Pid, {Local, Remote}) ->
              {Local,
               maps:update_with(
                 node(Pid), fun (List) -> [Pid | List] end, [Pid], Remote)}
      end, {[], maps:new()}, Pids).

group_local_call_pids_by_node(Pids) ->
    {LocalPids0, Grouped0} = group_pids_by_node(Pids),
    maps:fold(fun(K, V, {AccIn, MapsIn}) -> 
        case V of
            %% just one Pid for the node
            [SinglePid] -> {[SinglePid | AccIn], MapsIn};
            %% If the value is a list of more than one pid, 
            %% the (K,V) will be put into the new map which will be called 
            %% through delegate to reduce inter-node communication.
            _ -> {AccIn, maps:update_with(K, fun(V1) -> V1 end, V, MapsIn)}
        end
    end, {LocalPids0, maps:new()}, Grouped0).

delegate_name(Name, Hash) ->
    list_to_atom(Name ++ integer_to_list(Hash)).

delegate(Pid, Prefix, RemoteNodes) ->
    case get(delegate) of
        undefined -> Name = delegate_name(Prefix,
                              erlang:phash2(Pid,
                                            delegate_sup:count(RemoteNodes, Prefix))),
                     put(delegate, Name),
                     Name;
        Name      -> Name
    end.

safe_invoke(Pids, FunOrMFA) when is_list(Pids) ->
    [safe_invoke(Pid, FunOrMFA) || Pid <- Pids];
safe_invoke(Pid, FunOrMFA) when is_pid(Pid) ->
    try
        {ok, Pid, apply1(FunOrMFA, Pid)}
    catch Class:Reason:Stacktrace ->
            {error, Pid, {Class, Reason, Stacktrace}}
    end.

apply1({M, F, A}, Arg) -> apply(M, F, [Arg | A]);
apply1(Fun,       Arg) -> Fun(Arg).

%%----------------------------------------------------------------------------

init([Name]) ->
    {ok, #state{node = node(), monitors = dict:new(), name = Name}, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({invoke, FunOrMFA, Grouped}, _From, State = #state{node = Node}) ->
    {reply, safe_invoke(maps:get(Node, Grouped), FunOrMFA), State,
     hibernate}.

handle_cast({monitor, MonitoringPid, Pid},
            State = #state{monitors = Monitors}) ->
    Monitors1 = case dict:find(Pid, Monitors) of
                    {ok, {Ref, Pids}} ->
                        Pids1 = gb_sets:add_element(MonitoringPid, Pids),
                        dict:store(Pid, {Ref, Pids1}, Monitors);
                    error ->
                        Ref = erlang:monitor(process, Pid),
                        Pids = gb_sets:singleton(MonitoringPid),
                        dict:store(Pid, {Ref, Pids}, Monitors)
                end,
    {noreply, State#state{monitors = Monitors1}, hibernate};

handle_cast({demonitor, MonitoringPid, Pid},
            State = #state{monitors = Monitors}) ->
    Monitors1 = case dict:find(Pid, Monitors) of
                    {ok, {Ref, Pids}} ->
                        Pids1 = gb_sets:del_element(MonitoringPid, Pids),
                        case gb_sets:is_empty(Pids1) of
                            true  -> erlang:demonitor(Ref),
                                     dict:erase(Pid, Monitors);
                            false -> dict:store(Pid, {Ref, Pids1}, Monitors)
                        end;
                    error ->
                        Monitors
                end,
    {noreply, State#state{monitors = Monitors1}, hibernate};

handle_cast({invoke, FunOrMFA, Grouped}, State = #state{node = Node}) ->
    _ = safe_invoke(maps:get(Node, Grouped), FunOrMFA),
    {noreply, State, hibernate}.

handle_info({'DOWN', Ref, process, Pid, Info},
            State = #state{monitors = Monitors, name = Name}) ->
    {noreply,
     case dict:find(Pid, Monitors) of
         {ok, {Ref, Pids}} ->
             Msg = {'DOWN', {Name, Pid}, process, Pid, Info},
             gb_sets:fold(fun (MonitoringPid, _) -> MonitoringPid ! Msg end,
                          none, Pids),
             State#state{monitors = dict:erase(Pid, Monitors)};
         error ->
             State
     end, hibernate};

handle_info(_Info, State) ->
    {noreply, State, hibernate}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
