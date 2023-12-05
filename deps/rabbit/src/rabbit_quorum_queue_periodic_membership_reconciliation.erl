%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_quorum_queue_periodic_membership_reconciliation).

-feature(maybe_expr, enable).

-behaviour(gen_server).

-export([on_node_up/1, on_node_down/1, queue_created/1, policy_set/0]).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_INTERVAL, 60_000*60).
-define(DEFAULT_TRIGGER_INTERVAL, 10_000).
-define(QUEUE_COUNT_START_RANDOM_SELECTION, 1_000).

-define(EVAL_MSG, membership_reconciliation).

-record(state, {timer_ref :: reference() | undefined,
                interval :: non_neg_integer(),
                trigger_interval :: non_neg_integer(),
                target_group_size :: non_neg_integer() | undefined,
                enabled :: boolean(),
                auto_remove :: boolean()}).

%%----------------------------------------------------------------------------
%% Start
%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().
start_link() -> gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

on_node_up(Node) ->
    gen_server:cast(?SERVER, {membership_reconciliation_trigger, {node_up, Node}}).

on_node_down(Node) ->
    gen_server:cast(?SERVER, {membership_reconciliation_trigger, {node_down, Node}}).

queue_created(Q) ->
    gen_server:cast(?SERVER, {membership_reconciliation_trigger, {queue_created, Q}}).

policy_set() ->
    gen_server:cast(?SERVER, {membership_reconciliation_trigger, policy_set}).

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([]) ->
    Enabled = rabbit_misc:get_env(rabbit, quorum_membership_reconciliation_enabled,
                                  false),
    AutoRemove = rabbit_misc:get_env(rabbit, quorum_membership_reconciliation_auto_remove,
                                     false),
    Interval = rabbit_misc:get_env(rabbit, quorum_membership_reconciliation_interval,
                                          ?DEFAULT_INTERVAL),
    TriggerInterval = rabbit_misc:get_env(rabbit, quorum_membership_reconciliation_trigger_interval,
                                        ?DEFAULT_TRIGGER_INTERVAL),
    TargetGroupSize = rabbit_misc:get_env(rabbit, quorum_membership_reconciliation_target_group_size,
                                          undefined),
    State = #state{interval = Interval,
                   trigger_interval = TriggerInterval,
                   target_group_size = TargetGroupSize,
                   enabled = Enabled,
                   auto_remove = AutoRemove},
    case Enabled of
        true ->
            Ref = erlang:send_after(Interval, self(), ?EVAL_MSG),
            {ok, State#state{timer_ref = Ref}};
        false ->
            {ok, State, hibernate}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({membership_reconciliation_trigger, _Reason}, #state{enabled = false} = State) ->
    {noreply, State, hibernate};
handle_cast({membership_reconciliation_trigger, Reason}, #state{timer_ref = OldRef,
                               trigger_interval = Time} = State) ->
    rabbit_log:debug("Quorum Queue membership reconciliation triggered: ~p",
                     [Reason]),
    _ = erlang:cancel_timer(OldRef),
    Ref = erlang:send_after(Time, self(), ?EVAL_MSG),
    {noreply, State#state{timer_ref = Ref}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(?EVAL_MSG, #state{interval = Interval,
                              trigger_interval = TriggerInterval} = State) ->
    Res = reconciliate_quorum_queue_membership(State),
    NewTimeout = case Res of
                     noop ->
                         Interval;
                     _ ->
                         TriggerInterval
                 end,
    Ref = erlang:send_after(NewTimeout, self(), ?EVAL_MSG),
    {noreply, State#state{timer_ref = Ref}};
handle_info(_Info, #state{enabled = false} = State) ->
    {noreply, State, hibernate};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------

reconciliate_quorum_queue_membership(State) ->
    LocalLeaders = rabbit_amqqueue:list_local_leaders(),
    ExpectedNodes = rabbit_nodes:list_members(),
    Running = rabbit_nodes:list_running(),
    reconciliate_quorum_members(ExpectedNodes, Running, LocalLeaders, State, noop).

reconciliate_quorum_members(_ExpectedNodes, _Running, [], _State, Result) ->
    Result;
reconciliate_quorum_members(ExpectedNodes, Running, [Q | LocalLeaders],
                             #state{target_group_size = TargetSize} = State,
                             OldResult) ->
    Result =
        maybe
            {ok, Members, {_, LeaderNode}} = ra:members(amqqueue:get_pid(Q), 500),
            %% Check if Leader is indeed this node
            LeaderNode ?= node(),
            %% And that this not is not in maintenance mode
            true ?= not rabbit_maintenance:is_being_drained_local_read(node()),
            MemberNodes = [Node || {_, Node} <- Members],
            DanglingNodes = MemberNodes -- ExpectedNodes,
            case maybe_remove(DanglingNodes, State) of
                false ->
                    maybe_add_member(Q, Running, MemberNodes, get_target_size(Q, TargetSize));
                true ->
                    remove_members(Q, DanglingNodes)
            end
        else
            {timeout, Reason} ->
                rabbit_log:debug("Find leader timeout: ~p", [Reason]),
                ok;
            _ ->
                noop
        end,
    reconciliate_quorum_members(ExpectedNodes, Running, LocalLeaders, State,
                                 update_result(OldResult, Result)).

maybe_remove(_, #state{auto_remove = false}) ->
    false;
maybe_remove([], #state{auto_remove = true}) ->
    false;
maybe_remove(_Nodes, #state{auto_remove = true}) ->
    true.

maybe_add_member(Q, Running, MemberNodes, TargetSize) ->
    %% Filter out any new nodes under maintenance
    New = rabbit_maintenance:filter_out_drained_nodes_local_read(Running -- MemberNodes),
    case should_add_node(MemberNodes, New, TargetSize) of
        true ->
            %% In the future, sort the list of new nodes based on load,
            %% availability zones etc
            Node = select_node(New),
            QName = amqqueue:get_name(Q),
            case rabbit_quorum_queue:add_member(Q, Node) of
                ok ->
                    rabbit_log:debug(
                      "Added node ~ts as a member to ~ts as "
                      "the queues target group size(#~w) is not met and "
                      "there are enough new nodes(#~w) in the cluster",
                      [Node, rabbit_misc:rs(QName), TargetSize, length(New)]);
                {error, Err} ->
                    rabbit_log:warning(
                      "~ts: failed to add member (replica) on node ~w, error: ~w",
                      [rabbit_misc:rs(QName), Node, Err])
            end,
            ok;
        false ->
            noop
    end.

should_add_node(MemberNodes, New, TargetSize) ->
    CurrentSize = length(MemberNodes),
    NumberOfNewNodes = length(New),
    maybe
        true ?= NumberOfNewNodes > 0, %% There are new nodes to grow to
        true ?= CurrentSize < TargetSize, %% Target size not reached
        true ?= rabbit_misc:is_even(CurrentSize) orelse NumberOfNewNodes > 1, %% Enough nodes to grow to odd member size
        true ?= rabbit_nodes:is_running(lists:delete(node(), MemberNodes))
    end.

get_target_size(Q, undefined) ->
    get_target_size(Q);
get_target_size(Q, N) when N > 0 ->
    max(N, get_target_size(Q)).

get_target_size(Q) ->
    PolicyValue = case rabbit_policy:get(<<"target-group-size">>, Q) of
                      undefined ->
                          0;
                      PolicyN ->
                          PolicyN
                  end,
    Arguments = amqqueue:get_arguments(Q),
    case rabbit_misc:table_lookup(Arguments, <<"x-quorum-target-group-size">>) of
        undefined ->
            PolicyValue;
        ArgN ->
            max(ArgN, PolicyValue)
    end.

remove_members(_Q, []) ->
    ok;
remove_members(Q, [Node | Nodes]) ->
    case rabbit_quorum_queue:delete_member(Q, Node) of
        ok ->
            QName = amqqueue:get_name(Q),
            rabbit_log:debug("~ts: Successfully removed member (replica) on node ~w",
                               [rabbit_misc:rs(QName), Node]),
            ok;
        {error, Err} ->
            QName = amqqueue:get_name(Q),
            rabbit_log:warning("~ts: failed to remove member (replica) on node "
                               "~w, error: ~w",
                               [rabbit_misc:rs(QName), Node, Err])
    end,
    remove_members(Q, Nodes).


%% Make sure any non-noop result is stored.
update_result(noop, Result) ->
    Result;
update_result(Result, noop) ->
    Result;
update_result(Result, Result) ->
    Result.

select_node([Node]) ->
    Node;
select_node(Nodes) ->
    lists:nth(rand:uniform(length(Nodes)), Nodes).
