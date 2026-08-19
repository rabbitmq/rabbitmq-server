%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_quorum_queue_periodic_membership_reconciliation).

-behaviour(gen_server).

-export([on_node_up/1, on_node_down/1, queue_created/1, policy_set/0,
         schedule/2]).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("kernel/include/logger.hrl").

-define(SERVER, ?MODULE).
-define(DEFAULT_INTERVAL, 60_000*60).
-define(DEFAULT_TRIGGER_INTERVAL, 10_000).
-define(QUEUE_COUNT_START_RANDOM_SELECTION, 1_000).

-define(EVAL_MSG, membership_reconciliation).

%% Force delete retry: retrying force deletes of quorum queue members that were
%% left behind on unreachable nodes during a force delete.
-define(RA_SYSTEM, quorum_queues).
-define(FORCE_DELETE_TABLE, rabbit_qq_force_delete_pending).
-define(DEFAULT_FORCE_DELETE_RETRY_INTERVAL, 30_000).
-define(FORCE_DELETE_TABLE_OPEN_RETRIES, 10).
-define(UID_QUERY_TIMEOUT, 5_000).

-record(state, {timer_ref :: reference() | undefined,
                interval :: non_neg_integer(),
                trigger_interval :: non_neg_integer(),
                target_group_size :: non_neg_integer() | undefined,
                enabled :: boolean(),
                auto_remove :: boolean(),
                force_delete_timer_ref :: reference() | undefined,
                force_delete_interval :: non_neg_integer()}).

-type server_id() :: {atom(), node()}.
-type member_uid() :: binary() | undefined.

%%----------------------------------------------------------------------------
%% Start
%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().
start_link() -> gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

on_node_up(Node) ->
    gen_server:cast(?SERVER, {membership_reconciliation_trigger, {node_up, Node}}),
    %% A returning node may host members left behind by an earlier force delete;
    %% retry now instead of waiting for the next tick. This runs regardless of
    %% whether reconciliation is enabled.
    gen_server:cast(?SERVER, force_delete_retry).

on_node_down(Node) ->
    gen_server:cast(?SERVER, {membership_reconciliation_trigger, {node_down, Node}}).

queue_created(Q) ->
    gen_server:cast(?SERVER, {membership_reconciliation_trigger, {queue_created, Q}}).

policy_set() ->
    gen_server:cast(?SERVER, {membership_reconciliation_trigger, policy_set}).

%% Schedule leaked members of a force-deleted queue for retry. Each member is
%% tagged with the UID of the incarnation being deleted so a newer incarnation
%% of the same queue name is never touched.
%%
%% Those UIDs only exist once `track_qq_members_uids' is enabled, so the retry
%% is not supported without it.
-spec schedule(rabbit_amqqueue:name(), [{server_id(), member_uid()}]) -> ok.
schedule(_QName, []) ->
    ok;
schedule(QName, PendingMembers) ->
    case rabbit_feature_flags:is_enabled(track_qq_members_uids) of
        true ->
            gen_server:cast(?SERVER, {schedule_force_delete, QName, PendingMembers});
        false ->
            ok
    end.

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
    ForceDeleteInterval = rabbit_misc:get_env(rabbit,
                                              quorum_queue_force_delete_retry_interval,
                                              ?DEFAULT_FORCE_DELETE_RETRY_INTERVAL),
    ok = open_force_delete_table(),
    State0 = #state{interval = Interval,
                    trigger_interval = TriggerInterval,
                    target_group_size = TargetGroupSize,
                    enabled = Enabled,
                    auto_remove = AutoRemove,
                    force_delete_interval = ForceDeleteInterval},
    %% The force delete retry path runs regardless of the reconciliation toggle;
    %% resume any pending force deletes that did not complete before a restart.
    State = ensure_force_delete_timer_if_pending(State0),
    case Enabled of
        true ->
            Ref = erlang:send_after(Interval, self(), ?EVAL_MSG),
            {ok, State#state{timer_ref = Ref}};
        false ->
            {ok, State, hibernate}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({schedule_force_delete, QName, PendingMembers}, State) ->
    _ = [dets:insert(?FORCE_DELETE_TABLE, {ServerId, {QName, UId}})
         || {ServerId, UId} <- PendingMembers],
    _ = dets:sync(?FORCE_DELETE_TABLE),
    {noreply, ensure_force_delete_timer(State)};
handle_cast(force_delete_retry, State) ->
    {noreply, retry_force_deletes(State)};
handle_cast({membership_reconciliation_trigger, _Reason}, #state{enabled = false} = State) ->
    {noreply, State, hibernate};
handle_cast({membership_reconciliation_trigger, Reason}, #state{timer_ref = OldRef,
                               trigger_interval = Time} = State) ->
    ?LOG_DEBUG("Quorum Queue membership reconciliation scheduled: ~p", [Reason]),
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
handle_info(force_delete_retry, State) ->
    {noreply, retry_force_deletes(State#state{force_delete_timer_ref = undefined})};
handle_info(_Info, #state{enabled = false} = State) ->
    {noreply, State, hibernate};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    _ = dets:close(?FORCE_DELETE_TABLE),
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

reconciliate_quorum_members([], _Running, _, _State, Result) ->
    %% if there are no expected nodes rabbit_nodes:list_running/0 encountered
    %% an error during query and returned the empty list which is case we need
    %% to handle
    Result;
reconciliate_quorum_members(_ExpectedNodes, _Running, [], _State, Result) ->
    Result;
reconciliate_quorum_members(ExpectedNodes, Running, [Q | LocalLeaders],
                             #state{target_group_size = TargetSize} = State,
                             OldResult) ->
    Result =
        maybe
            {ok, Members, {_, LeaderNode}} ?= ra:members(amqqueue:get_pid(Q), 500),
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
                ?LOG_DEBUG("Find leader timeout: ~p", [Reason]),
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
                    ?LOG_DEBUG(
                      "Added node ~ts as a member to ~ts as "
                      "the queues target group size(#~w) is not met and "
                      "there are enough new nodes(#~w) in the cluster",
                      [Node, rabbit_misc:rs(QName), TargetSize, length(New)]);
                {error, Err} ->
                    ?LOG_WARNING(
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
            ?LOG_DEBUG("~ts: Successfully removed member (replica) on node ~w",
                               [rabbit_misc:rs(QName), Node]),
            ok;
        {error, Err} ->
            QName = amqqueue:get_name(Q),
            ?LOG_DEBUG("~ts: failed to remove member (replica) on node "
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

%%----------------------------------------------------------------------------
%% Force delete retry
%%
%% When a quorum queue is force-deleted while one or more of its member nodes are
%% unreachable, `ra:force_delete_server/3' fails on those nodes and the Ra members
%% are left behind. The next declare under the same Ra name then collides with the
%% orphans (`{already_started, _}' on every member) and cannot form a cluster, so
%% the name stays poisoned.
%%
%% Leaked members arrive via a `queue_force_deleted' event (see `schedule/2'),
%% are persisted in a node-local DETS table, and `ra:force_delete_server/3' is
%% retried until each member is gone. The pending set survives a broker restart
%% because it is persisted.
%%
%% This requires the `track_qq_members_uids' feature flag: each pending member is
%% tagged with the UID of the incarnation being deleted, and those UIDs are only
%% recorded once the flag is enabled.
%%
%% Before a retry the member's currently registered UID on the target node is read
%% and the force delete is only performed when it still matches, so a member
%% belonging to a newer incarnation of the same queue name is never deleted. A
%% member that cannot be verified this way is dropped rather than deleted, and is
%% reported so that it can be cleaned up manually.
%%----------------------------------------------------------------------------

open_force_delete_table() ->
    open_force_delete_table(?FORCE_DELETE_TABLE_OPEN_RETRIES).

open_force_delete_table(RetriesLeft) ->
    File = filename:join(rabbit:data_dir(), "qq_force_delete_retry.dets"),
    Opts = [{file, File}, {auto_save, infinity}],
    case dets:open_file(?FORCE_DELETE_TABLE, Opts) of
        {ok, _} ->
            ok;
        {error, Error} when RetriesLeft > 0 ->
            _ = file:delete(File),
            ?LOG_WARNING("Failed to open the quorum queue force delete retry DETS "
                         "file at ~tp: ~tp. Deleting it and retrying (~b retries "
                         "left)", [File, Error, RetriesLeft]),
            timer:sleep(1000),
            open_force_delete_table(RetriesLeft - 1);
        {error, Error} ->
            {error, Error}
    end.

retry_force_deletes(State) ->
    %% Collect first, then mutate: DETS tables must not be modified during a
    %% foldl traversal.
    Entries = dets:foldl(fun(Entry, Acc) -> [Entry | Acc] end, [],
                         ?FORCE_DELETE_TABLE),
    _ = [retry_member(Entry) || Entry <- Entries],
    _ = dets:sync(?FORCE_DELETE_TABLE),
    ensure_force_delete_timer_if_pending(State).

retry_member({{RaName, Node} = ServerId, {QName, ExpectedUId}}) ->
    case member_status(Node, RaName, ExpectedUId) of
        delete ->
            case rabbit_quorum_queue:force_delete_member(ServerId) of
                ok ->
                    ?LOG_INFO("Force delete of leaked member ~w of ~ts "
                              "succeeded on retry", [ServerId, rabbit_misc:rs(QName)]),
                    dets:delete(?FORCE_DELETE_TABLE, ServerId);
                Err ->
                    ?LOG_DEBUG("Force delete retry of member ~w of ~ts failed "
                               "with ~w, will retry", [ServerId, rabbit_misc:rs(QName), Err]),
                    ok
            end;
        gone ->
            ?LOG_INFO("Leaked member ~w of ~ts is no longer present, dropping "
                      "from the force delete retry set", [ServerId, rabbit_misc:rs(QName)]),
            dets:delete(?FORCE_DELETE_TABLE, ServerId);
        superseded ->
            ?LOG_INFO("Member ~w of ~ts belongs to a newer incarnation, dropping "
                      "from the force delete retry set", [ServerId, rabbit_misc:rs(QName)]),
            dets:delete(?FORCE_DELETE_TABLE, ServerId);
        unverifiable ->
            ?LOG_INFO("Member ~w of ~ts has no recorded UID to verify against, "
                      "dropping from the force delete retry set. This member may "
                      "need manual data clean up",
                      [ServerId, rabbit_misc:rs(QName)]),
            dets:delete(?FORCE_DELETE_TABLE, ServerId);
        unreachable ->
            ok
    end.

%% Decide what to do with a pending member by comparing the UID currently
%% registered on the target node with the UID of the incarnation we intended to
%% delete. This mirrors the UID handling in rabbit_quorum_queue:recover/2.
-spec member_status(node(), atom(), member_uid()) ->
    delete | gone | superseded | unverifiable | unreachable.
member_status(Node, RaName, ExpectedUId) ->
    case remote_uid(Node, RaName) of
        {error, _} ->
            unreachable;
        undefined ->
            gone;
        _CurrentUId when ExpectedUId =:= undefined ->
            %% A queue record that predates the feature flag can still carry no
            %% UID for this node. Deleting a member that cannot be verified risks
            %% removing a newer incarnation of the same queue name, so leave it.
            unverifiable;
        ExpectedUId ->
            delete;
        _OtherUId ->
            superseded
    end.

remote_uid(Node, RaName) ->
    try erpc:call(Node, ra_directory, uid_of, [?RA_SYSTEM, RaName],
                  ?UID_QUERY_TIMEOUT) of
        UId ->
            UId
    catch
        _:Reason ->
            {error, Reason}
    end.

ensure_force_delete_timer_if_pending(State) ->
    case dets:info(?FORCE_DELETE_TABLE, size) of
        0 -> State;
        _ -> ensure_force_delete_timer(State)
    end.

ensure_force_delete_timer(#state{force_delete_timer_ref = Ref} = State)
  when is_reference(Ref) ->
    State;
ensure_force_delete_timer(#state{force_delete_interval = Interval} = State) ->
    Ref = erlang:send_after(Interval, self(), force_delete_retry),
    State#state{force_delete_timer_ref = Ref}.
