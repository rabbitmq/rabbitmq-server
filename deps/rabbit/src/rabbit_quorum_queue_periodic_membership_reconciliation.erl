%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_quorum_queue_periodic_membership_reconciliation).

-behaviour(gen_server).

-export([on_node_up/1, on_node_down/1, queue_created/1, policy_set/0,
         record_pending_force_deletes/2]).

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
-define(FORCE_DELETE_TABLE, rabbit_qq_force_delete_pending).
-define(DEFAULT_FORCE_DELETE_RETRY_INTERVAL, 30_000).
-define(FORCE_DELETE_TABLE_OPEN_RETRIES, 10).

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

%% Record leaked members of a force-deleted queue for retry. Each member is
%% tagged with the UID of the incarnation being deleted so a newer incarnation
%% of the same queue name is never touched.
%%
%% Those UIDs only exist once `track_qq_members_uids' is enabled, so the retry
%% is not supported without it. A queue record that predates the flag can also
%% carry no UID for an individual member even when the flag is enabled. Such a
%% member is not recorded at all: a retry could not tell it apart from a newer
%% incarnation of the same queue name, so there is nothing the retry loop could
%% safely do with the entry. It is reported instead, for manual clean up.
%%
%% The entries are written and synced in the calling process rather than handed
%% to the owner of the table. The caller is deleting the queue, and once the
%% queue record is gone these entries are the only remaining trace of the leaked
%% members, so they have to reach disk before the delete completes.
%%
%% A failure to write them is logged rather than raised: the queue is already
%% being removed, so the delete has to carry on regardless.
-spec record_pending_force_deletes(rabbit_amqqueue:name(),
                                   [{server_id(), member_uid()}]) -> ok.
record_pending_force_deletes(_QName, []) ->
    ok;
record_pending_force_deletes(QName, PendingMembers) ->
    case rabbit_feature_flags:is_enabled(track_qq_members_uids) of
        true ->
            {Verifiable, Unverifiable} =
                lists:partition(fun({_ServerId, UId}) -> is_binary(UId) end,
                                PendingMembers),
            _ = [?LOG_WARNING(
                   "Leaked member ~w of ~ts has no recorded UID to verify "
                   "against, so it cannot be force-deleted on retry without "
                   "risking a newer incarnation of the same queue name. This "
                   "member may need manual data clean up",
                   [ServerId, rabbit_misc:rs(QName)])
                 || {ServerId, _} <- Unverifiable],
            record_verifiable_force_deletes(QName, Verifiable);
        false ->
            ok
    end.

record_verifiable_force_deletes(_QName, []) ->
    ok;
record_verifiable_force_deletes(QName, PendingMembers) ->
    case persist_pending_force_deletes(QName, PendingMembers) of
        ok ->
            ?LOG_INFO("Recorded ~b leaked member(s) of ~ts for force delete "
                      "retry: ~w",
                      [length(PendingMembers), rabbit_misc:rs(QName),
                       [ServerId || {ServerId, _UId} <- PendingMembers]]),
            %% Arming the retry timer can be asynchronous: the entries are on
            %% disk, and the owner arms the timer from `init/1' for anything
            %% still pending after a restart.
            gen_server:cast(?SERVER, force_delete_recorded);
        {error, Reason} ->
            ?LOG_WARNING(
              "Failed to record ~b leaked member(s) of ~ts for force delete "
              "retry: ~tp. These members may need manual data clean up",
              [length(PendingMembers), rabbit_misc:rs(QName), Reason])
    end,
    ok.

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

handle_cast(force_delete_recorded, State) ->
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
%% Leaked members are recorded by the force delete itself (see
%% `record_pending_force_deletes/2') in a node-local DETS table, and
%% `ra:force_delete_server/3' is retried until each member is gone. The pending
%% set survives a broker restart because it is persisted.
%%
%% This requires the `track_qq_members_uids' feature flag: each pending member is
%% tagged with the UID of the incarnation being deleted, and those UIDs are only
%% recorded once the flag is enabled.
%%
%% A retry is guarded by that UID: `rabbit_quorum_queue:force_delete_member/2'
%% compares it against the UID currently registered on the member's node and only
%% deletes when the two still match, so a member belonging to a newer incarnation
%% of the same queue name is never deleted. A member with no UID cannot be
%% guarded this way, so it never enters the table: it is reported at force delete
%% time so that it can be cleaned up manually.
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

persist_pending_force_deletes(QName, PendingMembers) ->
    Objects = [{ServerId, {QName, UId}} || {ServerId, UId} <- PendingMembers],
    try dets:insert(?FORCE_DELETE_TABLE, Objects) of
        ok ->
            dets:sync(?FORCE_DELETE_TABLE);
        {error, _} = Err ->
            Err
    catch
        _:Reason ->
            {error, Reason}
    end.

retry_force_deletes(State) ->
    %% Collect first, then mutate: DETS tables must not be modified during a
    %% foldl traversal.
    Entries = dets:foldl(fun(Entry, Acc) -> [Entry | Acc] end, [],
                         ?FORCE_DELETE_TABLE),
    _ = [retry_member(Entry) || Entry <- Entries],
    _ = dets:sync(?FORCE_DELETE_TABLE),
    ensure_force_delete_timer_if_pending(State).

%% Entries without a UID are refused by `record_pending_force_deletes/2', but
%% the table outlives the code that wrote it, so one can still be read back from
%% a file written by an earlier version. Deleting a member that cannot be guarded
%% by a UID risks removing a newer incarnation of the same queue name, so leave
%% it in place.
retry_member({ServerId, {QName, undefined}}) ->
    ?LOG_INFO("Member ~w of ~ts has no recorded UID to verify against, "
              "dropping from the force delete retry set. This member may "
              "need manual data clean up",
              [ServerId, rabbit_misc:rs(QName)]),
    dets:delete(?FORCE_DELETE_TABLE, ServerId);
retry_member({ServerId, {QName, ExpectedUId}}) ->
    case rabbit_quorum_queue:force_delete_member(ServerId, ExpectedUId) of
        ok ->
            ?LOG_INFO("Force delete of leaked member ~w of ~ts "
                      "succeeded on retry", [ServerId, rabbit_misc:rs(QName)]),
            dets:delete(?FORCE_DELETE_TABLE, ServerId);
        {skipped, gone} ->
            ?LOG_INFO("Leaked member ~w of ~ts is no longer present, dropping "
                      "from the force delete retry set", [ServerId, rabbit_misc:rs(QName)]),
            dets:delete(?FORCE_DELETE_TABLE, ServerId);
        {skipped, superseded} ->
            ?LOG_INFO("Member ~w of ~ts belongs to a newer incarnation, dropping "
                      "from the force delete retry set", [ServerId, rabbit_misc:rs(QName)]),
            dets:delete(?FORCE_DELETE_TABLE, ServerId);
        {error, Reason} ->
            ?LOG_DEBUG("Force delete retry of member ~w of ~ts failed "
                       "with ~w, will retry", [ServerId, rabbit_misc:rs(QName), Reason]),
            ok
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
