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
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_persister).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([transaction/1, extend_transaction/2, dirty_work/1,
         commit_transaction/1, rollback_transaction/1,
         force_snapshot/0, serial/0]).

-include("rabbit.hrl").

-define(SERVER, ?MODULE).

-define(LOG_BUNDLE_DELAY, 5).
-define(COMPLETE_BUNDLE_DELAY, 2).

-define(HIBERNATE_AFTER, 10000).

-define(MAX_WRAP_ENTRIES, 500).

-define(PERSISTER_LOG_FORMAT_VERSION, {2, 4}).

-record(pstate, {log_handle, entry_count, deadline,
                 pending_logs, pending_replies,
                 snapshot}).             

%% two tables for efficient persistency
%% one maps a key to a message
%% the other maps a key to one or more queues.
%% The aim is to reduce the overload of storing a message multiple times
%% when it appears in several queues.
-record(psnapshot, {serial, transactions, messages, queues}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(qmsg() :: {amqqueue(), pkey()}).
-type(work_item() ::
      {publish, message(), qmsg()} |
      {deliver, qmsg()} |
      {ack, qmsg()}).

-spec(start_link/0 :: () -> {'ok', pid()} | 'ignore' | {'error', any()}).
-spec(transaction/1 :: ([work_item()]) -> 'ok').
-spec(extend_transaction/2 :: (txn(), [work_item()]) -> 'ok').
-spec(dirty_work/1 :: ([work_item()]) -> 'ok').
-spec(commit_transaction/1 :: (txn()) -> 'ok').
-spec(rollback_transaction/1 :: (txn()) -> 'ok').
-spec(force_snapshot/0 :: () -> 'ok').
-spec(serial/0 :: () -> non_neg_integer()).

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

transaction(MessageList) ->
    ?LOGDEBUG("transaction ~p~n", [MessageList]),
    TxnKey = rabbit_guid:guid(),
    gen_server:call(?SERVER, {transaction, TxnKey, MessageList}, infinity).

extend_transaction(TxnKey, MessageList) ->
    ?LOGDEBUG("extend_transaction ~p ~p~n", [TxnKey, MessageList]),
    gen_server:cast(?SERVER, {extend_transaction, TxnKey, MessageList}).

dirty_work(MessageList) ->
    ?LOGDEBUG("dirty_work ~p~n", [MessageList]),
    gen_server:cast(?SERVER, {dirty_work, MessageList}).

commit_transaction(TxnKey) ->
    ?LOGDEBUG("commit_transaction ~p~n", [TxnKey]),
    gen_server:call(?SERVER, {commit_transaction, TxnKey}, infinity).

rollback_transaction(TxnKey) ->
    ?LOGDEBUG("rollback_transaction ~p~n", [TxnKey]),
    gen_server:cast(?SERVER, {rollback_transaction, TxnKey}).

force_snapshot() ->
    gen_server:call(?SERVER, force_snapshot, infinity).

serial() ->
    gen_server:call(?SERVER, serial, infinity).

%%--------------------------------------------------------------------

init(_Args) ->
    process_flag(trap_exit, true),
    FileName = base_filename(),
    ok = filelib:ensure_dir(FileName),
    Snapshot = #psnapshot{serial       = 0,
                          transactions = dict:new(),
                          messages     = ets:new(messages, []),
                          queues       = ets:new(queues, [])},
    LogHandle =
        case disk_log:open([{name, rabbit_persister},
                            {head, current_snapshot(Snapshot)},
                            {file, FileName}]) of
            {ok, LH} -> LH;
            {repaired, LH, {recovered, Recovered}, {badbytes, Bad}} ->
                WarningFun = if
                                 Bad > 0 -> fun rabbit_log:warning/2;
                                 true    -> fun rabbit_log:info/2
                             end,
                WarningFun("Repaired persister log - ~p recovered, ~p bad~n",
                           [Recovered, Bad]),
                LH
        end,
    {Res, LoadedSnapshot} = internal_load_snapshot(LogHandle, Snapshot),
    NewSnapshot = LoadedSnapshot#psnapshot{
                    serial = LoadedSnapshot#psnapshot.serial + 1},
    case Res of
        ok ->
            ok = take_snapshot(LogHandle, NewSnapshot);
        {error, Reason} ->
            rabbit_log:error("Failed to load persister log: ~p~n", [Reason]),
            ok = take_snapshot_and_save_old(LogHandle, NewSnapshot)
    end,
    State = #pstate{log_handle = LogHandle,
                    entry_count = 0,
                    deadline = infinity,
                    pending_logs = [],
                    pending_replies = [],
                    snapshot = NewSnapshot},
    {ok, State}.

handle_call({transaction, Key, MessageList}, From, State) ->
    NewState = internal_extend(Key, MessageList, State),
    do_noreply(internal_commit(From, Key, NewState));
handle_call({commit_transaction, TxnKey}, From, State) ->
    do_noreply(internal_commit(From, TxnKey, State));
handle_call(force_snapshot, _From, State) -> 
    do_reply(ok, flush(true, State));
handle_call(serial, _From,
            State = #pstate{snapshot = #psnapshot{serial = Serial}}) ->
    do_reply(Serial, State);
handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({rollback_transaction, TxnKey}, State) ->
    do_noreply(internal_rollback(TxnKey, State));
handle_cast({dirty_work, MessageList}, State) ->
    do_noreply(internal_dirty_work(MessageList, State));
handle_cast({extend_transaction, TxnKey, MessageList}, State) ->
    do_noreply(internal_extend(TxnKey, MessageList, State));
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State = #pstate{deadline = infinity}) ->
    State1 = flush(true, State),
    %% TODO: Once we drop support for R11B-5, we can change this to
    %% {noreply, State1, hibernate};
    proc_lib:hibernate(gen_server2, enter_loop, [?MODULE, [], State1]);
handle_info(timeout, State) ->
    do_noreply(flush(State));
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State = #pstate{log_handle = LogHandle}) ->
    flush(State),
    disk_log:close(LogHandle),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, flush(State)}.

%%--------------------------------------------------------------------

internal_extend(Key, MessageList, State) ->
    log_work(fun (ML) -> {extend_transaction, Key, ML} end,
             MessageList, State).

internal_dirty_work(MessageList, State) ->
    log_work(fun (ML) -> {dirty_work, ML} end,
             MessageList, State).

internal_commit(From, Key, State = #pstate{snapshot = Snapshot}) -> 
    Unit = {commit_transaction, Key},
    NewSnapshot = internal_integrate1(Unit, Snapshot),
    complete(From, Unit, State#pstate{snapshot = NewSnapshot}).

internal_rollback(Key, State = #pstate{snapshot = Snapshot}) ->
    Unit = {rollback_transaction, Key},
    NewSnapshot = internal_integrate1(Unit, Snapshot),
    log(State#pstate{snapshot = NewSnapshot}, Unit).

complete(From, Item, State = #pstate{deadline = ExistingDeadline,
                                     pending_logs = Logs,
                                     pending_replies = Waiting}) ->
    State#pstate{deadline = compute_deadline(
                              ?COMPLETE_BUNDLE_DELAY, ExistingDeadline),
                 pending_logs = [Item | Logs],
                 pending_replies = [From | Waiting]}.

%% This is made to limit disk usage by writing messages only once onto
%% disk.  We keep a table associating pkeys to messages, and provided
%% the list of messages to output is left to right, we can guarantee
%% that pkeys will be a backreference to a message in memory when a
%% "tied" is met.
log_work(CreateWorkUnit, MessageList,
         State = #pstate{
           snapshot = Snapshot = #psnapshot{
                        messages = Messages}}) ->
    Unit = CreateWorkUnit(
             rabbit_misc:map_in_order(
               fun(M = {publish, Message, QK = {_QName, PKey}}) ->
                       case ets:lookup(Messages, PKey) of
                           [_] -> {tied, QK};
                           []  -> ets:insert(Messages, {PKey, Message}), 
                                  M
                       end;
                  (M) -> M
               end,
               MessageList)),
    NewSnapshot = internal_integrate1(Unit, Snapshot),
    log(State#pstate{snapshot = NewSnapshot}, Unit).

log(State = #pstate{deadline = ExistingDeadline, pending_logs = Logs}, 
    Message) ->
    State#pstate{deadline = compute_deadline(?LOG_BUNDLE_DELAY,
                                             ExistingDeadline),
                 pending_logs = [Message | Logs]}.

base_filename() ->
    rabbit_mnesia:dir() ++ "/rabbit_persister.LOG".

take_snapshot(LogHandle, OldFileName, Snapshot) ->
    ok = disk_log:sync(LogHandle),
    %% current_snapshot is the Head (ie. first thing logged)
    ok = disk_log:reopen(LogHandle, OldFileName, current_snapshot(Snapshot)).

take_snapshot(LogHandle, Snapshot) ->
    OldFileName = lists:flatten(base_filename() ++ ".previous"),
    file:delete(OldFileName),
    rabbit_log:info("Rolling persister log to ~p~n", [OldFileName]),
    ok = take_snapshot(LogHandle, OldFileName, Snapshot).

take_snapshot_and_save_old(LogHandle, Snapshot) ->
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    Timestamp = MegaSecs * 1000000 + Secs * 1000 + MicroSecs,
    OldFileName = lists:flatten(io_lib:format("~s.saved.~p",
                                              [base_filename(), Timestamp])),
    rabbit_log:info("Saving persister log in ~p~n", [OldFileName]),
    ok = take_snapshot(LogHandle, OldFileName, Snapshot).

maybe_take_snapshot(Force, State = #pstate{entry_count = EntryCount,
                                           log_handle = LH,
                                           snapshot = Snapshot})
  when Force orelse EntryCount >= ?MAX_WRAP_ENTRIES ->
    ok = take_snapshot(LH, Snapshot),
    State#pstate{entry_count = 0};
maybe_take_snapshot(_Force, State) ->
    State.

later_ms(DeltaMilliSec) ->
    {MegaSec, Sec, MicroSec} = now(),
    %% Note: not normalised. Unimportant for this application.
    {MegaSec, Sec, MicroSec + (DeltaMilliSec * 1000)}.

%% Result = B - A, more or less
time_diff({B1, B2, B3}, {A1, A2, A3}) ->
    (B1 - A1) * 1000000 + (B2 - A2) + (B3 - A3) / 1000000.0 .

compute_deadline(TimerDelay, infinity) ->
    later_ms(TimerDelay);
compute_deadline(_TimerDelay, ExistingDeadline) ->
    ExistingDeadline.

compute_timeout(infinity) ->
    ?HIBERNATE_AFTER;
compute_timeout(Deadline) ->
    DeltaMilliSec = time_diff(Deadline, now()) * 1000.0,
    if
        DeltaMilliSec =< 1 ->
            0;
        true ->
            round(DeltaMilliSec)
    end.

do_noreply(State = #pstate{deadline = Deadline}) ->
    {noreply, State, compute_timeout(Deadline)}.

do_reply(Reply, State = #pstate{deadline = Deadline}) ->
    {reply, Reply, State, compute_timeout(Deadline)}.

flush(State) -> flush(false, State).

flush(ForceSnapshot, State = #pstate{pending_logs = PendingLogs,
                                     pending_replies = Waiting,
                                     log_handle = LogHandle}) ->
    State1 = if PendingLogs /= [] ->
                     disk_log:alog(LogHandle, lists:reverse(PendingLogs)),
                     State#pstate{entry_count = State#pstate.entry_count + 1};
                true ->
                     State
             end,
    State2 = maybe_take_snapshot(ForceSnapshot, State1),
    if Waiting /= [] ->
            ok = disk_log:sync(LogHandle),
            lists:foreach(fun (From) -> gen_server:reply(From, ok) end,
                          Waiting);
       true ->
            ok
    end,
    State2#pstate{deadline = infinity,
                  pending_logs = [],
                  pending_replies = []}.

current_snapshot(_Snapshot = #psnapshot{serial = Serial,
                                        transactions= Ts,
                                        messages = Messages,
                                        queues = Queues}) ->
    %% Avoid infinite growth of the table by removing messages not
    %% bound to a queue anymore
    prune_table(Messages, ets:foldl(
                            fun ({{_QName, PKey}, _Delivered}, S) ->
                                    sets:add_element(PKey, S)
                            end, sets:new(), Queues)),
    InnerSnapshot = {{serial, Serial},
                     {txns, Ts},
                     {messages, ets:tab2list(Messages)},
                     {queues, ets:tab2list(Queues)}},
    ?LOGDEBUG("Inner snapshot: ~p~n", [InnerSnapshot]),
    {persist_snapshot, {vsn, ?PERSISTER_LOG_FORMAT_VERSION},
     term_to_binary(InnerSnapshot)}.

prune_table(Tab, Keys) ->
    true = ets:safe_fixtable(Tab, true),
    ok = prune_table(Tab, Keys, ets:first(Tab)),
    true = ets:safe_fixtable(Tab, false).
    
prune_table(_Tab, _Keys, '$end_of_table') -> ok;
prune_table(Tab, Keys, Key) ->
    case sets:is_element(Key, Keys) of
        true  -> ok;
        false -> ets:delete(Tab, Key)
    end,
    prune_table(Tab, Keys, ets:next(Tab, Key)).

internal_load_snapshot(LogHandle, 
                       Snapshot = #psnapshot{messages = Messages,
                                             queues = Queues}) ->
    {K, [Loaded_Snapshot | Items]} = disk_log:chunk(LogHandle, start),
    case check_version(Loaded_Snapshot) of
        {ok, StateBin} ->
            {{serial, Serial}, {txns, Ts}, {messages, Ms}, {queues, Qs}} =
                binary_to_term(StateBin),
            true = ets:insert(Messages, Ms),
            true = ets:insert(Queues, Qs),
            Snapshot1 = replay(Items, LogHandle, K,
                               Snapshot#psnapshot{
                                 serial = Serial,
                                 transactions = Ts}),
            Snapshot2 = requeue_messages(Snapshot1),
            %% uncompleted transactions are discarded - this is TRTTD
            %% since we only get into this code on node restart, so
            %% any uncompleted transactions will have been aborted.
            {ok, Snapshot2#psnapshot{transactions = dict:new()}};
        {error, Reason} -> {{error, Reason}, Snapshot}
    end.

check_version({persist_snapshot, {vsn, ?PERSISTER_LOG_FORMAT_VERSION},
               StateBin}) ->
    {ok, StateBin};
check_version({persist_snapshot, {vsn, Vsn}, _StateBin}) ->
    {error, {unsupported_persister_log_format, Vsn}};
check_version(_Other) ->
    {error, unrecognised_persister_log_format}.

requeue_messages(Snapshot = #psnapshot{messages = Messages,
                                       queues = Queues}) ->
    Work = ets:foldl(fun accumulate_requeues/2, dict:new(), Queues),
    %% unstable parallel map, because order doesn't matter
    L = lists:append(
          rabbit_misc:upmap(
            %% we do as much work as possible in spawned worker
            %% processes, but we need to make sure the ets:inserts are
            %% performed in self()
            fun ({QName, Requeues}) ->
                    requeue(QName, Requeues, Messages)
            end, dict:to_list(Work))),
    NewMessages = [{K, M} || {{_Q, K}, M, _D} <- L],
    NewQueues  = [{QK, D} || {QK, _M, D} <- L],
    ets:delete_all_objects(Messages),
    ets:delete_all_objects(Queues),
    true = ets:insert(Messages, NewMessages),
    true = ets:insert(Queues, NewQueues),
    %% contains the mutated messages and queues tables
    Snapshot.

accumulate_requeues({{QName, PKey}, Delivered}, Acc) ->
    Requeue = {PKey, Delivered},
    dict:update(QName,
                fun (Requeues) -> [Requeue | Requeues] end,
                [Requeue],
                Acc).

requeue(QName, Requeues, Messages) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, #amqqueue{pid = QPid}} ->
            RequeueMessages = 
                [{{QName, PKey}, Message, Delivered} ||
                    {PKey, Delivered} <- Requeues, 
                    {_, Message} <- ets:lookup(Messages, PKey)],
            rabbit_amqqueue:redeliver(
              QPid,
              %% Messages published by the same process receive
              %% persistence keys that are monotonically
              %% increasing. Since message ordering is defined on a
              %% per-channel basis, and channels are bound to specific
              %% processes, sorting the list does provide the correct
              %% ordering properties.
              [{Message, Delivered} || {_, Message, Delivered} <-
                                           lists:sort(RequeueMessages)]),
            RequeueMessages;
        {error, not_found} ->
            []
    end.

replay([], LogHandle, K, Snapshot) ->
    case disk_log:chunk(LogHandle, K) of
        {K1, Items} ->
            replay(Items, LogHandle, K1, Snapshot);
        {K1, Items, Badbytes} ->
            rabbit_log:warning("~p bad bytes recovering persister log~n", 
                               [Badbytes]),
            replay(Items, LogHandle, K1, Snapshot);
        eof -> Snapshot
    end;
replay([Item | Items], LogHandle, K, Snapshot) ->
    NewSnapshot = internal_integrate_messages(Item, Snapshot),
    replay(Items, LogHandle, K, NewSnapshot).

internal_integrate_messages(Items, Snapshot) ->
    lists:foldl(fun (Item, Snap) -> internal_integrate1(Item, Snap) end,
                Snapshot, Items).

internal_integrate1({extend_transaction, Key, MessageList},
                    Snapshot = #psnapshot {transactions = Transactions}) ->
    NewTransactions =
        dict:update(Key,
                    fun (MessageLists) -> [MessageList | MessageLists] end,
                    [MessageList],
                    Transactions),
    Snapshot#psnapshot{transactions = NewTransactions};
internal_integrate1({rollback_transaction, Key},
                    Snapshot = #psnapshot{transactions = Transactions}) ->
    Snapshot#psnapshot{transactions = dict:erase(Key, Transactions)};
internal_integrate1({commit_transaction, Key},
                    Snapshot = #psnapshot{transactions = Transactions,
                                          messages = Messages,
                                          queues = Queues}) ->
    case dict:find(Key, Transactions) of
        {ok, MessageLists} ->
            ?LOGDEBUG("persist committing txn ~p~n", [Key]),
            lists:foreach(fun (ML) -> perform_work(ML, Messages, Queues) end,
                          lists:reverse(MessageLists)),
            Snapshot#psnapshot{transactions = dict:erase(Key, Transactions)};
        error ->
            Snapshot
    end;
internal_integrate1({dirty_work, MessageList},
                    Snapshot = #psnapshot {messages = Messages,
                                           queues = Queues}) ->
    perform_work(MessageList, Messages, Queues),
    Snapshot.

perform_work(MessageList, Messages, Queues) ->
    lists:foreach(
      fun (Item) -> perform_work_item(Item, Messages, Queues) end,
      MessageList).

perform_work_item({publish, Message, QK = {_QName, PKey}}, Messages, Queues) ->
    ets:insert(Messages, {PKey, Message}),
    ets:insert(Queues, {QK, false});

perform_work_item({tied, QK}, _Messages, Queues) ->
    ets:insert(Queues, {QK, false});

perform_work_item({deliver, QK}, _Messages, Queues) ->
    %% from R12B-2 onward we could use ets:update_element/3 here
    ets:delete(Queues, QK),
    ets:insert(Queues, {QK, true});

perform_work_item({ack, QK}, _Messages, Queues) ->
    ets:delete(Queues, QK).
