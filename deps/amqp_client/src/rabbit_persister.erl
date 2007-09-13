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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
%%   Technologies Ltd.; 
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

-export([make_key/0,
         transaction/1, begin_transaction/0, extend_transaction/2,
         dirty_work/1,
         commit_transaction/1, rollback_transaction/1,
         force_snapshot/0]).

-include("rabbit.hrl").

-define(SERVER, ?MODULE).

-define(LOG_BUNDLE_DELAY, 5).
-define(COMMIT_BUNDLE_DELAY, 2).

-define(MAX_WRAP_ENTRIES, 500).

-record(pstate, {log_handle, entry_count, deadline,
                 pending_logs, pending_commits}).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

make_key() ->
    {pkey, node(), make_ref()}.

transaction(MessageList) ->
    ?LOGDEBUG("transaction ~p~n", [MessageList]),
    gen_server:call(?SERVER, {transaction, MessageList}).

begin_transaction() ->
    ?LOGDEBUG0("begin_transaction~n"),
    gen_server:call(?SERVER, begin_transaction).

extend_transaction(TxnKey, MessageList) ->
    ?LOGDEBUG("extend_transaction ~p ~p~n", [TxnKey, MessageList]),
    gen_server:call(?SERVER, {extend_transaction, TxnKey, MessageList}).

dirty_work(MessageList) ->
    ?LOGDEBUG("dirty_work ~p~n", [MessageList]),
    gen_server:cast(?SERVER, {dirty_work, MessageList}).

commit_transaction(TxnKey) ->
    ?LOGDEBUG("commit_transaction ~p~n", [TxnKey]),
    gen_server:call(?SERVER, {commit_transaction, TxnKey}).

rollback_transaction(TxnKey) ->
    ?LOGDEBUG("rollback_transaction ~p~n", [TxnKey]),
    gen_server:cast(?SERVER, {rollback_transaction, TxnKey}).

force_snapshot() ->
    gen_server:call(?SERVER, force_snapshot).

%%--------------------------------------------------------------------

init(_Args) ->
    process_flag(trap_exit, true),
    FileName = base_filename(),
    ok = filelib:ensure_dir(FileName),
    LogHandle =
        case disk_log:open([{name, rabbit_persister},
                            {head_func, {rabbit_gc_persist, current_snapshot, []}},
                            {file, FileName}
                           ]) of
            {ok, LH} -> LH;
            {repaired, LH, {recovered, Recovered}, {badbytes, Bad}} ->
                WarningFun = if
                                 Bad > 0 -> fun rabbit_log:warning/2;
                                 true -> fun rabbit_log:info/2
                             end,
                WarningFun("Repaired persister log - ~p recovered, ~p bad~n",
                           [Recovered, Bad]),
                LH
        end,
    rabbit_gc_persist:load_snapshot(LogHandle),
    ok = take_snapshot(LogHandle),
    {ok, #pstate{log_handle = LogHandle,
                 entry_count = 0,
                 deadline = infinity,
                 pending_logs = [],
                 pending_commits = []}}.

handle_call({transaction, MessageList}, From, State) ->
    {Key, NewState0} = internal_begin(State),
    NewState1 = internal_extend(Key, MessageList, NewState0),
    do_noreply(internal_commit(From, Key, NewState1));
handle_call(begin_transaction, _From, State) ->
    {Key, NewState} = internal_begin(State),
    do_reply(Key, NewState);
handle_call({extend_transaction, TxnKey, MessageList}, _From, State) ->
    do_reply(ok, internal_extend(TxnKey, MessageList, State));
handle_call({commit_transaction, TxnKey}, From, State) ->
    do_noreply(internal_commit(From, TxnKey, State));
handle_call(force_snapshot, _From, State) ->
    ok = take_snapshot(State#pstate.log_handle),
    do_reply(ok, State);
handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({dirty_work, MessageList}, State) ->
    do_noreply(internal_dirty_work(MessageList, State));
handle_cast({rollback_transaction, TxnKey}, State) ->
    do_noreply(internal_rollback(TxnKey, State));
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    {noreply, flush(State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State = #pstate{log_handle = LogHandle}) ->
    flush(State),
    disk_log:close(LogHandle),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, flush(State)}.

%%--------------------------------------------------------------------

internal_begin(State) ->
    Key = make_key(),
    {Key, log(State, {begin_transaction, Key})}.

internal_extend(Key, MessageList, State) ->
    log(State, {extend_transaction, Key, MessageList}).

internal_dirty_work(MessageList, State) ->
    log(State, {dirty_work, MessageList}).

internal_commit(From, Key, State = #pstate{deadline = ExistingDeadline,
                                           pending_logs = Logs,
                                           pending_commits = Waiting}) ->
    State#pstate{deadline = compute_deadline(?COMMIT_BUNDLE_DELAY, ExistingDeadline),
                 pending_logs = [{commit_transaction, Key} | Logs],
                 pending_commits = [{From, Key} | Waiting]}.

internal_rollback(Key, State) ->
    log(State, {rollback_transaction, Key}).

log(State = #pstate{deadline = ExistingDeadline, pending_logs = Logs}, Message) ->
    State#pstate{deadline = compute_deadline(?LOG_BUNDLE_DELAY, ExistingDeadline),
                 pending_logs = [Message | Logs]}.

base_filename() ->
    mnesia:system_info(directory) ++ "/rabbit_persister.LOG".

take_snapshot(LogHandle) ->
    OldFileName = base_filename() ++ ".previous",
    file:delete(OldFileName),
    rabbit_log:info("Rolling persister log to ~p~n", [OldFileName]),
    ok = disk_log:sync(LogHandle),
    ok = disk_log:reopen(LogHandle, OldFileName),
    ok.

maybe_take_snapshot(State = #pstate{entry_count = EntryCount})
  when EntryCount >= ?MAX_WRAP_ENTRIES ->
    ok = take_snapshot(State#pstate.log_handle),
    State#pstate{entry_count = 0};
maybe_take_snapshot(State) ->
    State.

later_ms(DeltaMilliSec) ->
    {MegaSec, Sec, MicroSec} = now(),
    % Note: not normalised. Unimportant for this application.
    {MegaSec, Sec, MicroSec + (DeltaMilliSec * 1000)}.

%% Result = B - A, more or less
time_diff({B1,B2,B3}, {A1,A2,A3}) ->
    (B1 - A1) * 1000000 + (B2 - A2) + (B3 - A3) / 1000000.0 .

compute_deadline(TimerDelay, infinity) ->
    later_ms(TimerDelay);
compute_deadline(_TimerDelay, ExistingDeadline) ->
    ExistingDeadline.

compute_timeout(infinity) ->
    infinity;
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

flush(State = #pstate{pending_logs = PendingLogs,
                      pending_commits = PendingCommits,
                      log_handle = LogHandle}) ->
    State1 = if
                 PendingLogs /= [] ->
                     Unit = {most_recent_first, PendingLogs},
                     rabbit_gc_persist:integrate_messages(Unit),
                     disk_log:alog(LogHandle, Unit),
                     maybe_take_snapshot(State#pstate{entry_count = State#pstate.entry_count + 1});
                 true ->
                     State
             end,
    if
        PendingCommits /= [] ->
            ok = disk_log:sync(LogHandle),
            lists:foreach(fun ({From, _Key}) ->
                                  gen_server:reply(From, ok)
                          end, PendingCommits);
        true ->
            ok
    end,
    State1#pstate{deadline = infinity,
                  pending_logs = [],
                  pending_commits = []}.

