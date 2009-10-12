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

-module(rabbit_disk_queue).

-behaviour(gen_server2).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([publish/3, fetch/1, phantom_fetch/1, ack/2, tx_publish/1, tx_commit/3,
         tx_rollback/1, requeue/2, purge/1, delete_queue/1,
         delete_non_durable_queues/1, requeue_next_n/2, len/1, foldl/3,
         prefetch/1
        ]).

-export([stop/0, stop_and_obliterate/0]).

%%----------------------------------------------------------------------------

-include("rabbit.hrl").

-define(SEQUENCE_ETS_NAME,       rabbit_disk_queue_sequences).
-define(BATCH_SIZE,              10000).

-define(SHUTDOWN_MESSAGE_KEY, {internal_token, shutdown}).
-define(SHUTDOWN_MESSAGE,
        #dq_msg_loc { queue_and_seq_id = ?SHUTDOWN_MESSAGE_KEY,
                      msg_id = infinity_and_beyond,
                      is_delivered = never,
                      is_persistent = true
                    }).

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

-define(SERVER, ?MODULE).

-record(dqstate, { sequences }).      %% next read and write for each q

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(msg_id() :: guid()).
-type(seq_id() :: non_neg_integer()).
-type(ack_tag() :: {msg_id(), seq_id()}).

-spec(start_link/0 :: () ->
              ({'ok', pid()} | 'ignore' | {'error', any()})).
-spec(publish/3 :: (queue_name(), message(), boolean()) -> 'ok').
-spec(fetch/1 :: (queue_name()) ->
             ('empty' |
              {message(), boolean(), ack_tag(), non_neg_integer()})).
-spec(phantom_fetch/1 :: (queue_name()) ->
             ('empty' |
              {msg_id(), boolean(), ack_tag(), non_neg_integer()})).
-spec(prefetch/1 :: (queue_name()) -> 'ok').
-spec(ack/2 :: (queue_name(), [ack_tag()]) -> 'ok').
-spec(tx_publish/1 :: (message()) -> 'ok').
-spec(tx_commit/3 :: (queue_name(), [{msg_id(), boolean(), boolean()}],
                      [ack_tag()]) ->
             'ok').
-spec(tx_rollback/1 :: ([msg_id()]) -> 'ok').
-spec(requeue/2 :: (queue_name(), [{ack_tag(), boolean()}]) -> 'ok').
-spec(requeue_next_n/2 :: (queue_name(), non_neg_integer()) -> 'ok').
-spec(purge/1 :: (queue_name()) -> non_neg_integer()).
-spec(delete_queue/1 :: (queue_name()) -> 'ok').
-spec(delete_non_durable_queues/1 :: ([queue_name()]) -> 'ok').
-spec(len/1 :: (queue_name()) -> non_neg_integer()).
-spec(foldl/3 :: (fun ((message(), ack_tag(), boolean(), A) -> A),
                  A, queue_name()) -> A).
-spec(stop/0 :: () -> 'ok').
-spec(stop_and_obliterate/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% public API
%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

publish(Q, Message = #basic_message {}, IsDelivered) ->
    gen_server2:cast(?SERVER, {publish, Q, Message, IsDelivered}).

fetch(Q) ->
    gen_server2:call(?SERVER, {fetch, Q}, infinity).

phantom_fetch(Q) ->
    gen_server2:call(?SERVER, {phantom_fetch, Q}, infinity).

prefetch(Q) ->
    gen_server2:pcast(?SERVER, -1, {prefetch, Q, self()}).

ack(Q, MsgSeqIds) when is_list(MsgSeqIds) ->
    gen_server2:cast(?SERVER, {ack, Q, MsgSeqIds}).

tx_publish(Message = #basic_message {}) ->
    gen_server2:cast(?SERVER, {tx_publish, Message}).

tx_commit(Q, PubMsgIds, AckSeqIds)
  when is_list(PubMsgIds) andalso is_list(AckSeqIds) ->
    gen_server2:call(?SERVER, {tx_commit, Q, PubMsgIds, AckSeqIds}, infinity).

tx_rollback(MsgIds) when is_list(MsgIds) ->
    gen_server2:cast(?SERVER, {tx_rollback, MsgIds}).

requeue(Q, MsgSeqIds) when is_list(MsgSeqIds) ->
    gen_server2:cast(?SERVER, {requeue, Q, MsgSeqIds}).

requeue_next_n(Q, N) when is_integer(N) ->
    gen_server2:cast(?SERVER, {requeue_next_n, Q, N}).

purge(Q) ->
    gen_server2:call(?SERVER, {purge, Q}, infinity).

delete_queue(Q) ->
    gen_server2:call(?SERVER, {delete_queue, Q}, infinity).

delete_non_durable_queues(DurableQueues) ->
    gen_server2:call(?SERVER, {delete_non_durable_queues, DurableQueues},
                     infinity).

len(Q) ->
    gen_server2:call(?SERVER, {len, Q}, infinity).

foldl(Fun, Init, Acc) ->
    gen_server2:call(?SERVER, {foldl, Fun, Init, Acc}, infinity).

stop() ->
    gen_server2:call(?SERVER, stop, infinity).

stop_and_obliterate() ->
    gen_server2:call(?SERVER, stop_vaporise, infinity).

%% private

finalise_commit(TxDetails) ->
    gen_server2:cast(?SERVER, {finalise_commit, TxDetails}).

%%----------------------------------------------------------------------------
%% gen_server behaviour
%%----------------------------------------------------------------------------

init([]) ->
    %% If the gen_server is part of a supervision tree and is ordered
    %% by its supervisor to terminate, terminate will be called with
    %% Reason=shutdown if the following conditions apply:
    %%     * the gen_server has been set to trap exit signals, and
    %%     * the shutdown strategy as defined in the supervisor's
    %%       child specification is an integer timeout value, not
    %%       brutal_kill.
    %% Otherwise, the gen_server will be immediately terminated.
    process_flag(trap_exit, true),

    ok = filelib:ensure_dir(form_filename("nothing")),

    ok = detect_shutdown_state_and_adjust_delivered_flags(),

    {ok, _Pid} = rabbit_msg_store:start_link(base_directory(),
                                             fun msg_ref_gen/1,
                                             msg_ref_gen_init()),
    ok = prune(),

    Sequences = ets:new(?SEQUENCE_ETS_NAME, [set, private]),
    ok = extract_sequence_numbers(Sequences),

    State = #dqstate { sequences = Sequences },
    {ok, State, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({fetch, Q}, _From, State) ->
    {Result, State1} = internal_fetch_body(Q, pop_queue, State),
    reply(Result, State1);
handle_call({phantom_fetch, Q}, _From, State) ->
    Result = internal_fetch_attributes(Q, record_delivery, State),
    reply(Result, State);
handle_call({tx_commit, Q, PubMsgIds, AckSeqIds}, From, State) ->
    State1 =
        internal_tx_commit(Q, PubMsgIds, AckSeqIds, From, State),
    noreply(State1);
handle_call({purge, Q}, _From, State) ->
    {ok, Count, State1} = internal_purge(Q, State),
    reply(Count, State1);
handle_call({delete_queue, Q}, From, State) ->
    gen_server2:reply(From, ok),
    {ok, State1} = internal_delete_queue(Q, State),
    noreply(State1);
handle_call({len, Q}, _From, State = #dqstate { sequences = Sequences }) ->
    {ReadSeqId, WriteSeqId} = sequence_lookup(Sequences, Q),
    reply(WriteSeqId - ReadSeqId, State);
handle_call({foldl, Fun, Init, Q}, _From, State) ->
    {ok, Result, State1} = internal_foldl(Q, Fun, Init, State),
    reply(Result, State1);
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}; %% gen_server now calls terminate
handle_call(stop_vaporise, _From, State) ->
    State1 = shutdown(State),
    {atomic, ok} = mnesia:clear_table(rabbit_disk_queue),
    lists:foreach(fun file:delete/1, filelib:wildcard(form_filename("*"))),
    {stop, normal, ok, State1}; %% gen_server now calls terminate
handle_call({delete_non_durable_queues, DurableQueues}, _From, State) ->
    {ok, State1} = internal_delete_non_durable_queues(DurableQueues, State),
    reply(ok, State1).

handle_cast({publish, Q, Message, IsDelivered}, State) ->
    {ok, _MsgSeqId, State1} = internal_publish(Q, Message, IsDelivered, State),
    noreply(State1);
handle_cast({ack, Q, MsgSeqIds}, State) ->
    {ok, State1} = internal_ack(Q, MsgSeqIds, State),
    noreply(State1);
handle_cast({tx_publish, Message}, State) ->
    {ok, State1} = internal_tx_publish(Message, State),
    noreply(State1);
handle_cast({tx_rollback, MsgIds}, State) ->
    {ok, State1} = internal_tx_rollback(MsgIds, State),
    noreply(State1);
handle_cast({requeue, Q, MsgSeqIds}, State) ->
    {ok, State1} = internal_requeue(Q, MsgSeqIds, State),
    noreply(State1);
handle_cast({requeue_next_n, Q, N}, State) ->
    {ok, State1} = internal_requeue_next_n(Q, N, State),
    noreply(State1);
handle_cast({prefetch, Q, From}, State) ->
    {Result, State1} = internal_fetch_body(Q, peek_queue, State),
    case rabbit_misc:with_exit_handler(
           fun () -> false end,
           fun () ->
                   ok = rabbit_queue_prefetcher:publish(From, Result),
                   true
           end) of
        true ->
            internal_fetch_attributes(Q, ignore_delivery, State1);
        false -> ok
    end,
    noreply(State1);
handle_cast({finalise_commit, TxDetails}, State) ->
    noreply(finalise_commit(TxDetails, State)).

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State}.

terminate(_Reason, State) ->
    State1 = shutdown(State),
    store_safe_shutdown(),
    State1.

shutdown(State = #dqstate { sequences = undefined }) ->
    State;
shutdown(State = #dqstate { sequences = Sequences }) ->
    ok = rabbit_msg_store:stop(),
    ets:delete(Sequences),
    State #dqstate { sequences = undefined }.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% general helper functions
%%----------------------------------------------------------------------------

noreply(State) ->
    {noreply, State, hibernate}.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

form_filename(Name) ->
    filename:join(base_directory(), Name).

base_directory() ->
    filename:join(rabbit_mnesia:dir(), "rabbit_disk_queue/").

sequence_lookup(Sequences, Q) ->
    case ets:lookup(Sequences, Q) of
        []                           -> {0, 0};
        [{_, ReadSeqId, WriteSeqId}] -> {ReadSeqId, WriteSeqId}
    end.

%%----------------------------------------------------------------------------
%% internal functions
%%----------------------------------------------------------------------------

internal_fetch_body(Q, Advance, State) ->
    case next(Q, record_delivery, Advance, State) of
        empty -> {empty, State};
        {MsgId, IsDelivered, AckTag, Remaining} ->
            {ok, Message} = rabbit_msg_store:read(MsgId),
            {{Message, IsDelivered, AckTag, Remaining}, State}
    end.

internal_fetch_attributes(Q, MarkDelivered, State) ->
    next(Q, MarkDelivered, pop_queue, State).

next(Q, MarkDelivered, Advance, #dqstate { sequences = Sequences }) ->
    case sequence_lookup(Sequences, Q) of
        {SeqId, SeqId} -> empty;
        {ReadSeqId, WriteSeqId} when WriteSeqId > ReadSeqId ->
            Remaining = WriteSeqId - ReadSeqId - 1,
            {MsgId, IsDelivered} =
                update_message_attributes(Q, ReadSeqId, MarkDelivered),
            ok = maybe_advance(Advance, Sequences, Q, ReadSeqId, WriteSeqId),
            AckTag = {MsgId, ReadSeqId},
            {MsgId, IsDelivered, AckTag, Remaining}
    end.

update_message_attributes(Q, SeqId, MarkDelivered) ->
    [Obj =
     #dq_msg_loc {is_delivered = IsDelivered, msg_id = MsgId}] =
        mnesia:dirty_read(rabbit_disk_queue, {Q, SeqId}),
    ok = case {IsDelivered, MarkDelivered} of
             {true, _} -> ok;
             {false, ignore_delivery} -> ok;
             {false, record_delivery} ->
                 mnesia:dirty_write(rabbit_disk_queue,
                                    Obj #dq_msg_loc {is_delivered = true})
         end,
    {MsgId, IsDelivered}.

maybe_advance(peek_queue, _, _, _, _) ->
    ok;
maybe_advance(pop_queue, Sequences, Q, ReadSeqId, WriteSeqId) ->
    true = ets:insert(Sequences, {Q, ReadSeqId + 1, WriteSeqId}),
    ok.

internal_foldl(Q, Fun, Init, State = #dqstate { sequences = Sequences }) ->
    {ReadSeqId, WriteSeqId} = sequence_lookup(Sequences, Q),
    internal_foldl(Q, WriteSeqId, Fun, State, Init, ReadSeqId).

internal_foldl(_Q, SeqId, _Fun, State, Acc, SeqId) ->
    {ok, Acc, State};
internal_foldl(Q, WriteSeqId, Fun, State, Acc, ReadSeqId) ->
    [#dq_msg_loc {is_delivered = IsDelivered, msg_id = MsgId}] =
        mnesia:dirty_read(rabbit_disk_queue, {Q, ReadSeqId}),
    {ok, Message} = rabbit_msg_store:read(MsgId),
    Acc1 = Fun(Message, {MsgId, ReadSeqId}, IsDelivered, Acc),
    internal_foldl(Q, WriteSeqId, Fun, State, Acc1, ReadSeqId + 1).

internal_ack(Q, MsgSeqIds, State) ->
    remove_messages(Q, MsgSeqIds, State).

remove_messages(Q, MsgSeqIds, State) ->
    MsgIds = lists:foldl(
               fun ({MsgId, SeqId}, MsgIdAcc) ->
                       ok = mnesia:dirty_delete(rabbit_disk_queue, {Q, SeqId}),
                       [MsgId | MsgIdAcc]
               end, [], MsgSeqIds),
    ok = rabbit_msg_store:remove(MsgIds),
    {ok, State}.

internal_tx_publish(Message = #basic_message { guid = MsgId,
                                               content = Content }, State) ->
    ClearedContent = rabbit_binary_parser:clear_decoded_content(Content),
    ok = rabbit_msg_store:write(
           MsgId, Message #basic_message { content = ClearedContent }),
    {ok, State}.

internal_tx_commit(Q, PubMsgIds, AckSeqIds, From, State) ->
    TxDetails = {Q, PubMsgIds, AckSeqIds, From},
    ok = rabbit_msg_store:sync([MsgId || {MsgId, _, _} <- PubMsgIds],
                               fun () -> finalise_commit(TxDetails) end),
    State.

finalise_commit({Q, PubMsgIds, AckSeqIds, From},
                State = #dqstate { sequences = Sequences }) ->
    {InitReadSeqId, InitWriteSeqId} = sequence_lookup(Sequences, Q),
    WriteSeqId =
        rabbit_misc:execute_mnesia_transaction(
          fun() ->
                  ok = mnesia:write_lock_table(rabbit_disk_queue),
                  lists:foldl(
                    fun ({MsgId, IsDelivered, IsPersistent}, SeqId) ->
                            ok = mnesia:write(
                                   rabbit_disk_queue,
                                   #dq_msg_loc {
                                     queue_and_seq_id = {Q, SeqId},
                                     msg_id           = MsgId,
                                     is_delivered     = IsDelivered,
                                     is_persistent    = IsPersistent
                                    }, write),
                            SeqId + 1
                    end, InitWriteSeqId, PubMsgIds)
          end),
    {ok, State1} = remove_messages(Q, AckSeqIds, State),
    true = case PubMsgIds of
               [] -> true;
               _  -> ets:insert(Sequences, 
                                {Q, InitReadSeqId, WriteSeqId})
           end,
    gen_server2:reply(From, ok),
    State1.

internal_publish(Q, Message = #basic_message { guid = MsgId,
                                               is_persistent = IsPersistent },
                 IsDelivered, State) ->
    {ok, State1 = #dqstate { sequences = Sequences }} =
        internal_tx_publish(Message, State),
    {ReadSeqId, WriteSeqId} = sequence_lookup(Sequences, Q),
    ok = mnesia:dirty_write(rabbit_disk_queue,
                            #dq_msg_loc { queue_and_seq_id = {Q, WriteSeqId},
                                          msg_id = MsgId,
                                          is_delivered = IsDelivered,
                                          is_persistent = IsPersistent }),
    true = ets:insert(Sequences, {Q, ReadSeqId, WriteSeqId + 1}),
    {ok, {MsgId, WriteSeqId}, State1}.

internal_tx_rollback(MsgIds, State) ->
    ok = rabbit_msg_store:remove(MsgIds),
    {ok, State}.

internal_requeue(_Q, [], State) ->
    {ok, State};
internal_requeue(Q, MsgSeqIds, State = #dqstate { sequences = Sequences }) ->
    %% We know that every seq_id in here is less than the ReadSeqId
    %% you'll get if you look up this queue in Sequences (i.e. they've
    %% already been delivered). We also know that the rows for these
    %% messages are still in rabbit_disk_queue (i.e. they've not been
    %% ack'd).
    %%
    %% Now, it would be nice if we could adjust the sequence ids in
    %% rabbit_disk_queue (mnesia) to create a contiguous block and
    %% then drop the ReadSeqId for the queue by the corresponding
    %% amount. However, this is not safe because there may be other
    %% sequence ids which have been sent out as part of deliveries
    %% which are not being requeued. As such, moving things about in
    %% rabbit_disk_queue _under_ the current ReadSeqId would result in
    %% such sequence ids referring to the wrong messages.
    %%
    %% Therefore, the only solution is to take these messages, and to
    %% reenqueue them at the top of the queue. Usefully, this only
    %% affects the Sequences and rabbit_disk_queue structures - there
    %% is no need to physically move the messages about on disk, so
    %% the message store remains unaffected, except we need to tell it
    %% about the ids of the requeued messages so it can remove them
    %% from its message cache if necessary.

    {ReadSeqId, WriteSeqId} = sequence_lookup(Sequences, Q),
    {WriteSeqId1, Q, MsgIds} =
        rabbit_misc:execute_mnesia_transaction(
          fun() ->
                  ok = mnesia:write_lock_table(rabbit_disk_queue),
                  lists:foldl(fun requeue_message/2, {WriteSeqId, Q, []},
                              MsgSeqIds)
          end),
    true = ets:insert(Sequences, {Q, ReadSeqId, WriteSeqId1}),
    ok = rabbit_msg_store:release(MsgIds),
    {ok, State}.

requeue_message({{MsgId, SeqId}, IsDelivered}, {WriteSeqId, Q, Acc}) ->
    [Obj = #dq_msg_loc { is_delivered = true, msg_id = MsgId }] =
        mnesia:read(rabbit_disk_queue, {Q, SeqId}, write),
    ok = mnesia:write(rabbit_disk_queue,
                      Obj #dq_msg_loc {queue_and_seq_id = {Q, WriteSeqId},
                                       is_delivered = IsDelivered
                                      },
                      write),
    ok = mnesia:delete(rabbit_disk_queue, {Q, SeqId}, write),
    {WriteSeqId + 1, Q, [MsgId | Acc]}.

%% move the next N messages from the front of the queue to the back.
internal_requeue_next_n(Q, N, State = #dqstate { sequences = Sequences }) ->
    {ReadSeqId, WriteSeqId} = sequence_lookup(Sequences, Q),
    if N >= (WriteSeqId - ReadSeqId) -> {ok, State};
       true ->
            {ReadSeqIdN, WriteSeqIdN, MsgIds} =
                rabbit_misc:execute_mnesia_transaction(
                  fun() ->
                          ok = mnesia:write_lock_table(rabbit_disk_queue),
                          requeue_next_messages(Q, N, ReadSeqId, WriteSeqId, [])
                  end
                 ),
            true = ets:insert(Sequences, {Q, ReadSeqIdN, WriteSeqIdN}),
            ok = rabbit_msg_store:release(MsgIds),
            {ok, State}
    end.

requeue_next_messages(_Q, 0, ReadSeq, WriteSeq, Acc) ->
    {ReadSeq, WriteSeq, Acc};
requeue_next_messages(Q, N, ReadSeq, WriteSeq, Acc) ->
    [Obj = #dq_msg_loc { msg_id = MsgId }] =
        mnesia:read(rabbit_disk_queue, {Q, ReadSeq}, write),
    ok = mnesia:write(rabbit_disk_queue,
                      Obj #dq_msg_loc {queue_and_seq_id = {Q, WriteSeq}},
                      write),
    ok = mnesia:delete(rabbit_disk_queue, {Q, ReadSeq}, write),
    requeue_next_messages(Q, N - 1, ReadSeq + 1, WriteSeq + 1, [MsgId | Acc]).

internal_purge(Q, State = #dqstate { sequences = Sequences }) ->
    case sequence_lookup(Sequences, Q) of
        {SeqId, SeqId} -> {ok, 0, State};
        {ReadSeqId, WriteSeqId} ->
            {MsgSeqIds, WriteSeqId} =
                rabbit_misc:unfold(
                  fun (SeqId) when SeqId == WriteSeqId -> false;
                      (SeqId) ->
                          [#dq_msg_loc { msg_id = MsgId }] =
                              mnesia:dirty_read(rabbit_disk_queue, {Q, SeqId}),
                          {true, {MsgId, SeqId}, SeqId + 1}
                  end, ReadSeqId),
            true = ets:insert(Sequences, {Q, WriteSeqId, WriteSeqId}),
            {ok, State1} = remove_messages(Q, MsgSeqIds, State),
            {ok, WriteSeqId - ReadSeqId, State1}
    end.

internal_delete_queue(Q, State) ->
    %% remove everything undelivered
    {ok, _Count, State1 = #dqstate { sequences = Sequences }} =
        internal_purge(Q, State),
    true = ets:delete(Sequences, Q),
    %% remove everything already delivered
    remove_messages(
      Q, [{MsgId, SeqId} || #dq_msg_loc { queue_and_seq_id = {_Q, SeqId},
                                          msg_id = MsgId } <-
                                mnesia:dirty_match_object(
                                  rabbit_disk_queue,
                                  #dq_msg_loc {
                                    queue_and_seq_id = {Q, '_'},
                                    _ = '_' })], State1).

internal_delete_non_durable_queues(
  DurableQueues, State = #dqstate { sequences = Sequences }) ->
    DurableQueueSet =  sets:from_list(DurableQueues),
    ets:foldl(
      fun ({Q, _Read, _Write}, {ok, State1}) ->
              case sets:is_element(Q, DurableQueueSet) of
                  true -> {ok, State1};
                  false -> internal_delete_queue(Q, State1)
              end
      end, {ok, State}, Sequences).

%%----------------------------------------------------------------------------
%% recovery
%%----------------------------------------------------------------------------

store_safe_shutdown() ->
    ok = rabbit_misc:execute_mnesia_transaction(
           fun() ->
                   mnesia:write(rabbit_disk_queue,
                                ?SHUTDOWN_MESSAGE, write)
           end).

detect_shutdown_state_and_adjust_delivered_flags() ->
    MarkDelivered =
        rabbit_misc:execute_mnesia_transaction(
          fun() ->
                  case mnesia:read(rabbit_disk_queue,
                                   ?SHUTDOWN_MESSAGE_KEY, read) of
                      [?SHUTDOWN_MESSAGE] ->
                          mnesia:delete(rabbit_disk_queue,
                                        ?SHUTDOWN_MESSAGE_KEY, write),
                          false;
                      [] ->
                          true
                  end
          end),
    %% if we crash here, then on startup we'll not find the
    %% SHUTDOWN_MESSAGE so will mark everything delivered, which is
    %% the safe thing to do.
    case MarkDelivered of
        true -> mark_messages_delivered();
        false -> ok
    end.

mark_messages_delivered() ->
    mark_message_delivered('$start_of_table').

%% A single huge transaction is a bad idea because of memory
%% use. Equally, using dirty operations is a bad idea because you
%% shouldn't do writes when doing mnesia:dirty_next, because the
%% ordering can change. So we use transactions of bounded
%% size. However, even this does necessitate restarting between
%% transactions.
mark_message_delivered('$end_of_table') ->
    ok;
mark_message_delivered(_Key) ->
    mark_message_delivered(
      rabbit_misc:execute_mnesia_transaction(
        fun () ->
                ok = mnesia:write_lock_table(rabbit_disk_queue),
                mark_message_delivered(mnesia:first(rabbit_disk_queue),
                                       ?BATCH_SIZE)
        end)).

mark_message_delivered(Key, 0) ->
    Key;
mark_message_delivered(Key = '$end_of_table', _N) ->
    Key;
mark_message_delivered(Key, N) ->
    [Obj] = mnesia:read(rabbit_disk_queue, Key, write),
    M = case Obj #dq_msg_loc.is_delivered of
            true -> N;
            false ->
                ok = mnesia:write(rabbit_disk_queue,
                                  Obj #dq_msg_loc { is_delivered = true },
                                  write),
                N - 1
        end,
    mark_message_delivered(mnesia:next(rabbit_disk_queue, Key), M).

msg_ref_gen_init() -> mnesia:dirty_first(rabbit_disk_queue).

msg_ref_gen('$end_of_table') -> finished;
msg_ref_gen(Key) ->
    [#dq_msg_loc { msg_id = MsgId, is_persistent = IsPersistent }] =
        mnesia:dirty_read(rabbit_disk_queue, Key),
    NextKey = mnesia:dirty_next(rabbit_disk_queue, Key),
    {MsgId, case IsPersistent of true -> 1; false -> 0 end, NextKey}.

prune_flush_batch(DeleteAcc) ->
    lists:foldl(fun (Key, ok) ->
                        mnesia:dirty_delete(rabbit_disk_queue, Key)
                end, ok, DeleteAcc).

prune() ->
    prune(mnesia:dirty_first(rabbit_disk_queue), [], 0).

prune('$end_of_table', DeleteAcc, _Len) ->
    prune_flush_batch(DeleteAcc);
prune(Key, DeleteAcc, Len) ->
    [#dq_msg_loc { msg_id = MsgId, queue_and_seq_id = {Q, SeqId} }] =
        mnesia:dirty_read(rabbit_disk_queue, Key),
    {DeleteAcc1, Len1} =
        case rabbit_msg_store:contains(MsgId) of
            true  -> {DeleteAcc, Len};
            false -> {[{Q, SeqId} | DeleteAcc], Len + 1}
        end,
    if Len1 >= ?BATCH_SIZE ->
            %% We have no way of knowing how flushing the batch will
            %% affect ordering of records within the table, so have no
            %% choice but to start again. Although this will make
            %% recovery slower for large queues, we guarantee we can
            %% start up in constant memory
            ok = prune_flush_batch(DeleteAcc1),
            NextKey = mnesia:dirty_first(rabbit_disk_queue),
            prune(NextKey, [], 0);
       true ->
            NextKey = mnesia:dirty_next(rabbit_disk_queue, Key),
            prune(NextKey, DeleteAcc1, Len1)
    end.

extract_sequence_numbers(Sequences) ->
    true =
        rabbit_misc:execute_mnesia_transaction(
          %% the ets manipulation within this transaction is
          %% idempotent, in particular we're only reading from mnesia,
          %% and combining what we read with what we find in
          %% ets. Should the transaction restart, the non-rolledback
          %% data in ets can still be successfully combined with what
          %% we find in mnesia
          fun() ->
                  ok = mnesia:read_lock_table(rabbit_disk_queue),
                  mnesia:foldl(
                    fun (#dq_msg_loc { queue_and_seq_id = {Q, SeqId} }, true) ->
                            NextWrite = SeqId + 1,
                            case ets:lookup(Sequences, Q) of
                                [] -> ets:insert_new(Sequences,
                                                     {Q, SeqId, NextWrite});
                                [Orig = {_, Read, Write}] ->
                                    Repl = {Q, lists:min([Read, SeqId]),
                                            lists:max([Write, NextWrite])},
                                    case Orig == Repl of
                                        true -> true;
                                        false -> ets:insert(Sequences, Repl)
                                    end
                            end
                    end, true, rabbit_disk_queue)
          end),
    ok = remove_gaps_in_sequences(Sequences).

remove_gaps_in_sequences(Sequences) ->
    %% read the comments at internal_requeue.

    %% Because we are at startup, we know that no sequence ids have
    %% been issued (or at least, they were, but have been
    %% forgotten). Therefore, we can nicely shuffle up and not
    %% worry. Note that I'm choosing to shuffle up, but alternatively
    %% we could shuffle downwards. However, I think there's greater
    %% likelihood of gaps being at the bottom rather than the top of
    %% the queue, so shuffling up should be the better bet.
    QueueBoundaries =
        rabbit_misc:execute_mnesia_transaction(
          fun() ->
                  ok = mnesia:write_lock_table(rabbit_disk_queue),
                  lists:foldl(
                    fun ({Q, ReadSeqId, WriteSeqId}, Acc) ->
                            Gap = shuffle_up(Q, ReadSeqId-1, WriteSeqId-1, 0),
                            [{Q, ReadSeqId + Gap, WriteSeqId} | Acc]
                    end, [], ets:match_object(Sequences, '_'))
          end),
    true = lists:foldl(fun (Obj, true) -> ets:insert(Sequences, Obj) end,
                       true, QueueBoundaries),
    ok.

shuffle_up(_Q, SeqId, SeqId, Gap) ->
    Gap;
shuffle_up(Q, BaseSeqId, SeqId, Gap) ->
    GapInc =
        case mnesia:read(rabbit_disk_queue, {Q, SeqId}, write) of
            [] -> 1;
            [Obj] ->
                case Gap of
                    0 -> ok;
                    _ -> mnesia:write(rabbit_disk_queue,
                                      Obj #dq_msg_loc {
                                        queue_and_seq_id = {Q, SeqId + Gap }},
                                      write),
                         mnesia:delete(rabbit_disk_queue, {Q, SeqId}, write)
                end,
                0
        end,
    shuffle_up(Q, BaseSeqId, SeqId - 1, Gap + GapInc).
