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
%% Copyright (c) 2007-2011 VMware, Inc. All rights reserved.
%%

-module(rabbit_mnesia_queue).

-export(
   [start/1, stop/0, init/5, terminate/1, delete_and_terminate/1, purge/1,
    publish/3, publish_delivered/4, drain_confirmed/1, fetch/2, ack/2,
    tx_publish/4, tx_ack/3, tx_rollback/2, tx_commit/4, requeue/3, len/1,
    is_empty/1, dropwhile/2, set_ram_duration_target/2, ram_duration/1,
    needs_idle_timeout/1, idle_timeout/1, handle_pre_hibernate/1, status/1]).

%% ----------------------------------------------------------------------------
%% This is a simple implementation of the rabbit_backing_queue
%% behavior, with all msgs in Mnesia.
%%
%% This will eventually be structured as a plug-in instead of an extra
%% module in the middle of the server tree....
%% ----------------------------------------------------------------------------

%% ----------------------------------------------------------------------------
%% This module wraps msgs into msg_status records for internal use,
%% including additional information. Pending acks are also recorded as
%% msg_status records. These are both stored in Mnesia.
%%
%% All queues are durable in this version, and all msgs are treated as
%% persistent. (This may break some clients and some tests for
%% non-durable queues.)
%% ----------------------------------------------------------------------------

%% BUG: The rabbit_backing_queue_spec behaviour needs improvement. For
%% example, there are points in the protocol where failures can lose
%% msgs.

%% TODO: Need to provide better back-pressure when queue is filling up.

%% BUG: Should not use mnesia:all_keys to count entries.

%% BUG: p_records do not need a separate seq_id.

%% TODO: Worry about dropping txn_dict upon failure.

-behaviour(rabbit_backing_queue).

%% The state record is the in-RAM AMQP queue state. It contains the
%% names of two Mnesia queues; the next_seq_id; and the AMQP
%% transaction dict (which can be dropped on a crash).

-record(state,              % The in-RAM queue state
        { q_table,          % The Mnesia queue table name
          p_table,          % The Mnesia pending-ack table name
          next_seq_id,      % The next M's seq_id
          confirmed,        % The set of msgs recently confirmed
          txn_dict          % In-progress txn->tx map
        }).

%% An msg_status record is a wrapper around a msg. It contains a
%% seq_id, assigned when the msg is published; the msg itself; the
%% msg's props, as presented by the client or as transformed by the
%% client; and an is-delivered flag, for reporting.

-record(msg_status,         % A wrapper aroung a msg
        { seq_id,           % The seq_id for the msg
          msg,              % The msg itself
          props,            % The msg properties
          is_delivered      % Has the msg been delivered? (for reporting)
        }).

%% A TX record is the value stored in the txn_dict. It contains a list
%% of (msg, props) pairs to be published after the AMQP transaction,
%% in reverse order, and a list of seq_ids to ack after the AMQP
%% transaction, in any order. No other write-operations are allowed in
%% AMQP transactions, and the effects of these operations are not
%% visible to the client until after the AMQP transaction commits.

-record(tx,
        { to_pub,           % List of (msg, props) pairs to publish
          to_ack            % List of seq_ids to ack
        }).

%% A Q record is a msg stored in the Q table in Mnesia. It is indexed
%% by the out-id, which orders msgs; and contains the msg_status
%% record itself. We push msg_status records with a new high seq_id,
%% and pop the msg_status record with the lowest seq_id.

-record(q_record,           % Q records in Mnesia
        { seq_id,           % The key: The seq_id
          msg_status        % The value: The msg_status record
          }).

%% A P record is a pending-ack stored in the P table in Mnesia. It is
%% indexed by the seq_id, and contains the msg_status record
%% itself. It is randomly accessed by seq_id.

-record(p_record,           % P records in Mnesia
        { seq_id,           % The key: The seq_id
          msg_status        % The value: The msg_status record
          }).

-include("rabbit.hrl").

%% ----------------------------------------------------------------------------

%% BUG: Restore -ifdef, -endif.

%% -ifdef(use_specs).

-type(maybe(T) :: nothing | {just, T}).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id()).

-type(state() :: #state { q_table :: atom(),
                          p_table :: atom(),
                          next_seq_id :: seq_id(),
                          confirmed :: gb_set(),
                          txn_dict :: dict() }).

-type(msg_status() :: #msg_status { msg :: rabbit_types:basic_message(),
                                    seq_id :: seq_id(),
                                    props :: rabbit_types:message_properties(),
                                    is_delivered :: boolean() }).

-type(tx() :: #tx { to_pub :: [pub()],
                    to_ack :: [seq_id()] }).

-type(pub() :: { rabbit_types:basic_message(),
                 rabbit_types:message_properties() }).

-include("rabbit_backing_queue_spec.hrl").

%% -endif.

%% ----------------------------------------------------------------------------
%% Public API
%%
%% Specs are in rabbit_backing_queue_spec.hrl but are repeated here
%% for clarity.

%% ----------------------------------------------------------------------------
%% start/1 predicts that a list of (durable) queues will be started in
%% the near future. This lets us perform early checking of the
%% consistency of those queues, and initialize other shared
%% resources. These queues might not in fact be started, and other
%% queues might be started instead. It is ignored in this
%% implementation.
%%
%% -spec(start/1 :: ([rabbit_amqqueue:name()]) -> 'ok').

start(_DurableQueues) -> ok.

%% ----------------------------------------------------------------------------
%% stop/0 tears down all state/resources upon shutdown. It might not
%% be called. It is ignored in this implementation.
%%
%% -spec(stop/0 :: () -> 'ok').

stop() -> ok.

%% ----------------------------------------------------------------------------
%% init/5 creates one backing queue, returning its state. Names are
%% local to the vhost, and must be unique.
%%
%% init/5 creates Mnesia transactions to run in, and therefore may not
%% be called from inside another Mnesia transaction.
%%
%% -spec(init/5 ::
%%         (rabbit_amqqueue:name(), is_durable(), attempt_recovery())
%%         -> state()).

%% BUG: We should allow clustering of the Mnesia tables.

%% BUG: It's unfortunate that this can't all be done in a single
%% Mnesia transaction!

init(QueueName, IsDurable, Recover, _AsyncCallback, _SyncCallback) ->
    {QTable, PTable} = tables(QueueName),
    case Recover of
        false -> _ = mnesia:delete_table(QTable),
                 _ = mnesia:delete_table(PTable);
        true -> ok
    end,
    create_table(QTable, 'q_record', 'ordered_set', record_info(fields,
                                                                q_record)),
    create_table(PTable, 'p_record', 'set', record_info(fields, p_record)),
    {atomic, State} =
        mnesia:transaction(
          fun () ->
                  case IsDurable of
                      false -> clear_table(QTable),
                               clear_table(PTable);
                      true -> delete_nonpersistent_msgs(QTable)
                  end,
                  NextSeqId = case mnesia:first(QTable) of
                                  '$end_of_table' -> 0;
                                  SeqId -> SeqId
                              end,
                  #state { q_table = QTable,
                           p_table = PTable,
                           next_seq_id = NextSeqId,
			   confirmed = gb_sets:new(),
                           txn_dict = dict:new() }
          end),
    State.

%% ----------------------------------------------------------------------------
%% terminate/1 deletes all of a queue's pending acks, prior to
%% shutdown. Other calls might be made following terminate/1.
%%
%% terminate/1 creates an Mnesia transaction to run in, and therefore
%% may not be called from inside another Mnesia transaction.
%%
%% -spec(terminate/1 :: (state()) -> state()).

terminate(State = #state { q_table = QTable, p_table = PTable }) ->
    {atomic, _} = mnesia:clear_table(PTable),
    mnesia:dump_tables([QTable, PTable]),
    State.

%% ----------------------------------------------------------------------------
%% delete_and_terminate/1 deletes all of a queue's enqueued msgs and
%% pending acks, prior to shutdown. Other calls might be made
%% following delete_and_terminate/1.
%%
%% delete_and_terminate/1 creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(delete_and_terminate/1 :: (state()) -> state()).

delete_and_terminate(State = #state { q_table = QTable, p_table = PTable }) ->
    {atomic, _} =
        mnesia:transaction(fun () -> clear_table(QTable),
                                     clear_table(PTable)
                           end),
    mnesia:dump_tables([QTable, PTable]),
    State.

%% ----------------------------------------------------------------------------
%% purge/1 deletes all of queue's enqueued msgs, returning the count
%% of msgs purged.
%%
%% purge/1 creates an Mnesia transaction to run in, and therefore may
%% not be called from inside another Mnesia transaction.
%%
%% -spec(purge/1 :: (state()) -> {purged_msg_count(), state()}).

purge(State = #state { q_table = QTable }) ->
    {atomic, Result} =
        mnesia:transaction(fun () -> LQ = length(mnesia:all_keys(QTable)),
                                     clear_table(QTable),
                                     {LQ, State}
                           end),
    Result.

%% ----------------------------------------------------------------------------
%% publish/3 publishes a msg.
%%
%% publish/3 creates an Mnesia transaction to run in, and therefore
%% may not be called from inside another Mnesia transaction.
%%
%% -spec(publish/3 ::
%%         (rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

publish(Msg, Props, State) ->
    {atomic, State1} =
        mnesia:transaction(
          fun () -> internal_publish(Msg, Props, false, State) end),
    confirm([{Msg, Props}], State1).

%% ----------------------------------------------------------------------------
%% publish_delivered/4 is called after a msg has been passed straight
%% out to a client because the queue is empty. We update all state
%% (e.g., next_seq_id) as if we had in fact handled the msg.
%%
%% publish_delivered/4 creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(publish_delivered/4 :: (true, rabbit_types:basic_message(),
%%                               rabbit_types:message_properties(), state())
%%                              -> {ack(), state()};
%%                              (false, rabbit_types:basic_message(),
%%                               rabbit_types:message_properties(), state())
%%                              -> {undefined, state()}).

publish_delivered(false, Msg, Props, State) ->
    {undefined, confirm([{Msg, Props}], State)};
publish_delivered(true,
		  Msg,
		  Props,
		  State = #state { next_seq_id = SeqId }) ->
    MsgStatus = #msg_status { seq_id = SeqId,
                              msg = Msg,
                              props = Props,
                              is_delivered = true },
    {atomic, State1} =
        mnesia:transaction(
          fun () ->
                  add_pending_ack(MsgStatus, State),
                  State #state { next_seq_id = SeqId + 1 }
          end),
    {SeqId, confirm([{Msg, Props}], State1)}.

%% ----------------------------------------------------------------------------
%% drain_confirmed/1 returns the ids of all of the messages that have
%% been confirmed since the last invocation of this function (or since
%% initialisation).
%%
%% -spec(drain_confirmed/1 :: (state()) -> {[rabbit_guid:guid()], state()}).

drain_confirmed(State = #state { confirmed = Confirmed }) ->
    {gb_sets:to_list(Confirmed), State #state { confirmed = gb_sets:new() }}.

%% ----------------------------------------------------------------------------
%% dropwhile/2 drops msgs from the head of the queue while there are
%% msgs and while the supplied predicate returns true.
%%
%% dropwhile/2 creates an Mnesia transaction to run in, and therefore
%% may not be called from inside another Mnesia transaction. The
%% supplied Pred is called from inside the transaction, and therefore
%% may not call another function that creates an Mnesia transaction.
%%
%% -spec(dropwhile/2 ::
%%         (fun ((rabbit_types:message_properties()) -> boolean()), state())
%%         -> state()).

dropwhile(Pred, State) ->
    {atomic, Result} =
        mnesia:transaction(fun () -> internal_dropwhile(Pred, State) end),
    Result.

%% ----------------------------------------------------------------------------
%% fetch/2 produces the next msg, if any.
%%
%% fetch/2 creates an Mnesia transaction to run in, and therefore may
%% not be called from inside another Mnesia transaction.
%%
%% -spec(fetch/2 :: (true,  state()) -> {fetch_result(ack()), state()};
%%                  (false, state()) -> {fetch_result(undefined), state()}).

fetch(AckRequired, State) ->
    {atomic, Result} =
        mnesia:transaction(fun () -> internal_fetch(AckRequired, State) end),
    Result.

%% ----------------------------------------------------------------------------
%% ack/2 acknowledges msgs named by SeqIds.
%%
%% ack/2 creates an Mnesia transaction to run in, and therefore may
%% not be called from inside another Mnesia transaction.
%%
%% -spec(ack/2 :: ([ack()], state()) -> state()).

ack(SeqIds, State) ->
    {atomic, Result} =
        mnesia:transaction(fun () -> internal_ack(SeqIds, State) end),
    Result.

%% ----------------------------------------------------------------------------
%% tx_publish/4 records a pending publish within an AMQP
%% transaction. It stores the msg and its properties in the to_pub
%% field of the txn, waiting to be committed.
%%
%% -spec(tx_publish/4 ::
%%         (rabbit_types:txn(),
%%          rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

tx_publish(Txn, Msg, Props, State = #state { txn_dict = TxnDict}) ->
    Tx = #tx { to_pub = Pubs } = lookup_tx(Txn, TxnDict),
    State #state {
      txn_dict =
          store_tx(Txn, Tx #tx { to_pub = [{Msg, Props} | Pubs] }, TxnDict) }.

%% ----------------------------------------------------------------------------
%% tx_ack/3 records pending acks within an AMQP transaction. It stores
%% the seq_id in the acks field of the txn, waiting to be committed.
%%
%% -spec(tx_ack/3 :: (rabbit_types:txn(), [ack()], state()) -> state()).

tx_ack(Txn, SeqIds, State = #state { txn_dict = TxnDict }) ->
    Tx = #tx { to_ack = SeqIds0 } = lookup_tx(Txn, TxnDict),
    State #state {
      txn_dict =
          store_tx(Txn, Tx #tx { to_ack = SeqIds ++ SeqIds0 }, TxnDict) }.

%% ----------------------------------------------------------------------------
%% tx_rollback/2 aborts a pending AMQP transaction.
%%
%% -spec(tx_rollback/2 :: (rabbit_types:txn(), state()) -> {[ack()], state()}).

tx_rollback(Txn, State = #state { txn_dict = TxnDict }) ->
    #tx { to_ack = SeqIds } = lookup_tx(Txn, TxnDict),
    {SeqIds, State #state { txn_dict = erase_tx(Txn, TxnDict) }}.

%% ----------------------------------------------------------------------------
%% tx_commit/4 commits a pending AMQP transaction. The F passed in is
%% called once the msgs have really been commited (which does not
%% matter here).
%%
%% -spec(tx_commit/4 ::
%%         (rabbit_types:txn(),
%%          fun (() -> any()),
%%          message_properties_transformer(),
%%          state())
%%         -> {[ack()], state()}).

tx_commit(Txn, F, PropsF, State = #state { txn_dict = TxnDict }) ->
    #tx { to_ack = SeqIds, to_pub = Pubs } = lookup_tx(Txn, TxnDict),
    F(),
    State1 = internal_tx_commit(
               Pubs,
               SeqIds,
               PropsF,
               State #state { txn_dict = erase_tx(Txn, TxnDict) }),
    {SeqIds, confirm(Pubs, State1)}.

%% ----------------------------------------------------------------------------
%% requeue/3 reinserts msgs into the queue that have already been
%% delivered and were pending acknowledgement.
%%
%% requeue/3 creates an Mnesia transaction to run in, and therefore
%% may not be called from inside another Mnesia transaction.
%%
%% -spec(requeue/3 ::
%%         ([ack()], message_properties_transformer(), state()) -> state()).

requeue(SeqIds, PropsF, State) ->
    {atomic, Result} =
        mnesia:transaction(
          fun () -> del_pending_acks(
                      fun (#msg_status { msg = Msg, props = Props }, S) ->
                              internal_publish(
                                Msg, PropsF(Props), true, S)
                      end,
                      SeqIds,
                      State)
          end),
    Result.

%% ----------------------------------------------------------------------------
%% len/1 returns the queue length. (The queue length is computed on
%% demand, since it may change due to external actions.)
%%
%% len/1 creates an Mnesia transaction to run in, and therefore may
%% not be called from inside another Mnesia transaction.
%%
%% -spec(len/1 :: (state()) -> non_neg_integer()).

len(#state { q_table = QTable }) ->
    {atomic, Result} =
        mnesia:transaction(fun () -> length(mnesia:all_keys(QTable)) end),
    Result.

%% ----------------------------------------------------------------------------
%% is_empty/1 returns true iff the queue is empty.
%%
%% is_empty/1 creates an Mnesia transaction to run in, and therefore
%% may not be called from inside another Mnesia transaction.
%%
%% -spec(is_empty/1 :: (state()) -> boolean()).

is_empty(#state { q_table = QTable }) ->
    {atomic, Result} =
        mnesia:transaction(fun () -> 0 == length(mnesia:all_keys(QTable)) end),
    Result.

%% ----------------------------------------------------------------------------
%% set_ram_duration_target informs us that the target is to have no
%% more msgs in RAM than indicated by the duration and the current
%% queue rates. It is ignored in this implementation.
%%
%% -spec(set_ram_duration_target/2 ::
%%         (('undefined' | 'infinity' | number()), state())
%%         -> state()).

set_ram_duration_target(_, State) -> State.

%% ----------------------------------------------------------------------------
%% ram_duration/1 optionally recalculates the duration internally
%% (likely to be just update your internal rates), and report how many
%% seconds the msgs in RAM represent given the current rates of the
%% queue. It is a dummy in this implementation.
%%
%% -spec(ram_duration/1 :: (state()) -> {number(), state()}).

ram_duration(State) -> {0, State}.

%% ----------------------------------------------------------------------------
%% needs_idle_timeout/1 returns true iff idle_timeout should be called
%% as soon as the queue process can manage (either on an empty
%% mailbox, or when a timer fires). It always returns false in this
%% implementation.
%%
%% -spec(needs_idle_timeout/1 :: (state()) -> boolean()).

needs_idle_timeout(_) -> false.

%% ----------------------------------------------------------------------------
%% idle_timeout/1 is called (eventually) after needs_idle_timeout
%% returns true. It is a dummy in this implementation.
%%
%% -spec(idle_timeout/1 :: (state()) -> state()).

idle_timeout(State) -> State.

%% ----------------------------------------------------------------------------
%% handle_pre_hibernate/1 is called immediately before the queue
%% hibernates. It is a dummy in this implementation.
%%
%% -spec(handle_pre_hibernate/1 :: (state()) -> state()).

handle_pre_hibernate(State) -> State.

%% ----------------------------------------------------------------------------
%% status/1 exists for debugging and operational purposes, to be able
%% to expose state via rabbitmqctl.
%%
%% status/1 creates an Mnesia transaction to run in, and therefore may
%% not be called from inside another Mnesia transaction.
%%
%% -spec(status/1 :: (state()) -> [{atom(), any()}]).

status(#state { q_table = QTable,
                p_table = PTable,
                next_seq_id = NextSeqId }) ->
    {atomic, Result} =
        mnesia:transaction(
          fun () -> LQ = length(mnesia:all_keys(QTable)),
                    LP = length(mnesia:all_keys(PTable)),
                    [{len, LQ}, {next_seq_id, NextSeqId}, {acks, LP}]
          end),
    Result.

%% ----------------------------------------------------------------------------
%% Monadic helper functions for inside transactions.
%% ----------------------------------------------------------------------------

-spec create_table(atom(), atom(), atom(), [atom()]) -> ok.

create_table(Table, RecordName, Type, Attributes) ->
    case mnesia:create_table(Table, [{record_name, RecordName},
                                     {type, Type},
                                     {attributes, Attributes},
                                     {ram_copies, [node()]}]) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, Table}} ->
            RecordName = mnesia:table_info(Table, record_name),
            Type = mnesia:table_info(Table, type),
            Attributes = mnesia:table_info(Table, attributes),
            ok
    end.

%% Like mnesia:clear_table, but within an Mnesia transaction.

%% BUG: The write-set of the transaction may be huge if the table is
%% huge. Then again, this might not bother Mnesia.

-spec clear_table(atom()) -> ok.

clear_table(Table) ->
    case mnesia:first(Table) of
        '$end_of_table' -> ok;
        Key -> mnesia:delete(Table, Key, 'write'),
               clear_table(Table)
    end.

%% Delete non-persistent msgs after a restart.

-spec delete_nonpersistent_msgs(atom()) -> ok.

delete_nonpersistent_msgs(QTable) ->
    lists:foreach(
      fun (Key) ->
              [#q_record { seq_id = Key, msg_status = MsgStatus }] =
                  mnesia:read(QTable, Key, 'read'),
              case MsgStatus of
                  #msg_status { msg = #basic_message {
                                  is_persistent = true }} -> ok;
                  _ -> mnesia:delete(QTable, Key, 'write')
              end
      end,
      mnesia:all_keys(QTable)).

%% internal_fetch/2 fetches the next msg, if any, inside an Mnesia
%% transaction, generating a pending ack as necessary.

-spec(internal_fetch(true, state()) -> fetch_result(ack());
          (false, state()) -> fetch_result(undefined)).

internal_fetch(AckRequired, State) ->
    case q_pop(State) of
        nothing -> empty;
        {just, MsgStatus} -> post_pop(AckRequired, MsgStatus, State)
    end.

-spec internal_tx_commit([pub()],
                         [seq_id()],
                         message_properties_transformer(),
                         state()) ->
                                state().

internal_tx_commit(Pubs, SeqIds, PropsF, State) ->
    State1 = internal_ack(SeqIds, State),
    lists:foldl(
      fun ({Msg, Props}, S) ->
              internal_publish(Msg, PropsF(Props), false, S)
      end,
      State1,
      lists:reverse(Pubs)).

-spec internal_publish(rabbit_types:basic_message(),
                       rabbit_types:message_properties(),
                       boolean(),
                       state()) ->
                              state().

internal_publish(Msg,
                 Props,
                 IsDelivered,
                 State = #state { q_table = QTable, next_seq_id = SeqId }) ->
    MsgStatus = #msg_status {
      seq_id = SeqId,
      msg = Msg,
      props = Props,
      is_delivered = IsDelivered },
    mnesia:write(
      QTable, #q_record { seq_id = SeqId, msg_status = MsgStatus }, 'write'),
    State #state { next_seq_id = SeqId + 1 }.

-spec(internal_ack/2 :: ([seq_id()], state()) -> state()).

internal_ack(SeqIds, State) ->
    del_pending_acks(fun (_, S) -> S end, SeqIds, State).

-spec(internal_dropwhile/2 ::
        (fun ((rabbit_types:message_properties()) -> boolean()), state())
        -> state()).

internal_dropwhile(Pred, State) ->
    case q_peek(State) of
        nothing -> State;
        {just, MsgStatus = #msg_status { props = Props }} ->
            case Pred(Props) of
                true -> _ = q_pop(State),
                        _ = post_pop(false, MsgStatus, State),
                        internal_dropwhile(Pred, State);
                false -> State
            end
    end.

%% q_pop pops a msg, if any, from the Q table in Mnesia.

-spec q_pop(state()) -> maybe(msg_status()).

q_pop(#state { q_table = QTable }) ->
    case mnesia:first(QTable) of
        '$end_of_table' -> nothing;
        SeqId -> [#q_record { seq_id = SeqId, msg_status = MsgStatus }] =
                     mnesia:read(QTable, SeqId, 'read'),
                 mnesia:delete(QTable, SeqId, 'write'),
                 {just, MsgStatus}
    end.

%% q_peek returns the first msg, if any, from the Q table in
%% Mnesia.

-spec q_peek(state()) -> maybe(msg_status()).

q_peek(#state { q_table = QTable }) ->
    case mnesia:first(QTable) of
        '$end_of_table' -> nothing;
        SeqId -> [#q_record { seq_id = SeqId, msg_status = MsgStatus }] =
                     mnesia:read(QTable, SeqId, 'read'),
                 {just, MsgStatus}
    end.

%% post_pop operates after q_pop, calling add_pending_ack if necessary.

-spec(post_pop(true, msg_status(), state()) -> {fetch_result(ack()), state()};
              (false, msg_status(), state()) ->
                 {fetch_result(undefined), state()}).

post_pop(true,
         MsgStatus = #msg_status {
           seq_id = SeqId, msg = Msg, is_delivered = IsDelivered },
         State = #state { q_table = QTable }) ->
    LQ = length(mnesia:all_keys(QTable)),
    add_pending_ack(MsgStatus #msg_status { is_delivered = true }, State),
    {Msg, IsDelivered, SeqId, LQ};
post_pop(false,
         #msg_status { msg = Msg, is_delivered = IsDelivered },
         #state { q_table = QTable }) ->
    LQ = length(mnesia:all_keys(QTable)),
    {Msg, IsDelivered, undefined, LQ}.

%% add_pending_ack adds a pending ack to the P table in Mnesia.

-spec add_pending_ack(msg_status(), state()) -> ok.

add_pending_ack(MsgStatus = #msg_status { seq_id = SeqId },
      #state { p_table = PTable }) ->
    mnesia:write(PTable,
                 #p_record { seq_id = SeqId, msg_status = MsgStatus },
                 'write'),
    ok.

%% del_pending_acks deletes some set of pending acks from the P table
%% in Mnesia, applying a (Mnesia transactional) function F after each
%% msg is deleted.

-spec del_pending_acks(fun ((msg_status(), state()) -> state()),
		       [seq_id()],
		       state()) ->
			      state().

del_pending_acks(F, SeqIds, State = #state { p_table = PTable }) ->
    lists:foldl(
      fun (SeqId, S) ->
              [#p_record { msg_status = MsgStatus }] =
                  mnesia:read(PTable, SeqId, 'read'),
              mnesia:delete(PTable, SeqId, 'write'),
              F(MsgStatus, S)
      end,
      State,
      SeqIds).

%% ----------------------------------------------------------------------------
%% Pure helper functions.
%% ----------------------------------------------------------------------------

%% Convert a queue name (a record) into its Mnesia table names (atoms).

%% TODO: Import correct argument type.

%% BUG: Mnesia has undocumented restrictions on table names. Names
%% with slashes fail some operations, so we eliminate slashes. We
%% should extend this as necessary, and perhaps make it a little
%% prettier.

-spec tables({resource, binary(), queue, binary()}) -> {atom(), atom()}.

tables({resource, VHost, queue, Name}) ->
    VHost2 = re:split(binary_to_list(VHost), "[/]", [{return, list}]),
    Name2 = re:split(binary_to_list(Name), "[/]", [{return, list}]),
    Str = lists:flatten(io_lib:format("~999999999p", [{VHost2, Name2}])),
    {list_to_atom("q" ++ Str), list_to_atom("p" ++ Str)}.

-spec lookup_tx(rabbit_types:txn(), dict()) -> tx().

lookup_tx(Txn, TxnDict) ->
    case dict:find(Txn, TxnDict) of
        error -> #tx { to_pub = [], to_ack = [] };
        {ok, Tx} -> Tx
    end.

-spec store_tx(rabbit_types:txn(), tx(), dict()) -> dict().

store_tx(Txn, Tx, TxnDict) -> dict:store(Txn, Tx, TxnDict).

-spec erase_tx(rabbit_types:txn(), dict()) -> dict().

erase_tx(Txn, TxnDict) -> dict:erase(Txn, TxnDict).

%% ----------------------------------------------------------------------------
%% Internal plumbing for confirms (aka publisher acks)
%% ----------------------------------------------------------------------------

%% confirm/1 records confirmed messages.

-spec confirm([pub()], state()) -> state().

confirm(Pubs, State = #state { confirmed = Confirmed }) ->
    MsgIds =
        [MsgId || {#basic_message { id = MsgId },
                   #message_properties { needs_confirming = true }} <- Pubs],
    case MsgIds of
        [] -> State;
        _ -> State #state {
               confirmed =
                   gb_sets:union(Confirmed, gb_sets:from_list(MsgIds)) }
    end.

