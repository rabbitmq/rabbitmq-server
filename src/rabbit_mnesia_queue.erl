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
   [start/1, stop/0, init/3, terminate/1, delete_and_terminate/1, purge/1,
    publish/3, publish_delivered/4, fetch/2, ack/2, tx_publish/4, tx_ack/3,
    tx_rollback/2, tx_commit/4, requeue/3, len/1, is_empty/1, dropwhile/2,
    set_ram_duration_target/2, ram_duration/1, needs_idle_timeout/1,
    idle_timeout/1, handle_pre_hibernate/1, status/1]).

%%----------------------------------------------------------------------------
%% This is a simple implementation of the rabbit_backing_queue
%% behavior, with all msgs in Mnesia.
%%
%% This will eventually be structured as a plug-in instead of an extra
%% module in the middle of the server tree....
%% ----------------------------------------------------------------------------

%%----------------------------------------------------------------------------
%% This module wraps msgs into Ms for internal use, including
%% additional information. Pending acks are also recorded as Ms. Msgs
%% and pending acks are both stored in Mnesia.
%%
%% All queues are durable in this version, and all msgs are treated as
%% persistent. (This will break some clients and some tests for
%% non-durable queues.)
%% ----------------------------------------------------------------------------

%% BUG: The rabbit_backing_queue_spec behaviour needs improvement. For
%% example, rabbit_amqqueue_process knows too much about the state of
%% a backing queue, even though this state may now change without its
%% knowledge. Additionally, there are points in the protocol where
%% failures can lose msgs.

%% TODO: Need to provide better back-pressure when queue is filling up.

%% BUG: Should not use mnesia:all_keys to count entries.

%% BUG: p_records do not need a separate seq_id.

%% TODO: Worry about dropping txn_dict upon failure.

-behaviour(rabbit_backing_queue).

%% The S record is the in-RAM AMQP queue state. It contains the names
%% of three Mnesia queues; the next_seq_id and next_out_id (also
%% stored in the N table in Mnesia); and the AMQP transaction dict
%% (which can be dropped on a crash).

-record(s,                  % The in-RAM queue state
        { q_table,          % The Mnesia queue table name
          p_table,          % The Mnesia pending-ack table name
          n_table,          % The Mnesia next_(seq_id, out_id) table name
          next_seq_id,      % The next M's seq_id
          next_out_id,      % The next M's out id
          txn_dict          % In-progress txn->tx map
        }).

%% An M record is a wrapper around a msg. It contains a seq_id,
%% assigned when the msg is published; the msg itself; the msg's
%% props, as presented by the client or as transformed by the client;
%% and an is-delivered flag, for reporting.

-record(m,                  % A wrapper aroung a msg
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
%% by the out-id, which orders msgs; and contains the M itself. We
%% push Ms with a new high out_id, and pop the M with the lowest
%% out_id.  (We cannot use the seq_id for ordering since msgs may be
%% requeued while keeping the same seq_id.)

-record(q_record,           % Q records in Mnesia
        { out_id,           % The key: The out_id
          m                 % The value: The M
          }).

%% A P record is a pending-ack stored in the P table in Mnesia. It is
%% indexed by the seq_id, and contains the M itself. It is randomly
%% accssed by seq_id.

-record(p_record,           % P records in Mnesia
        { seq_id,           % The key: The seq_id
          m                 % The value: The M
          }).

%% An N record holds counters in the single row in the N table in
%% Mnesia. It contains the next_seq_id and next_out_id from the S, so
%% that they can be recovered after a crash. They are updated on every
%% Mnesia transaction that updates them in the in-RAM S.

-record(n_record,           % next_seq_id & next_out_id record in Mnesia
        { key,              % The key: the atom 'n'
          next_seq_id,      % The Mnesia next_seq_id
          next_out_id       % The Mnesia next_out_id
          }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

%% BUG: Restore -ifdef, -endif.

%% -ifdef(use_specs).

-type(maybe(T) :: nothing | {just, T}).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id()).

-type(s() :: #s { q_table :: atom(),
                  p_table :: atom(),
                  n_table :: atom(),
                  next_seq_id :: seq_id(),
                  next_out_id :: non_neg_integer(),
                  txn_dict :: dict() }).
-type(state() :: s()).

-type(m() :: #m { msg :: rabbit_types:basic_message(),
                  seq_id :: seq_id(),
                  props :: rabbit_types:message_properties(),
                  is_delivered :: boolean() }).

-type(tx() :: #tx { to_pub :: [{rabbit_types:basic_message(),
                                rabbit_types:message_properties()}],
                    to_ack :: [seq_id()] }).

-type(q_record() :: #q_record { out_id :: non_neg_integer(),
                                m :: m() }).

-type(p_record() :: #p_record { seq_id :: seq_id(),
                                m :: m() }).

-type(n_record() :: #n_record { key :: 'n',
                                next_seq_id :: seq_id(),
                                next_out_id :: non_neg_integer() }).

-include("rabbit_backing_queue_spec.hrl").

%% -endif.

%%----------------------------------------------------------------------------
%% Public API
%%
%% Specs are in rabbit_backing_queue_spec.hrl but are repeated here.

%%----------------------------------------------------------------------------
%% start/1 promises that a list of (durable) queue names will be
%% started in the near future. This lets us perform early checking of
%% the consistency of those queues, and initialize other shared
%% resources. It is ignored in this implementation.
%%
%% -spec(start/1 :: ([rabbit_amqqueue:name()]) -> 'ok').

start(_DurableQueues) -> ok.

%%----------------------------------------------------------------------------
%% stop/0 tears down all state/resources upon shutdown. It might not
%% be called. It is ignored in this implementation.
%%
%% -spec(stop/0 :: () -> 'ok').

stop() -> ok.

%%----------------------------------------------------------------------------
%% init/3 creates one backing queue, returning its state. Names are
%% local to the vhost, and must be unique.
%%
%% This function creates Mnesia transactions to run in, and therefore
%% may not be called from inside another Mnesia transaction.
%%
%% -spec(init/3 ::
%%         (rabbit_amqqueue:name(), is_durable(), attempt_recovery())
%%         -> state()).

%% BUG: We should allow clustering of the Mnesia tables.

%% BUG: It's unfortunate that this can't all be done in a single
%% Mnesia transaction!

init(QueueName, IsDurable, Recover) ->
    rabbit_log:info("init(~n ~p,~n ~p,~n ~p) ->",
                    [QueueName, IsDurable, Recover]),
    {QTable, PTable, NTable} = tables(QueueName),
    case Recover of
        false -> _ = mnesia:delete_table(QTable),
                 _ = mnesia:delete_table(PTable),
                 _ = mnesia:delete_table(NTable);
        true -> ok
    end,
    create_table(QTable, 'q_record', 'ordered_set', record_info(fields,
                                                                q_record)),
    create_table(PTable, 'p_record', 'set', record_info(fields, p_record)),
    create_table(NTable, 'n_record', 'set', record_info(fields, n_record)),
    {atomic, Result} =
        mnesia:transaction(
          fun () ->
                  {NextSeqId, NextOutId} =
                      case mnesia:read(NTable, 'n', 'read') of
                          [] -> {0, 0};
                          [#n_record { next_seq_id = NextSeqId0,
                                       next_out_id = NextOutId0 }] ->
                              {NextSeqId0, NextOutId0}
                      end,
                  delete_nonpersistent_msgs(QTable),
                  RS = #s { q_table = QTable,
                            p_table = PTable,
                            n_table = NTable,
                            next_seq_id = NextSeqId,
                            next_out_id = NextOutId,
                            txn_dict = dict:new() },
                  save(RS),
                  RS
          end),
    rabbit_log:info("init ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% terminate/1 deletes all of a queue's pending acks, prior to
%% shutdown.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(terminate/1 :: (state()) -> state()).

terminate(S = #s { q_table = QTable, p_table = PTable, n_table = NTable }) ->
    rabbit_log:info("terminate(~n ~p) ->", [S]),
    {atomic, Result} =
        mnesia:transaction(fun () -> clear_table(PTable), S end),
    mnesia:dump_tables([QTable, PTable, NTable]),
    rabbit_log:info("terminate ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% delete_and_terminate/1 deletes all of a queue's enqueued msgs and
%% pending acks, prior to shutdown.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(delete_and_terminate/1 :: (state()) -> state()).

delete_and_terminate(S = #s { q_table = QTable,
                              p_table = PTable,
                              n_table = NTable }) ->
    rabbit_log:info("delete_and_terminate(~n ~p) ->", [S]),
    {atomic, Result} =
        mnesia:transaction(fun () -> clear_table(QTable),
                                     clear_table(PTable),
                                     S
                           end),
    mnesia:dump_tables([QTable, PTable, NTable]),
    rabbit_log:info("delete_and_terminate ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% purge/1 deletes all of queue's enqueued msgs, generating pending
%% acks as required, and returning the count of msgs purged.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(purge/1 :: (state()) -> {purged_msg_count(), state()}).

purge(S = #s { q_table = QTable }) ->
    rabbit_log:info("purge(~n ~p) ->", [S]),
    {atomic, Result} =
        mnesia:transaction(fun () -> LQ = length(mnesia:all_keys(QTable)),
                                     internal_purge(S),
                                     {LQ, S}
                           end),
    rabbit_log:info("purge ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% publish/3 publishes a msg.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(publish/3 ::
%%         (rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

publish(Msg, Props, S) ->
    rabbit_log:info("publish(~n ~p,~n ~p,~n ~p) ->", [Msg, Props, S]),
    {atomic, Result} =
        mnesia:transaction(fun () -> RS = publish_state(Msg, Props, false, S),
                                     save(RS),
                                     RS
                           end),
    rabbit_log:info("publish ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% publish_delivered/4 is called after a msg has been passed straight
%% out to a client because the queue is empty. We update all state
%% (e.g., next_seq_id) as if we had in fact handled the msg.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(publish_delivered/4 :: (true, rabbit_types:basic_message(),
%%                               rabbit_types:message_properties(), state())
%%                              -> {ack(), state()};
%%                              (false, rabbit_types:basic_message(),
%%                               rabbit_types:message_properties(), state())
%%                              -> {undefined, state()}).

publish_delivered(false, _, _, S) ->
    rabbit_log:info("publish_delivered(false, _, _,~n ~p) ->", [S]),
    Result = {undefined, S},
    rabbit_log:info("publish_delivered ->~n ~p", [Result]),
    Result;
publish_delivered(true,
                  Msg,
                  Props,
                  S = #s { next_seq_id = SeqId, next_out_id = OutId }) ->
    rabbit_log:info(
      "publish_delivered(true,~n ~p,~n ~p,~n ~p) ->", [Msg, Props, S]),
    {atomic, Result} =
        mnesia:transaction(
          fun () ->
                  add_p((m(Msg, SeqId, Props)) #m { is_delivered = true }, S),
                  RS = S #s { next_seq_id = SeqId + 1,
                              next_out_id = OutId + 1 },
                  save(RS),
                  {SeqId, RS}
          end),
    rabbit_log:info("publish_delivered ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% dropwhile/2 drops msgs from the head of the queue while there are
%% msgs and while the supplied predicate returns true.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia
%% transaction. The supplied Pred is called from inside the
%% transaction, and therefore may not call another function that
%% creates an Mnesia transaction.
%%
%% -spec(dropwhile/2 ::
%%         (fun ((rabbit_types:message_properties()) -> boolean()), state())
%%         -> state()).

dropwhile(Pred, S) ->
    rabbit_log:info("dropwhile(~n ~p,~n ~p) ->", [Pred, S]),
    {atomic, {_, Result}} =
        mnesia:transaction(fun () -> {Atom, RS} = internal_dropwhile(Pred, S),
                                     save(RS),
                                     {Atom, RS}
                           end),
    rabbit_log:info("dropwhile ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% fetch/2 produces the next msg, if any.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(fetch/2 :: (true,  state()) -> {fetch_result(ack()), state()};
%%                  (false, state()) -> {fetch_result(undefined), state()}).

fetch(AckRequired, S) ->
    rabbit_log:info("fetch(~n ~p,~n ~p) ->", [AckRequired, S]),
    {atomic, FR} =
        mnesia:transaction(fun () -> internal_fetch(AckRequired, S) end),
    Result = {FR, S},
    rabbit_log:info("fetch ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% ack/2 acknowledges msgs named by SeqIds, mapping SeqIds to guids
%% upon return.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(ack/2 :: ([ack()], state()) -> state()).

ack(SeqIds, S) ->
    rabbit_log:info("ack(~n ~p,~n ~p) ->", [SeqIds, S]),
    {atomic, Result} =
        mnesia:transaction(fun () -> {_, RS} = internal_ack(SeqIds, S),
                                     save(RS),
                                     RS
                           end),
    rabbit_log:info("ack ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_publish/4 is a publish within an AMQP transaction. It stores the
%% msg and its properties in the to_pub field of the txn, waiting to
%% be committed.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(tx_publish/4 ::
%%         (rabbit_types:txn(),
%%          rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

tx_publish(Txn, Msg, Props, S) ->
    rabbit_log:info(
      "tx_publish(~n ~p,~n ~p,~n ~p,~n ~p) ->", [Txn, Msg, Props, S]),
    {atomic, Result} =
        mnesia:transaction(
          fun () -> Tx = #tx { to_pub = Pubs } = lookup_tx(Txn, S),
                    RS = store_tx(Txn,
                                  Tx #tx { to_pub = [{Msg, Props} | Pubs] },
                                  S),
                    save(RS),
                    RS
          end),
    rabbit_log:info("tx_publish ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_ack/3 acks within an AMQP transaction. It stores the seq_id in
%% the acks field of the txn, waiting to be committed.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(tx_ack/3 :: (rabbit_types:txn(), [ack()], state()) -> state()).

tx_ack(Txn, SeqIds, S) ->
    rabbit_log:info("tx_ack(~n ~p,~n ~p,~n ~p) ->", [Txn, SeqIds, S]),
    {atomic, Result} =
        mnesia:transaction(
          fun () -> Tx = #tx { to_ack = SeqIds0 } = lookup_tx(Txn, S),
                    RS = store_tx(Txn,
                                  Tx #tx {
                                    to_ack = lists:append(SeqIds, SeqIds0) },
                                S),
                    save(RS),
                    RS
          end),
    rabbit_log:info("tx_ack ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_rollback/2 aborts an AMQP transaction.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(tx_rollback/2 :: (rabbit_types:txn(), state()) -> {[ack()], state()}).

tx_rollback(Txn, S) ->
    rabbit_log:info("tx_rollback(~n ~p,~n ~p) ->", [Txn, S]),
    {atomic, Result} =
        mnesia:transaction(fun () ->
                                   #tx { to_ack = SeqIds } = lookup_tx(Txn, S),
                                   RS = erase_tx(Txn, S),
                                   save(RS),
                                   {SeqIds, RS}
                           end),
    rabbit_log:info("tx_rollback ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% tx_commit/4 commits an AMQP transaction. The F passed in is called
%% once the msgs have really been commited. This CPS permits the
%% possibility of commit coalescing.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia
%% transaction. However, the supplied F is called outside the
%% transaction.
%%
%% -spec(tx_commit/4 ::
%%         (rabbit_types:txn(),
%%          fun (() -> any()),
%%          message_properties_transformer(),
%%          state())
%%         -> {[ack()], state()}).

tx_commit(Txn, F, PropsF, S) ->
    rabbit_log:info(
      "tx_commit(~n ~p,~n ~p,~n ~p,~n ~p) ->", [Txn, F, PropsF, S]),
    {atomic, Result} =
        mnesia:transaction(
          fun () ->
                  #tx { to_ack = SeqIds, to_pub = Pubs } = lookup_tx(Txn, S),
                  RS =
                      tx_commit_state(Pubs, SeqIds, PropsF, erase_tx(Txn, S)),
                  save(RS),
                  {SeqIds, RS}
          end),
    F(),
    rabbit_log:info("tx_commit ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% requeue/3 reinserts msgs into the queue that have already been
%% delivered and were pending acknowledgement.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(requeue/3 ::
%%         ([ack()], message_properties_transformer(), state()) -> state()).

requeue(SeqIds, PropsF, S) ->
    rabbit_log:info("requeue(~n ~p,~n ~p,~n ~p) ->", [SeqIds, PropsF, S]),
    {atomic, Result} =
        mnesia:transaction(
          fun () -> {_, RS} =
                        del_ps(
                          fun (#m { msg = Msg, props = Props }, Si) ->
                                  publish_state(Msg, PropsF(Props), true, Si)
                          end,
                          SeqIds,
                          S),
                    save(RS),
                    RS
          end),
    rabbit_log:info("requeue ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% len/1 returns the queue length.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(len/1 :: (state()) -> non_neg_integer()).

len(S = #s { q_table = QTable }) ->
    rabbit_log:info("len(~n ~p) ->", [S]),
    {atomic, Result} =
        mnesia:transaction(fun () -> length(mnesia:all_keys(QTable)) end),
    rabbit_log:info("len ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% is_empty/1 returns true iff the queue is empty.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(is_empty/1 :: (state()) -> boolean()).

is_empty(S = #s { q_table = QTable }) ->
    rabbit_log:info("is_empty(~n ~p) ->", [S]),
    {atomic, Result} =
        mnesia:transaction(fun () -> 0 == length(mnesia:all_keys(QTable)) end),
    rabbit_log:info("is_empty ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
%% set_ram_duration_target informs us that the target is to have no
%% more msgs in RAM than indicated by the duration and the current
%% queue rates. It is ignored in this implementation.
%%
%% -spec(set_ram_duration_target/2 ::
%%         (('undefined' | 'infinity' | number()), state())
%%         -> state()).

set_ram_duration_target(_, S) -> S.

%%----------------------------------------------------------------------------
%% ram_duration/1 optionally recalculates the duration internally
%% (likely to be just update your internal rates), and report how many
%% seconds the msgs in RAM represent given the current rates of the
%% queue. It is a dummy in this implementation.
%%
%% -spec(ram_duration/1 :: (state()) -> {number(), state()}).

ram_duration(S) -> {0, S}.

%%----------------------------------------------------------------------------
%% needs_idle_timeout/1 returns true iff idle_timeout should be called
%% as soon as the queue process can manage (either on an empty
%% mailbox, or when a timer fires). It always returns false in this
%% implementation.
%%
%% -spec(needs_idle_timeout/1 :: (state()) -> boolean()).

needs_idle_timeout(_) -> false.

%%----------------------------------------------------------------------------
%% idle_timeout/1 is called (eventually) after needs_idle_timeout
%% returns true. It is a dummy in this implementation.
%%
%% -spec(idle_timeout/1 :: (state()) -> state()).

idle_timeout(S) -> S.

%%----------------------------------------------------------------------------
%% handle_pre_hibernate/1 is called immediately before the queue
%% hibernates. It is a dummy in this implementation.
%%
%% -spec(handle_pre_hibernate/1 :: (state()) -> state()).

handle_pre_hibernate(S) -> S.

%%----------------------------------------------------------------------------
%% status/1 exists for debugging and operational purposes, to be able
%% to expose state via rabbitmqctl.
%%
%% This function creates an Mnesia transaction to run in, and
%% therefore may not be called from inside another Mnesia transaction.
%%
%% -spec(status/1 :: (state()) -> [{atom(), any()}]).

status(S = #s { q_table = QTable,
		p_table = PTable,
                next_seq_id = NextSeqId }) ->
    rabbit_log:info("status(~n ~p) ->", [S]),
    {atomic, Result} =
        mnesia:transaction(
          fun () -> LQ = length(mnesia:all_keys(QTable)),
                    LP = length(mnesia:all_keys(PTable)),
                    [{len, LQ}, {next_seq_id, NextSeqId}, {acks, LP}]
          end),
    rabbit_log:info("status ->~n ~p", [Result]),
    Result.

%%----------------------------------------------------------------------------
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
              [#q_record { out_id = Key, m = M }] =
                  mnesia:read(QTable, Key, 'read'),
              case M of
                  #m { msg = #basic_message { is_persistent = true }} -> ok;
                  _ -> mnesia:delete(QTable, Key, 'write')
              end
      end,
      mnesia:all_keys(QTable)).

%% internal_purge/1 purges all messages, generating pending acks as
%% necessary.

-spec internal_purge(state()) -> ok.

internal_purge(S) -> case internal_fetch(true, S) of
                         empty -> ok;
                         _ -> internal_purge(S)
                     end.

%% internal_fetch/2 fetches the next msg, if any, inside an Mnesia
%% transaction, generating a pending ack as necessary.

-spec(internal_fetch(true, s()) -> {fetch_result(ack()), s()};
                    (false, s()) -> {fetch_result(undefined), s()}).

internal_fetch(AckRequired, S) -> 
    case q_pop(S) of
        nothing -> empty;
        {just, M} -> post_pop(AckRequired, M, S)
    end.

-spec tx_commit_state([rabbit_types:basic_message()],
                      [seq_id()],
                      message_properties_transformer(),
                      s()) ->
                             s().

tx_commit_state(Pubs, SeqIds, PropsF, S) ->
    {_, S1} = internal_ack(SeqIds, S),
    lists:foldl(
      fun ({Msg, Props}, Si) -> publish_state(Msg, Props, false, Si) end,
      S1,
      [{Msg, PropsF(Props)} || {Msg, Props} <- lists:reverse(Pubs)]).

-spec publish_state(rabbit_types:basic_message(),
                    rabbit_types:message_properties(),
                    boolean(),
                    s()) ->
                           s().

publish_state(Msg,
              Props,
              IsDelivered,
              S = #s { q_table = QTable,
                       next_seq_id = SeqId,
                       next_out_id = OutId }) ->
    M = (m(Msg, SeqId, Props)) #m { is_delivered = IsDelivered },
    mnesia:write(QTable, #q_record { out_id = OutId, m = M }, 'write'),
    S #s { next_seq_id = SeqId + 1, next_out_id = OutId + 1 }.

-spec(internal_ack/2 :: ([seq_id()], s()) -> {[rabbit_guid:guid()], s()}).

internal_ack(SeqIds, S) -> del_ps(fun (_, Si) -> Si end, SeqIds, S).

-spec(internal_dropwhile/2 ::
        (fun ((rabbit_types:message_properties()) -> boolean()), s())
        -> {empty | ok, s()}).

internal_dropwhile(Pred, S) ->
    case q_peek(S) of
        nothing -> {empty, S};
        {just, M = #m { props = Props }} ->
            case Pred(Props) of
                true -> _ = q_pop(S),
                        _ = post_pop(false, M, S),
                        internal_dropwhile(Pred, S);
                false -> {ok, S}
            end
    end.

%% q_pop pops a msg, if any, from the Q table in Mnesia.

-spec q_pop(s()) -> maybe(m()).

q_pop(#s { q_table = QTable }) ->
    case mnesia:first(QTable) of
        '$end_of_table' -> nothing;
        OutId -> [#q_record { out_id = OutId, m = M }] =
                     mnesia:read(QTable, OutId, 'read'),
                 mnesia:delete(QTable, OutId, 'write'),
                 {just, M}
    end.

%% q_peek returns the first msg, if any, from the Q table in
%% Mnesia.

-spec q_peek(s()) -> maybe(m()).

q_peek(#s { q_table = QTable }) ->
    case mnesia:first(QTable) of
        '$end_of_table' -> nothing;
        OutId -> [#q_record { out_id = OutId, m = M }] =
                     mnesia:read(QTable, OutId, 'read'),
                 {just, M}
    end.

%% post_pop operates after q_pop, calling add_p if necessary.

-spec(post_pop(true, m(), s()) -> {fetch_result(ack()), s()};
              (false, m(), s()) -> {fetch_result(undefined), s()}).

post_pop(true,
	 M = #m { seq_id = SeqId, msg = Msg, is_delivered = IsDelivered },
	 S = #s { q_table = QTable }) ->
    LQ = length(mnesia:all_keys(QTable)),
    add_p(M #m { is_delivered = true }, S),
    {Msg, IsDelivered, SeqId, LQ};
post_pop(false,
	 #m { msg = Msg, is_delivered = IsDelivered },
	 #s { q_table = QTable }) ->
    LQ = length(mnesia:all_keys(QTable)),
    {Msg, IsDelivered, undefined, LQ}.

%% add_p adds a pending ack to the P table in Mnesia.

-spec add_p(m(), s()) -> ok.

add_p(M = #m { seq_id = SeqId }, #s { p_table = PTable }) ->
    mnesia:write(PTable, #p_record { seq_id = SeqId, m = M }, 'write'),
    ok.

%% del_fs deletes some number of pending acks from the P table in
%% Mnesia, applying a (Mnesia transactional) function F after each msg
%% is deleted, and returning their guids.

-spec del_ps(fun (([rabbit_guid:guid()], s()) -> s()),
	     [rabbit_guid:guid()],
	     s()) ->
		    {[rabbit_guid:guid()], s()}.

del_ps(F, SeqIds, S = #s { p_table = PTable }) ->
    {AllGuids, Sn} =
        lists:foldl(
          fun (SeqId, {Acc, Si}) ->
                  [#p_record {
                      m = M = #m { msg = #basic_message { guid = Guid }} }] =
                      mnesia:read(PTable, SeqId, 'read'),
                  mnesia:delete(PTable, SeqId, 'write'),
                  {[Guid | Acc], F(M, Si)}
          end,
          {[], S},
          SeqIds),
    {lists:reverse(AllGuids), Sn}.

%% save copies the volatile part of the state (next_seq_id and
%% next_out_id) to Mnesia.

-spec save(s()) -> ok.

save(#s { n_table = NTable,
	  next_seq_id = NextSeqId,
	  next_out_id = NextOutId }) ->
    ok = mnesia:write(NTable,
                      #n_record { key = 'n',
                                  next_seq_id = NextSeqId,
                                  next_out_id = NextOutId },
                      'write').

%%----------------------------------------------------------------------------
%% Pure helper functions.
%% ----------------------------------------------------------------------------

%% Convert a queue name (a record) into an Mnesia table name (an atom).

%% TODO: Import correct argument type.

%% BUG: Mnesia has undocumented restrictions on table names. Names
%% with slashes fail some operations, so we replace replace slashes
%% with the string SLASH. We should extend this as necessary, and
%% perhaps make it a little prettier.

-spec tables({resource, binary(), queue, binary()}) ->
		    {atom(), atom(), atom()}.

tables({resource, VHost, queue, Name}) ->
    VHost2 = re:split(binary_to_list(VHost), "[/]", [{return, list}]),
    Name2 = re:split(binary_to_list(Name), "[/]", [{return, list}]),
    Str = lists:flatten(io_lib:format("~p ~p", [VHost2, Name2])),
    {list_to_atom(lists:append("q: ", Str)),
     list_to_atom(lists:append("p: ", Str)),
     list_to_atom(lists:append("n: ", Str))}.

-spec m(rabbit_types:basic_message(),
        seq_id(),
        rabbit_types:message_properties()) ->
               m().

m(Msg, SeqId, Props) ->
    #m { seq_id = SeqId, msg = Msg, props = Props, is_delivered = false }.

-spec lookup_tx(rabbit_types:txn(), s()) -> tx().

lookup_tx(Txn, #s { txn_dict = TxnDict }) ->
    case dict:find(Txn, TxnDict) of
        error -> #tx { to_pub = [], to_ack = [] };
        {ok, Tx} -> Tx
    end.

-spec store_tx(rabbit_types:txn(), tx(), s()) -> s().

store_tx(Txn, Tx, S = #s { txn_dict = TxnDict }) ->
    S #s { txn_dict = dict:store(Txn, Tx, TxnDict) }.

-spec erase_tx(rabbit_types:txn(), s()) -> s().

erase_tx(Txn, S = #s { txn_dict = TxnDict }) ->
    S #s { txn_dict = dict:erase(Txn, TxnDict) }.
