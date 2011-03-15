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

-module(rabbit_ram_queue).

-export(
   [start/1, stop/0, init/3, terminate/1, delete_and_terminate/1, purge/1,
    publish/3, publish_delivered/4, fetch/2, ack/2, tx_publish/4, tx_ack/3,
    tx_rollback/2, tx_commit/4, requeue/3, len/1, is_empty/1, dropwhile/2,
    set_ram_duration_target/2, ram_duration/1, needs_idle_timeout/1,
    idle_timeout/1, handle_pre_hibernate/1, status/1]).

%% ----------------------------------------------------------------------------
%% This is a simple implementation of the rabbit_backing_queue
%% behavior, with all msgs in RAM.
%%
%% This will eventually be structured as a plug-in instead of an extra
%% module in the middle of the server tree....
%% ----------------------------------------------------------------------------

%% ----------------------------------------------------------------------------
%% This module wraps msgs into msg_status records for internal use,
%% including additional information. Pending acks are also recorded as
%% msg_status records. These are both stored in RAM.
%%
%% All queues are non-durable in this version, and all msgs are
%% treated as non-persistent. (This may break some clients and some
%% tests for durable queues, but it also keeps some tests from
%% breaking the test apparatus.)
%% ----------------------------------------------------------------------------

%% TODO: Need to provide better back-pressure when queue is filling up.

-behaviour(rabbit_backing_queue).

%% The state record is the in-RAM AMQP queue state. It contains the
%% queue of msg_status records; the next_seq_id; and the AMQP
%% transaction dict.

-record(state,              % The in-RAM queue state
        { q,                % The queue of msg_status records
          q_len,            % queue:len of q
          pending_acks,     % The seq_id->msg_status map of pending acks
          next_seq_id,      % The next msg_status record's seq_id
          txn_dict          % In-progress txn->tx map
        }).

%% An msg_status record is a wrapper around a msg. It contains a
%% seq_id, assigned when the msg is published; the msg itself; the
%% msg's props, as presented by the client or as transformed by the
%% client; and an is-delivered flag, for reporting.

-record(msg_status,        % A wrapper aroung a msg
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

-include("rabbit.hrl").

%% ----------------------------------------------------------------------------

%% BUG: Restore -ifdef, -endif.

%% -ifdef(use_specs).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id()).

-type(state() :: #state { q :: queue(),
                          q_len :: non_neg_integer(),
                          pending_acks :: dict(),
                          next_seq_id :: seq_id(),
                          txn_dict :: dict() }).

-type(msg_status() ::
        #msg_status { seq_id :: seq_id(),
                      msg :: rabbit_types:basic_message(),
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
%% for readability.

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
%% stop/0 tears down all queue state/resources upon shutdown. It might
%% not be called. It is ignored in this implementation.
%%
%% -spec(stop/0 :: () -> 'ok').

stop() -> ok.

%% ----------------------------------------------------------------------------
%% init/3 creates one backing queue, returning its state. Names are
%% local to the vhost, and must be unique.
%%
%% -spec(init/3 ::
%%         (rabbit_amqqueue:name(), is_durable(), attempt_recovery())
%%         -> state()).

init(_QueueName, _IsDurable, _Recover) ->
    #state { q = queue:new(),
	     q_len = 0,
	     pending_acks = dict:new(),
	     next_seq_id = 0,
	     txn_dict = dict:new() }.

%% ----------------------------------------------------------------------------
%% terminate/1 deletes all of a queue's pending acks, prior to
%% shutdown. Other calls might be made following terminate/1.
%%
%% -spec(terminate/1 :: (state()) -> state()).

terminate(State) -> State #state { pending_acks = dict:new() }.

%% ----------------------------------------------------------------------------
%% delete_and_terminate/1 deletes all of a queue's enqueued msgs and
%% pending acks, prior to shutdown. Other calls might be made
%% following delete_and_terminate/1.
%%
%% -spec(delete_and_terminate/1 :: (state()) -> state()).

delete_and_terminate(State) ->
    State #state { q = queue:new(), q_len = 0, pending_acks = dict:new() }.

%% ----------------------------------------------------------------------------
%% purge/1 deletes all of queue's enqueued msgs, returning the count
%% of msgs purged.
%%
%% -spec(purge/1 :: (state()) -> {purged_msg_count(), state()}).

purge(State = #state { q_len = QLen }) ->
    {QLen, State #state { q = queue:new(), q_len = 0 }}.

%% ----------------------------------------------------------------------------
%% publish/3 publishes a msg.
%%
%% -spec(publish/3 ::
%%         (rabbit_types:basic_message(),
%%          rabbit_types:message_properties(),
%%          state())
%%         -> state()).

publish(Msg, Props, State) ->
    callback([{Msg, Props}]),
    internal_publish(Msg, Props, false, State).

%% ----------------------------------------------------------------------------
%% publish_delivered/4 is called after a msg has been passed straight
%% out to a client because the queue is empty. We update all state
%% (e.g., next_seq_id) as if we had in fact handled the msg.
%%
%% -spec(publish_delivered/4 :: (true, rabbit_types:basic_message(),
%%                               rabbit_types:message_properties(), state())
%%                              -> {ack(), state()};
%%                              (false, rabbit_types:basic_message(),
%%                               rabbit_types:message_properties(), state())
%%                              -> {undefined, state()}).

publish_delivered(false, Msg, Props, State) ->
    callback([{Msg, Props}]),
    {undefined, State};
publish_delivered(true,
		  Msg,
		  Props,
		  State = #state { next_seq_id = SeqId,
				   pending_acks = PendingAcks }) ->
    callback([{Msg, Props}]),
    MsgStatus = #msg_status { seq_id = SeqId,
			      msg = Msg,
			      props = Props,
			      is_delivered = true },
    {SeqId, State #state {
	      next_seq_id = SeqId + 1,
	      pending_acks = dict:store(SeqId, MsgStatus, PendingAcks) }}.

%% ----------------------------------------------------------------------------
%% dropwhile/2 drops msgs from the head of the queue while there are
%% msgs and while the supplied predicate returns true.
%%
%% dropwhile/2 cannot call callback/1 because callback/1 ultimately
%% calls rabbit_amqqueue_process:maybe_run_queue_via_backing_queue/2,
%% which calls dropwhile/2.
%%
%% The only use of dropwhile/1 is to drop expired messages from the
%% head of the queue.
%%
%% -spec(dropwhile/2 ::
%%         (fun ((rabbit_types:message_properties()) -> boolean()), state())
%%         -> state()).

dropwhile(Pred, State) ->
    {_, Result} = internal_dropwhile(Pred, State),
    Result.

%% ----------------------------------------------------------------------------
%% fetch/2 produces the next msg, if any.
%%
%% -spec(fetch/2 :: (true,  state()) -> {fetch_result(ack()), state()};
%%                  (false, state()) -> {fetch_result(undefined), state()}).

fetch(AckRequired, State) ->
    % TODO: This dropwhile is to help the testPublishAndGetWithExpiry
    % functional test pass. Although msg expiration is asynchronous by
    % design, that test depends on very quick expiration. That test is
    % therefore nondeterministic (sometimes passing, sometimes
    % failing) and should be rewritten, at which point this dropwhile
    % could be, well, dropped.
    Now = timer:now_diff(now(), {0,0,0}),
    State1 = dropwhile(
               fun (#message_properties{expiry = Expiry}) -> Expiry < Now end,
               State),
    internal_fetch(AckRequired, State1).

%% ----------------------------------------------------------------------------
%% ack/2 acknowledges msgs named by SeqIds.
%%
%% -spec(ack/2 :: ([ack()], state()) -> state()).

ack(SeqIds, State) -> internal_ack(SeqIds, State).

%% ----------------------------------------------------------------------------
%% tx_publish/4 is a publish within an AMQP transaction. It stores the
%% msg and its properties in the to_pub field of the txn, waiting to
%% be committed.
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
%% tx_ack/3 acks within an AMQP transaction. It stores the seq_id in
%% the acks field of the txn, waiting to be committed.
%%
%% -spec(tx_ack/3 :: (rabbit_types:txn(), [ack()], state()) -> state()).

tx_ack(Txn, SeqIds, State = #state { txn_dict = TxnDict }) ->
    Tx = #tx { to_ack = SeqIds0 } = lookup_tx(Txn, TxnDict),
    State #state {
      txn_dict =
	  store_tx(Txn, Tx #tx { to_ack = SeqIds ++ SeqIds0 }, TxnDict) }.

%% ----------------------------------------------------------------------------
%% tx_rollback/2 aborts an AMQP transaction.
%%
%% -spec(tx_rollback/2 :: (rabbit_types:txn(), state()) -> {[ack()], state()}).

tx_rollback(Txn, State = #state { txn_dict = TxnDict }) ->
    #tx { to_ack = SeqIds } = lookup_tx(Txn, TxnDict),
    {SeqIds, State #state { txn_dict = erase_tx(Txn, TxnDict) }}.

%% ----------------------------------------------------------------------------
%% tx_commit/4 commits an AMQP transaction. The F passed in is called
%% once the msgs have really been commited (which does not matter
%% here). This CPS permits the possibility of commit coalescing.
%%
%% -spec(tx_commit/4 ::
%%         (rabbit_types:txn(),
%%          fun (() -> any()),
%%          message_properties_transformer(),
%%          state())
%%         -> {[ack()], state()}).

tx_commit(Txn, F, PropsF, State = #state { txn_dict = TxnDict }) ->
    #tx { to_ack = SeqIds, to_pub = Pubs } = lookup_tx(Txn, TxnDict),
    callback(Pubs),
    F(),
    {SeqIds, internal_tx_commit(
	       Pubs,
	       SeqIds,
	       PropsF,
	       State #state { txn_dict = erase_tx(Txn, TxnDict) })}.

%% ----------------------------------------------------------------------------
%% requeue/3 reinserts msgs into the queue that have already been
%% delivered and were pending acknowledgement.
%%
%% -spec(requeue/3 ::
%%         ([ack()], message_properties_transformer(), state()) -> state()).

requeue(SeqIds, PropsF, State) ->
    del_ps(
      fun (#msg_status { msg = Msg, props = Props }, Si) ->
	      internal_publish(Msg, PropsF(Props), true, Si)
      end,
      SeqIds,
      State).

%% ----------------------------------------------------------------------------
%% len/1 returns the queue length. (The queue length is maintained in
%% the q_len field instead of being computed on demand, since the
%% rabbit_amqqueue_process module calls len/1 so frequently.)
%%
%% -spec(len/1 :: (state()) -> non_neg_integer()).

len(#state { q_len = QLen }) -> QLen.

%% ----------------------------------------------------------------------------
%% is_empty/1 returns true iff the queue is empty.
%%
%% -spec(is_empty/1 :: (state()) -> boolean()).

is_empty(#state { q_len = QLen }) -> QLen == 0.

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
%% -spec(status/1 :: (state()) -> [{atom(), any()}]).

status(#state { q_len = QLen,
		pending_acks = PendingAcks,
		next_seq_id = NextSeqId }) ->
    [{len, QLen}, {next_seq_id, NextSeqId}, {acks, dict:size(PendingAcks)}].

%% ----------------------------------------------------------------------------
%% Helper functions.
%% ----------------------------------------------------------------------------

%% internal_fetch/2 fetches the next msg, if any, generating a pending
%% ack as necessary.

-spec(internal_fetch(true, state()) -> {fetch_result(ack()), state()};
                    (false, state()) -> {fetch_result(undefined), state()}).

internal_fetch(AckRequired, State = #state { q = Q, q_len = QLen }) ->
    case queue:out(Q) of
        {empty, _} -> {empty, State};
        {{value, MsgStatus}, Q1} ->
	    post_pop(AckRequired,
		     MsgStatus,
		     State #state { q = Q1, q_len = QLen - 1 })
    end.

-spec internal_tx_commit([pub()],
                         [seq_id()],
                         message_properties_transformer(),
                         state()) ->
                                state().

internal_tx_commit(Pubs, SeqIds, PropsF, State) ->
    State1 = internal_ack(SeqIds, State),
    lists:foldl(
      fun ({Msg, Props}, Si) ->
	      internal_publish(Msg, PropsF(Props), false, Si)
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
                 State =
                     #state { q = Q, q_len = QLen, next_seq_id = SeqId }) ->
    State #state { q = queue:in(#msg_status { seq_id = SeqId,
					      msg = Msg,
					      props = Props,
					      is_delivered = IsDelivered },
				Q),
                   q_len = QLen + 1,
                   next_seq_id = SeqId + 1 }.

-spec(internal_ack/2 :: ([seq_id()], state()) -> state()).

internal_ack(SeqIds, State) -> del_ps(fun (_, Si) -> Si end, SeqIds, State).

-spec(internal_dropwhile/2 ::
        (fun ((rabbit_types:message_properties()) -> boolean()), state())
        -> {empty | ok, state()}).

internal_dropwhile(Pred, State = #state { q = Q, q_len = QLen }) ->
    case queue:out(Q) of
        {empty, _} -> {empty, State};
        {{value, MsgStatus = #msg_status { props = Props }}, Q1} ->
            case Pred(Props) of
                true -> State1 = State #state { q = Q1, q_len = QLen - 1 },
                        {_, State2} = post_pop(false, MsgStatus, State1),
                        internal_dropwhile(Pred, State2);
                false -> {ok, State}
            end
    end.

%% post_pop operates after popping a msg_status from the queue,
%% adding a pending ack if necessary.

-spec(post_pop(true, msg_status(), state()) -> {fetch_result(ack()), state()};
              (false, msg_status(), state()) ->
                 {fetch_result(undefined), state()}).

post_pop(true,
         MsgStatus = #msg_status { seq_id = SeqId,
                                   msg = Msg,
                                   is_delivered = IsDelivered },
         State = #state { q_len = QLen, pending_acks = PendingAcks }) ->
    MsgStatus1 = MsgStatus #msg_status { is_delivered = true },
    {{Msg, IsDelivered, SeqId, QLen},
     State #state {
       pending_acks = dict:store(SeqId, MsgStatus1, PendingAcks) }};
post_pop(false,
         #msg_status { msg = Msg, is_delivered = IsDelivered },
         State = #state { q_len = QLen }) ->
    {{Msg, IsDelivered, undefined, QLen}, State}.

%% del_ps deletes some number of pending acks from the pending_acks
%% dict, applying a function F after each msg is deleted.

-spec del_ps(fun ((msg_status(), state()) -> state()), [seq_id()], state()) ->
                    state().

del_ps(F, SeqIds, State) ->
    lists:foldl(
      fun (SeqId, StateI = #state { pending_acks = PendingAcks }) ->
              MsgStatus = dict:fetch(SeqId, PendingAcks),
              F(MsgStatus, StateI #state {
			     pending_acks = dict:erase(SeqId, PendingAcks) })
      end,
      State,
      SeqIds).

%% ----------------------------------------------------------------------------
%% Pure helper functions.
%% ----------------------------------------------------------------------------

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

%% callback/1 calls back into the broker to confirm msgs, and expire
%% msgs, and quite possibly to perform yet other side-effects. It's
%% black magic.

-spec callback([pub()]) -> ok.

callback(Pubs) ->
    Guids =
	[Guid || {#basic_message { guid = Guid },
		  #message_properties { needs_confirming = true }} <- Pubs],
    case Guids of
	[] -> ok;
	_ -> 
	    rabbit_amqqueue:maybe_run_queue_via_backing_queue_async(
	      self(), fun (State) -> {Guids, State} end),
	    ok
    end.
