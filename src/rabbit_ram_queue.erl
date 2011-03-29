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
   [start/1, stop/0, init/5, terminate/1, delete_and_terminate/1, purge/1,
    publish/3, publish_delivered/4, drain_confirmed/1, fetch/2, ack/2,
    tx_publish/4, tx_ack/3, tx_rollback/2, tx_commit/4, requeue/3, len/1,
    is_empty/1, dropwhile/2, set_ram_duration_target/2, ram_duration/1,
    needs_idle_timeout/1, idle_timeout/1, handle_pre_hibernate/1, status/1]).

-behaviour(rabbit_backing_queue).

-record(state, { q, q_len, pending_acks, next_seq_id, confirmed, txn_dict }).

-record(msg_status, { seq_id, msg, props, is_delivered }).

-record(tx, { to_pub, to_ack }).

-include("rabbit.hrl").

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id()).

-type(state() :: #state { q :: queue(), q_len :: non_neg_integer(),
                          pending_acks :: dict(), next_seq_id :: seq_id(),
                          confirmed :: gb_set(), txn_dict :: dict() }).

-type(msg_status() :: #msg_status { seq_id :: seq_id(),
                                    msg :: rabbit_types:basic_message(),
                                    props :: rabbit_types:message_properties(),
                                    is_delivered :: boolean() }).

-type(tx() :: #tx { to_pub :: [pub()], to_ack :: [seq_id()] }).

-type(pub() :: { rabbit_types:basic_message(),
                 rabbit_types:message_properties() }).

-include("rabbit_backing_queue_spec.hrl").

-spec(internal_fetch(true, state()) -> {fetch_result(ack()), state()};
                    (false, state()) -> {fetch_result(undefined), state()}).

-spec internal_tx_commit([pub()], [seq_id()], message_properties_transformer(),
                         state()) ->
                                state().

-spec internal_publish(rabbit_types:basic_message(),
                       rabbit_types:message_properties(), boolean(), state()) ->
                              state().

-spec(internal_ack/2 :: ([seq_id()], state()) -> state()).

-spec(internal_dropwhile/2 ::
        (fun ((rabbit_types:message_properties()) -> boolean()), state())
        -> state()).

-spec(post_pop(true, msg_status(), state()) -> {fetch_result(ack()), state()};
              (false, msg_status(), state()) ->
                 {fetch_result(undefined), state()}).

-spec del_pending_acks(fun ((msg_status(), state()) -> state()), [seq_id()],
                        state()) ->
                    state().

-spec lookup_tx(rabbit_types:txn(), dict()) -> tx().

-spec confirm([pub()], state()) -> state().

start(_DurableQueues) -> ok.

stop() -> ok.

init(_QueueName, _IsDurable, _Recover, _asyncCallback, _SyncCallback) ->
    #state { q = queue:new(), q_len = 0, pending_acks = dict:new(),
             next_seq_id = 0, confirmed = gb_sets:new(),
             txn_dict = dict:new() }.

terminate(State) -> State #state { pending_acks = dict:new() }.

delete_and_terminate(State) ->
    State #state { q = queue:new(), q_len = 0, pending_acks = dict:new() }.

purge(State = #state { q_len = QLen }) ->
    {QLen, State #state { q = queue:new(), q_len = 0 }}.

publish(Msg = #basic_message { is_persistent = false }, Props, State) ->
    State1 = internal_publish(Msg, Props, false, State),
    confirm([{Msg, Props}], State1).

publish_delivered(false, Msg = #basic_message { is_persistent = false }, Props,
                  State = #state { q_len = 0 }) ->
    {undefined, confirm([{Msg, Props}], State)};
publish_delivered(true, Msg = #basic_message { is_persistent = false }, Props,
                  State = #state { q_len = 0, next_seq_id = SeqId,
                                   pending_acks = PendingAcks }) ->
    MsgStatus = #msg_status { seq_id = SeqId, msg = Msg, props = Props,
                              is_delivered = true },
    State1 = State #state {
               next_seq_id = SeqId + 1,
               pending_acks = dict:store(SeqId, MsgStatus, PendingAcks) },
    {SeqId, confirm([{Msg, Props}], State1)}.

drain_confirmed(State = #state { confirmed = Confirmed }) ->
    {gb_sets:to_list(Confirmed), State #state { confirmed = gb_sets:new() }}.

dropwhile(Pred, State) -> internal_dropwhile(Pred, State).

fetch(AckRequired, State) -> internal_fetch(AckRequired, State).

ack(SeqIds, State) -> internal_ack(SeqIds, State).

tx_publish(Txn, Msg = #basic_message { is_persistent = false }, Props,
           State = #state { txn_dict = TxnDict}) ->
    Tx = #tx { to_pub = Pubs } = lookup_tx(Txn, TxnDict),
    State #state {
      txn_dict =
          dict:store(Txn, Tx #tx { to_pub = [{Msg, Props} | Pubs] }, TxnDict) }.

tx_ack(Txn, SeqIds, State = #state { txn_dict = TxnDict }) ->
    Tx = #tx { to_ack = SeqIds0 } = lookup_tx(Txn, TxnDict),
    State #state {
      txn_dict =
          dict:store(Txn, Tx #tx { to_ack = SeqIds ++ SeqIds0 }, TxnDict) }.

tx_rollback(Txn, State = #state { txn_dict = TxnDict }) ->
    #tx { to_ack = SeqIds } = lookup_tx(Txn, TxnDict),
    {SeqIds, State #state { txn_dict = dict:erase(Txn, TxnDict) }}.

tx_commit(Txn, F, PropsF, State = #state { txn_dict = TxnDict }) ->
    #tx { to_ack = SeqIds, to_pub = Pubs } = lookup_tx(Txn, TxnDict),
    F(),
    State1 = internal_tx_commit(
               Pubs, SeqIds, PropsF,
               State #state { txn_dict = dict:erase(Txn, TxnDict) }),
    {SeqIds, confirm(Pubs, State1)}.

requeue(SeqIds, PropsF, State) ->
    del_pending_acks(
      fun (#msg_status { msg = Msg, props = Props }, S) ->
              internal_publish(Msg, PropsF(Props), true, S)
      end,
      SeqIds, State).

len(#state { q_len = QLen }) -> QLen.

is_empty(#state { q_len = QLen }) -> QLen == 0.

set_ram_duration_target(_, State) -> State.

ram_duration(State) -> {0, State}.

needs_idle_timeout(_) -> false.

idle_timeout(State) -> State.

handle_pre_hibernate(State) -> State.

status(#state { q_len = QLen, pending_acks = PendingAcks,
                next_seq_id = NextSeqId }) ->
    [{len, QLen}, {next_seq_id, NextSeqId}, {acks, dict:size(PendingAcks)}].

internal_fetch(AckRequired, State = #state { q = Q, q_len = QLen }) ->
    case queue:out(Q) of
        {empty, _} -> {empty, State};
        {{value, MsgStatus}, Q1} ->
            post_pop(AckRequired, MsgStatus,
                     State #state { q = Q1, q_len = QLen - 1 })
    end.

internal_tx_commit(Pubs, SeqIds, PropsF, State) ->
    State1 = internal_ack(SeqIds, State),
    lists:foldl(
      fun ({Msg, Props}, S) ->
              internal_publish(Msg, PropsF(Props), false, S)
      end,
      State1, lists:reverse(Pubs)).

internal_publish(Msg, Props, IsDelivered,
                 State =
                     #state { q = Q, q_len = QLen, next_seq_id = SeqId }) ->
    MsgStatus = #msg_status {
      seq_id = SeqId, msg = Msg, props = Props, is_delivered = IsDelivered },
    State #state { q = queue:in(MsgStatus, Q), q_len = QLen + 1,
                   next_seq_id = SeqId + 1 }.

internal_ack(SeqIds, State) ->
    del_pending_acks(fun (_, S) -> S end, SeqIds, State).

internal_dropwhile(Pred, State = #state { q = Q, q_len = QLen }) ->
    case queue:out(Q) of
        {empty, _} -> State;
        {{value, MsgStatus = #msg_status { props = Props }}, Q1} ->
            case Pred(Props) of
                true -> State1 = State #state { q = Q1, q_len = QLen - 1 },
                        {_, State2} = post_pop(false, MsgStatus, State1),
                        internal_dropwhile(Pred, State2);
                false -> State
            end
    end.

post_pop(true,
         MsgStatus = #msg_status {
           seq_id = SeqId, msg = Msg, is_delivered = IsDelivered },
         State = #state { q_len = QLen, pending_acks = PendingAcks }) ->
    MsgStatus1 = MsgStatus #msg_status { is_delivered = true },
    {{Msg, IsDelivered, SeqId, QLen},
     State #state {
       pending_acks = dict:store(SeqId, MsgStatus1, PendingAcks) }};
post_pop(false, #msg_status { msg = Msg, is_delivered = IsDelivered },
         State = #state { q_len = QLen }) ->
    {{Msg, IsDelivered, undefined, QLen}, State}.

del_pending_acks(F, SeqIds, State) ->
    lists:foldl(
      fun (SeqId, S = #state { pending_acks = PendingAcks }) ->
              MsgStatus = dict:fetch(SeqId, PendingAcks),
              F(MsgStatus,
                S #state { pending_acks = dict:erase(SeqId, PendingAcks) })
      end,
      State, SeqIds).

lookup_tx(Txn, TxnDict) -> case dict:find(Txn, TxnDict) of
                               error -> #tx { to_pub = [], to_ack = [] };
                               {ok, Tx} -> Tx
                           end.

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

