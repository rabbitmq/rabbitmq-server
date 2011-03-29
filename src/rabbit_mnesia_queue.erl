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
%% ----------------------------------------------------------------------------

-behaviour(rabbit_backing_queue).

-record(state,
        { q_table, pending_ack_table, next_seq_id, confirmed, txn_dict }).

-record(msg_status, { seq_id, msg, props, is_delivered }).

-record(tx, { to_pub, to_ack }).

-include("rabbit.hrl").

-type(maybe(T) :: nothing | {just, T}).

-type(seq_id() :: non_neg_integer()).
-type(ack() :: seq_id()).

-type(state() :: #state { q_table :: atom(), pending_ack_table :: atom(),
                          next_seq_id :: seq_id(), confirmed :: gb_set(),
                          txn_dict :: dict() }).

-type(msg_status() :: #msg_status { msg :: rabbit_types:basic_message(),
                                    seq_id :: seq_id(),
                                    props :: rabbit_types:message_properties(),
                                    is_delivered :: boolean() }).

-type(tx() :: #tx { to_pub :: [pub()], to_ack :: [seq_id()] }).

-type(pub() :: { rabbit_types:basic_message(),
                 rabbit_types:message_properties() }).

-include("rabbit_backing_queue_spec.hrl").

-spec create_table(atom(), atom(), atom(), [atom()]) -> ok.

-spec clear_table(atom()) -> ok.

-spec delete_nonpersistent_msgs(atom()) -> ok.

-spec(internal_fetch(true, state()) -> fetch_result(ack());
                    (false, state()) -> fetch_result(undefined)).

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

-spec q_pop(state()) -> maybe(msg_status()).

-spec q_peek(state()) -> maybe(msg_status()).

-spec(post_pop(true, msg_status(), state()) -> fetch_result(ack());
              (false, msg_status(), state()) -> fetch_result(undefined)).

-spec add_pending_ack(msg_status(), state()) -> ok.

-spec del_pending_acks(fun ((msg_status(), state()) -> state()), [seq_id()],
                       state()) ->
                              state().

-spec tables({resource, binary(), queue, binary()}) -> {atom(), atom()}.

-spec lookup_tx(rabbit_types:txn(), dict()) -> tx().

-spec store_tx(rabbit_types:txn(), tx(), dict()) -> dict().

-spec erase_tx(rabbit_types:txn(), dict()) -> dict().

-spec table_length(atom()) -> non_neg_integer().

-spec confirm([pub()], state()) -> state().

start(_DurableQueues) -> ok.

stop() -> ok.

init(QueueName, IsDurable, Recover, _AsyncCallback, _SyncCallback) ->
    {QTable, PendingAckTable} = tables(QueueName),
    case Recover of
        false -> case mnesia:delete_table(QTable) of
                     {atomic, ok} -> ok;
                     {aborted, {no_exists, QTable}} -> ok
                 end,
                 case mnesia:delete_table(PendingAckTable) of
                     {atomic, ok} -> ok;
                     {aborted, {no_exists, PendingAckTable}} -> ok
                 end;
        true -> ok
    end,
    ok = create_table(QTable, msg_status, ordered_set,
                      record_info(fields, msg_status)),
    ok = create_table(PendingAckTable, msg_status, set,
                      record_info(fields, msg_status)),
    {atomic, State} =
        mnesia:sync_transaction(
          fun () ->
                  case IsDurable of
                      false -> ok = clear_table(QTable),
                               ok = clear_table(PendingAckTable);
                      true -> ok = delete_nonpersistent_msgs(QTable)
                  end,
                  NextSeqId = case mnesia:last(QTable) of
                                  '$end_of_table' -> 0;
                                  SeqId -> SeqId + 1
                              end,
                  #state { q_table = QTable,
                           pending_ack_table = PendingAckTable,
                           next_seq_id = NextSeqId, confirmed = gb_sets:new(),
                           txn_dict = dict:new() }
          end),
    State.

terminate(State = #state { pending_ack_table = PendingAckTable }) ->
    {atomic, ok} = mnesia:clear_table(PendingAckTable),
    State.

delete_and_terminate(State = #state { q_table = QTable,
                                      pending_ack_table = PendingAckTable }) ->
    {atomic, _} = mnesia:sync_transaction(
		    fun () -> ok = clear_table(QTable),
			      ok = clear_table(PendingAckTable)
		    end),
    State.

purge(State = #state { q_table = QTable }) ->
    {atomic, Result} = mnesia:sync_transaction(
			 fun () -> LQ = table_length(QTable),
				   ok = clear_table(QTable),
				   {LQ, State}
			 end),
    Result.

publish(Msg, Props, State) ->
    {atomic, State1} =
        mnesia:sync_transaction(
          fun () -> internal_publish(Msg, Props, false, State) end),
    confirm([{Msg, Props}], State1).

publish_delivered(false, Msg, Props, State) ->
    {undefined, confirm([{Msg, Props}], State)};
publish_delivered(true, Msg, Props, State = #state { next_seq_id = SeqId }) ->
    MsgStatus = #msg_status {
      seq_id = SeqId, msg = Msg, props = Props, is_delivered = true },
    {atomic, State1} = mnesia:sync_transaction(
			 fun () ->
				 ok = add_pending_ack(MsgStatus, State),
				 State #state { next_seq_id = SeqId + 1 }
			 end),
    {SeqId, confirm([{Msg, Props}], State1)}.

drain_confirmed(State = #state { confirmed = Confirmed }) ->
    {gb_sets:to_list(Confirmed), State #state { confirmed = gb_sets:new() }}.

dropwhile(Pred, State) ->
    {atomic, Result} =
        mnesia:sync_transaction(fun () -> internal_dropwhile(Pred, State) end),
    Result.

fetch(AckRequired, State) ->
    {atomic, FetchResult} = mnesia:sync_transaction(
                              fun () -> internal_fetch(AckRequired, State) end),
    {FetchResult, State}.

ack(SeqIds, State) ->
    {atomic, Result} = mnesia:sync_transaction(
			 fun () -> internal_ack(SeqIds, State) end),
    Result.

tx_publish(Txn, Msg, Props, State = #state { txn_dict = TxnDict}) ->
    Tx = #tx { to_pub = Pubs } = lookup_tx(Txn, TxnDict),
    State #state {
      txn_dict =
          store_tx(Txn, Tx #tx { to_pub = [{Msg, Props} | Pubs] }, TxnDict) }.

tx_ack(Txn, SeqIds, State = #state { txn_dict = TxnDict }) ->
    Tx = #tx { to_ack = SeqIds0 } = lookup_tx(Txn, TxnDict),
    State #state {
      txn_dict =
          store_tx(Txn, Tx #tx { to_ack = SeqIds ++ SeqIds0 }, TxnDict) }.

tx_rollback(Txn, State = #state { txn_dict = TxnDict }) ->
    #tx { to_ack = SeqIds } = lookup_tx(Txn, TxnDict),
    {SeqIds, State #state { txn_dict = erase_tx(Txn, TxnDict) }}.

tx_commit(Txn, F, PropsF, State = #state { txn_dict = TxnDict }) ->
    #tx { to_ack = SeqIds, to_pub = Pubs } = lookup_tx(Txn, TxnDict),
    {atomic, State1} =
        mnesia:sync_transaction(
          fun () ->
                  internal_tx_commit(
                    Pubs, SeqIds, PropsF,
                    State #state { txn_dict = erase_tx(Txn, TxnDict) })
          end),
    F(),
    {SeqIds, confirm(Pubs, State1)}.

requeue(SeqIds, PropsF, State) ->
    {atomic, Result} =
        mnesia:sync_transaction(
          fun () -> del_pending_acks(
                      fun (#msg_status { msg = Msg, props = Props }, S) ->
                              internal_publish(Msg, PropsF(Props), true, S)
                      end,
                      SeqIds, State)
          end),
    Result.

len(#state { q_table = QTable }) ->
    {atomic, Result} = mnesia:sync_transaction(
                         fun () -> table_length(QTable) end),
    Result.

is_empty(#state { q_table = QTable }) ->
    {atomic, Result} =
        mnesia:sync_transaction(fun () -> 0 == table_length(QTable) end),
    Result.

set_ram_duration_target(_, State) -> State.

ram_duration(State) -> {0, State}.

needs_idle_timeout(_) -> false.

idle_timeout(State) -> State.

handle_pre_hibernate(State) -> State.

status(#state { q_table = QTable, pending_ack_table = PendingAckTable,
                next_seq_id = NextSeqId }) ->
    {atomic, Result} =
        mnesia:sync_transaction(
          fun () -> [{len, table_length(QTable)}, {next_seq_id, NextSeqId},
                     {acks, table_length(PendingAckTable)}]
          end),
    Result.

create_table(Table, RecordName, Type, Attributes) ->
    case mnesia:create_table(
           Table,
           [{record_name, RecordName}, {type, Type}, {attributes, Attributes},
            {disc_copies, rabbit_mnesia:running_clustered_nodes()}]) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, Table}} ->
            RecordName = mnesia:table_info(Table, record_name),
            Type = mnesia:table_info(Table, type),
            Attributes = mnesia:table_info(Table, attributes),
            ok
    end.

clear_table(Table) ->
    mnesia:foldl(fun (#msg_status { seq_id = SeqId }, ok) ->
                         ok = mnesia:delete(Table, SeqId, write)
                 end,
                 ok, Table).

delete_nonpersistent_msgs(QTable) ->
    mnesia:foldl(fun (MsgStatus = #msg_status { seq_id = SeqId }, ok) ->
                         case MsgStatus of
                             #msg_status { msg = #basic_message {
                                             is_persistent = true }} -> ok;
                             _ -> ok = mnesia:delete(QTable, SeqId, write)
                         end
                 end,
                 ok, 
                 QTable).

internal_fetch(AckRequired, State) ->
    case q_pop(State) of
        nothing -> empty;
        {just, MsgStatus} -> post_pop(AckRequired, MsgStatus, State)
    end.

internal_tx_commit(Pubs, SeqIds, PropsF, State) ->
    State1 = internal_ack(SeqIds, State),
    lists:foldl(fun ({Msg, Props}, S) ->
                        internal_publish(Msg, PropsF(Props), false, S)
                end,
                State1, lists:reverse(Pubs)).

internal_publish(Msg, Props, IsDelivered,
                 State = #state { q_table = QTable, next_seq_id = SeqId }) ->
    MsgStatus = #msg_status {
      seq_id = SeqId, msg = Msg, props = Props, is_delivered = IsDelivered },
    ok = mnesia:write(QTable, MsgStatus, write),
    State #state { next_seq_id = SeqId + 1 }.

internal_ack(SeqIds, State) ->
    del_pending_acks(fun (_, S) -> S end, SeqIds, State).

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

q_pop(#state { q_table = QTable }) ->
    case mnesia:first(QTable) of
        '$end_of_table' -> nothing;
        SeqId -> [MsgStatus] = mnesia:read(QTable, SeqId, read),
                 ok = mnesia:delete(QTable, SeqId, write),
                 {just, MsgStatus}
    end.

q_peek(#state { q_table = QTable }) ->
    case mnesia:first(QTable) of
        '$end_of_table' -> nothing;
        SeqId -> [MsgStatus] = mnesia:read(QTable, SeqId, read),
                 {just, MsgStatus}
    end.

post_pop(true,
         MsgStatus = #msg_status {
           seq_id = SeqId, msg = Msg, is_delivered = IsDelivered },
         State = #state { q_table = QTable }) ->
    LQ = table_length(QTable),
    ok = add_pending_ack(MsgStatus #msg_status { is_delivered = true }, State),
    {Msg, IsDelivered, SeqId, LQ};
post_pop(false, #msg_status { msg = Msg, is_delivered = IsDelivered },
         #state { q_table = QTable }) ->
    {Msg, IsDelivered, undefined, table_length(QTable)}.

add_pending_ack(MsgStatus, #state { pending_ack_table = PendingAckTable }) ->
    ok = mnesia:write(PendingAckTable, MsgStatus, write).

del_pending_acks(
  F, SeqIds, State = #state { pending_ack_table = PendingAckTable }) ->
    lists:foldl(
      fun (SeqId, S) ->
              [MsgStatus] = mnesia:read(PendingAckTable, SeqId, read),
              ok = mnesia:delete(PendingAckTable, SeqId, write),
              F(MsgStatus, S)
      end,
      State, SeqIds).

tables({resource, VHost, queue, Name}) ->
    VHost2 = re:split(binary_to_list(VHost), "[/]", [{return, list}]),
    Name2 = re:split(binary_to_list(Name), "[/]", [{return, list}]),
    Str = lists:flatten(io_lib:format("~999999999p", [{VHost2, Name2}])),
    {list_to_atom("q" ++ Str), list_to_atom("p" ++ Str)}.

lookup_tx(Txn, TxnDict) ->
    case dict:find(Txn, TxnDict) of
        error -> #tx { to_pub = [], to_ack = [] };
        {ok, Tx} -> Tx
    end.

store_tx(Txn, Tx, TxnDict) -> dict:store(Txn, Tx, TxnDict).

erase_tx(Txn, TxnDict) -> dict:erase(Txn, TxnDict).

table_length(Table) -> mnesia:foldl(fun (_, N) -> N + 1 end, 0, Table).

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

