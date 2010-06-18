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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_invariable_queue).

-export([init/3, terminate/1, delete_and_terminate/1, purge/1, publish/2,
         publish_delivered/3, fetch/2, ack/2, tx_publish/3, tx_ack/3,
         tx_rollback/2, tx_commit/3, requeue/2, len/1, is_empty/1,
         set_ram_duration_target/2, ram_duration/1, needs_sync/1, sync/1,
         handle_pre_hibernate/1, status/1]).

-export([start/1]).

-behaviour(rabbit_backing_queue).

-include("rabbit.hrl").

-record(iv_state, { queue, qname, durable, len, pending_ack }).
-record(tx, { pending_messages, pending_acks, is_persistent }).

-ifdef(use_specs).

-type(ack() :: guid() | 'blank_ack').
-type(state() :: #iv_state { queue       :: queue(),
                             qname       :: queue_name(),
                             len         :: non_neg_integer(),
                             pending_ack :: dict()
                           }).
-include("rabbit_backing_queue_spec.hrl").

-endif.

start(DurableQueues) ->
    ok = rabbit_sup:start_child(rabbit_persister, [DurableQueues]).

init(QName, IsDurable, Recover) ->
    Q = queue:from_list(case IsDurable andalso Recover of
                            true  -> rabbit_persister:queue_content(QName);
                            false -> []
                        end),
    #iv_state { queue       = Q,
                qname       = QName,
                durable     = IsDurable,
                len         = queue:len(Q),
                pending_ack = dict:new() }.

terminate(State) ->
    State #iv_state { queue = queue:new(), len = 0, pending_ack = dict:new() }.

delete_and_terminate(State = #iv_state { qname = QName, durable = IsDurable,
                                         pending_ack = PA }) ->
    ok = persist_acks(QName, IsDurable, none, dict:fetch_keys(PA), PA),
    {_PLen, State1} = purge(State),
    terminate(State1).

purge(State = #iv_state { queue = Q, qname = QName, durable = IsDurable,
                          len = Len }) ->
    %% We do not purge messages pending acks.
    {AckTags, PA} =
        rabbit_misc:queue_fold(
          fun ({#basic_message { is_persistent = false }, _IsDelivered}, Acc) ->
                  Acc;
              ({Msg = #basic_message { guid = Guid }, IsDelivered},
               {AckTagsN, PAN}) ->
                  ok = persist_delivery(QName, IsDurable, IsDelivered, Msg),
                  {[Guid | AckTagsN], dict:store(Guid, Msg, PAN)}
          end, {[], dict:new()}, Q),
    ok = persist_acks(QName, IsDurable, none, AckTags, PA),
    {Len, State #iv_state { len = 0, queue = queue:new() }}.

publish(Msg, State = #iv_state { queue = Q, qname = QName, durable = IsDurable,
                                 len = Len }) ->
    ok = persist_message(QName, IsDurable, none, Msg),
    State #iv_state { queue = queue:in({Msg, false}, Q), len = Len + 1 }.

publish_delivered(false, _Msg, State) ->
    {blank_ack, State};
publish_delivered(true, Msg = #basic_message { guid = Guid },
                  State = #iv_state { qname = QName, durable = IsDurable,
                                      len = 0, pending_ack = PA }) ->
    ok = persist_message(QName, IsDurable, none, Msg),
    ok = persist_delivery(QName, IsDurable, false, Msg),
    {Guid, State #iv_state { pending_ack = dict:store(Guid, Msg, PA) }}.

fetch(_AckRequired, State = #iv_state { len = 0 }) ->
    {empty, State};
fetch(AckRequired, State = #iv_state { len = Len, queue = Q, qname = QName,
                                       durable = IsDurable,
                                       pending_ack = PA }) ->
    {{value, {Msg = #basic_message { guid = Guid }, IsDelivered}}, Q1} =
        queue:out(Q),
    Len1 = Len - 1,
    ok = persist_delivery(QName, IsDurable, IsDelivered, Msg),
    PA1 = dict:store(Guid, Msg, PA),
    {AckTag, PA2} = case AckRequired of
                        true  -> {Guid, PA1};
                        false -> ok = persist_acks(QName, IsDurable, none,
                                                   [Guid], PA1),
                                 {blank_ack, PA}
                    end,
    {{Msg, IsDelivered, AckTag, Len1},
     State #iv_state { queue = Q1, len = Len1, pending_ack = PA2 }}.

ack(AckTags, State = #iv_state { qname = QName, durable = IsDurable,
                                 pending_ack = PA }) ->
    ok = persist_acks(QName, IsDurable, none, AckTags, PA),
    PA1 = remove_acks(AckTags, PA),
    State #iv_state { pending_ack = PA1 }.

tx_publish(Txn, Msg, State = #iv_state { qname = QName,
                                         durable = IsDurable }) ->
    Tx = #tx { pending_messages = Pubs } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_messages = [Msg | Pubs] }),
    ok = persist_message(QName, IsDurable, Txn, Msg),
    State.

tx_ack(Txn, AckTags, State = #iv_state { qname = QName, durable = IsDurable,
                                         pending_ack = PA }) ->
    Tx = #tx { pending_acks = Acks } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_acks = [AckTags | Acks] }),
    ok = persist_acks(QName, IsDurable, Txn, AckTags, PA),
    State.

tx_rollback(Txn, State = #iv_state { qname = QName }) ->
    #tx { pending_acks = AckTags } = lookup_tx(Txn),
    ok = do_if_persistent(fun rabbit_persister:rollback_transaction/1,
                          Txn, QName),
    erase_tx(Txn),
    {lists:flatten(AckTags), State}.

tx_commit(Txn, Fun, State = #iv_state { qname = QName, pending_ack = PA,
                                        queue = Q, len = Len }) ->
    #tx { pending_acks = AckTags, pending_messages = PubsRev } = lookup_tx(Txn),
    ok = do_if_persistent(fun rabbit_persister:commit_transaction/1,
                          Txn, QName),
    erase_tx(Txn),
    Fun(),
    AckTags1 = lists:flatten(AckTags),
    PA1 = remove_acks(AckTags1, PA),
    {Q1, Len1} = lists:foldr(fun (Msg, {QN, LenN}) ->
                                     {queue:in({Msg, false}, QN), LenN + 1}
                             end, {Q, Len}, PubsRev),
    {AckTags1, State #iv_state { pending_ack = PA1, queue = Q1, len = Len1 }}.

requeue(AckTags, State = #iv_state { pending_ack = PA, queue = Q,
                                     len = Len }) ->
    %% We don't need to touch the persister here - the persister will
    %% already have these messages published and delivered as
    %% necessary. The complication is that the persister's seq_id will
    %% now be wrong, given the position of these messages in our queue
    %% here. However, the persister's seq_id is only used for sorting
    %% on startup, and requeue is silent as to where the requeued
    %% messages should appear, thus the persister is permitted to sort
    %% based on seq_id, even though it'll likely give a different
    %% order to the last known state of our queue, prior to shutdown.
    {Q1, Len1} = lists:foldl(
                   fun (Guid, {QN, LenN}) ->
                           {ok, Msg = #basic_message {}} = dict:find(Guid, PA),
                           {queue:in({Msg, true}, QN), LenN + 1}
                   end, {Q, Len}, AckTags),
    PA1 = remove_acks(AckTags, PA),
    State #iv_state { pending_ack = PA1, queue = Q1, len = Len1 }.

len(#iv_state { len = Len }) -> Len.

is_empty(State) -> 0 == len(State).

set_ram_duration_target(_DurationTarget, State) -> State.

ram_duration(State) -> {0, State}.

needs_sync(_State) -> false.

sync(State) -> State.

handle_pre_hibernate(State) -> State.

status(_State) -> [].

%%----------------------------------------------------------------------------

remove_acks(AckTags, PA) -> lists:foldl(fun dict:erase/2, PA, AckTags).

%%----------------------------------------------------------------------------

lookup_tx(Txn) ->
    case get({txn, Txn}) of
        undefined -> #tx { pending_messages = [],
                           pending_acks     = [],
                           is_persistent    = false };
        V         -> V
    end.

store_tx(Txn, Tx) ->
    put({txn, Txn}, Tx).

erase_tx(Txn) ->
    erase({txn, Txn}).

mark_tx_persistent(Txn) ->
    store_tx(Txn, (lookup_tx(Txn)) #tx { is_persistent = true }).

is_tx_persistent(Txn) ->
    (lookup_tx(Txn)) #tx.is_persistent.

do_if_persistent(F, Txn, QName) ->
    ok = case is_tx_persistent(Txn) of
             false -> ok;
             true  -> F({Txn, QName})
         end.

%%----------------------------------------------------------------------------

persist_message(QName, true, Txn, Msg = #basic_message {
                                    is_persistent = true }) ->
    Msg1 = Msg #basic_message {
             %% don't persist any recoverable decoded properties,
             %% rebuild from properties_bin on restore
             content = rabbit_binary_parser:clear_decoded_content(
                         Msg #basic_message.content)},
    persist_work(Txn, QName,
                 [{publish, Msg1, {QName, Msg1 #basic_message.guid}}]);
persist_message(_QName, _IsDurable, _Txn, _Msg) ->
    ok.

persist_delivery(QName, true, false, #basic_message { is_persistent = true,
                                                      guid = Guid }) ->
    persist_work(none, QName, [{deliver, {QName, Guid}}]);
persist_delivery(_QName, _IsDurable, _IsDelivered, _Msg) ->
    ok.

persist_acks(QName, true, Txn, AckTags, PA) ->
    persist_work(Txn, QName,
                 [{ack, {QName, Guid}} || Guid <- AckTags,
                                          begin
                                              {ok, Msg} = dict:find(Guid, PA),
                                              Msg #basic_message.is_persistent
                                          end]);
persist_acks(_QName, _IsDurable, _Txn, _AckTags, _PA) ->
    ok.

persist_work(_Txn,_QName, []) ->
    ok;
persist_work(none, _QName, WorkList) ->
    rabbit_persister:dirty_work(WorkList);
persist_work(Txn, QName, WorkList) ->
    mark_tx_persistent(Txn),
    rabbit_persister:extend_transaction({Txn, QName}, WorkList).
