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

-module(rabbit_transaction).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([start/0, init/1, mainloop/1, set_writer_pid/2]).
-export([publish/4, lock_message/6, release_message/3]).
-export([auto_acknowledge/2]).
-export([shutdown/1]).
-export([select/1, commit/1, rollback/1]).
-export([recover/2]).

-import(queue).
-import(mnesia).
-import(dict).
-import(erlang).

-record(thandle, {pid, writer_pid_cache, transactional}).

-record(tstate, {transaction_id,
                 writer_pid,
                 uncommitted_publish_q,
                 uncommitted_ack_q,
                 unacked_message_q}).

start() ->
    #thandle{pid = spawn_link(?MODULE, init, [#tstate{transaction_id = none,
                                                      writer_pid =
                                                        transaction_writer_pid_uninitialized,
                                                      uncommitted_publish_q = queue:new(),
                                                      uncommitted_ack_q = queue:new(),
                                                      unacked_message_q = queue:new()}]),
             writer_pid_cache = transaction_writer_pid_uninitialized,
             transactional = false}.

%---------------------------------------------------------------------------

set_writer_pid(Tx = #thandle{pid = T}, WriterPid) ->
    T ! {set_writer_pid, WriterPid},
    Tx#thandle{writer_pid_cache = WriterPid}.

publish(#thandle{transactional = false, writer_pid_cache = WriterPid},
        Mandatory, Immediate, Message0) ->
    {Message, Handlers} = publish_tophalf(none, Message0),
    ok = publish_bottomhalf(none, Handlers, Mandatory, Immediate,
                            Message, WriterPid);
publish(#thandle{transactional = true, pid = T}, Mandatory, Immediate, Message) ->
    T ! {publish, Mandatory, Immediate, Message},
    ok.

lock_message(#thandle{pid = T}, DeliveryTag, ConsumerTag, QName, QPid, Message) ->
    T ! {lock_message, DeliveryTag, ConsumerTag, QName, QPid, Message},
    ok.

release_message(#thandle{pid = T}, DeliveryTag, Multiple) ->
    T ! {release_message, DeliveryTag, Multiple},
    ok.

auto_acknowledge(QName, PKey) ->
    ok = rabbit_persister:dirty_work([{ack, QName, PKey}]).

shutdown(#thandle{pid = T}) ->
    T ! shutdown,
    ok.

select(Tx = #thandle{pid = T}) ->
    T ! select,
    Tx#thandle{transactional = true}.

commit(#thandle{transactional = false}) ->
    rabbit_misc:die(not_allowed, 'tx.commit');
commit(#thandle{pid = T, transactional = true, writer_pid_cache = WriterPid}) ->
    {ok, Cookie} = rabbit_writer:pause(WriterPid),
    T ! {commit, self()},
    receive
        commit_ok ->
            ok = rabbit_writer:unpause(WriterPid, Cookie),
            ok
    end.

rollback(#thandle{transactional = false}) ->
    rabbit_misc:die(not_allowed, 'tx.rollback');
rollback(#thandle{pid = T, transactional = true, writer_pid_cache = WriterPid}) ->
    {ok, Cookie} = rabbit_writer:pause(WriterPid),
    ?LOGDEBUG0("Commissioned rollback start~n"),
    T ! {rollback, self()},
    receive
        rollback_ok ->
            ?LOGDEBUG0("Commissioned rollback end~n"),
            ok = rabbit_writer:unpause(WriterPid, Cookie),
            ok
    end.

recover(#thandle{transactional = true}, _ShouldRequeue) ->
    rabbit_misc:die(not_allowed, 'basic.recover');
recover(#thandle{transactional = false, pid = T}, ShouldRequeue) ->
    T ! {recover, self(), ShouldRequeue},
    receive
        recover_ok ->
            ok
    end.

%---------------------------------------------------------------------------

init(State) ->
    process_flag(trap_exit, true),
    mainloop(State).

mainloop(State) ->
    receive
        Message ->
            State1 = handle_message(Message, State),
            ?MODULE:mainloop(State1)
    end.

handle_message({publish, Mandatory, Immediate, Message0}, State) ->
    case catch internal_publish(Mandatory, Immediate, Message0, State) of
        {'EXIT', Reason} ->
            internal_rollback(State),
            exit(Reason);
        {ok, NewState} ->
            NewState
    end;

handle_message({lock_message, DeliveryTag, ConsumerTag, QName, QPid, Message},
               State = #tstate{unacked_message_q = UAMQ}) ->
    State#tstate{unacked_message_q = queue:in({DeliveryTag, ConsumerTag, QName, QPid, Message},
                                              UAMQ)};

handle_message({release_message, DeliveryTag, Multiple},
               State = #tstate{transaction_id = TxnKey,
                               uncommitted_ack_q = UAQ,
                               unacked_message_q = UAMQ}) ->
    {Acked, Remaining} = collect_acks(UAMQ, DeliveryTag, Multiple),
    persist_acks(Acked, TxnKey),
    case TxnKey of
        none ->
            State#tstate{unacked_message_q = Remaining};
        _ ->
            State#tstate{uncommitted_ack_q = queue:join(UAQ, Acked),
                         unacked_message_q = Remaining}
    end;

handle_message(select,
               State) ->
    State#tstate{transaction_id = rabbit_persister:begin_transaction()};

handle_message({commit, ReplyPid}, State) ->
    case catch internal_commit(State) of
        {'EXIT', Reason} ->
            internal_rollback(State),
            exit(Reason);
        NewState ->
            ReplyPid ! commit_ok,
            NewState
    end;

handle_message({rollback, ReplyPid},
               State) ->
    NewState = internal_rollback(State),
    ReplyPid ! rollback_ok,
    NewState#tstate{transaction_id = rabbit_persister:begin_transaction()};

handle_message({recover, ReplyPid, ShouldRequeue},
               State = #tstate{writer_pid = WriterPid, unacked_message_q = UAMQ}) ->
    ok = if
             ShouldRequeue ->
                 requeue_messages([UAMQ]);
             true ->
                 redeliver_messages(WriterPid, UAMQ)
         end,
    ReplyPid ! recover_ok,
    State#tstate{unacked_message_q = queue:new()};

handle_message({set_writer_pid, WriterPid},
               State) ->
    State#tstate{writer_pid = WriterPid};

handle_message(shutdown,
               State) ->
    internal_rollback(State),
    exit(normal);

handle_message({'EXIT', _Pid, Reason},
               State) ->
    internal_rollback(State),
    exit(Reason);

handle_message(Message,
               State) ->
    internal_rollback(State),
    exit({transaction, message_not_understood, Message}).

%---------------------------------------------------------------------------

internal_publish(Mandatory, Immediate, Message0,
                 State = #tstate{writer_pid = WriterPid,
                                 transaction_id = TxnKey,
                                 uncommitted_publish_q = UPQ}) ->
    {Message, Handlers} = publish_tophalf(TxnKey, Message0),
    case TxnKey of
        none ->
            ok = publish_bottomhalf(TxnKey, Handlers, Mandatory, Immediate,
                                    Message, WriterPid),
            {ok, State};
        _ ->
            {ok, case check_routability(Handlers, Mandatory, Immediate,
                                        Message, WriterPid) of
                     true  -> State#tstate{uncommitted_publish_q
                                           = queue:in({Handlers, Immediate, Message}, UPQ)};
                     false -> State
                 end}
    end.

publish_tophalf(TxnKey,
                Message0 = #basic_message{exchange_name = ExchangeName,
                                          routing_key = RoutingKey}) ->
    Message = persist_publish(Message0, TxnKey),
    Exchange = rabbit_exchange:lookup_or_die(ExchangeName, 'basic.publish'),
    {Message, rabbit_exchange:route(Exchange, RoutingKey)}.

check_routability(Handlers, Mandatory, Immediate, Message, WriterPid) ->
    %% The wording in the spec appears to imply that messages are
    %% routed to queues straight away, rather than when the tx
    %% commits. Consequently routing errors should only be raised
    %% here.
    %%
    %% As a result mandatory messages may vanish without a trace later
    %% on, e.g. if the queues have disappeared by the time we
    %% commit. However, that is observationally equivalent to the
    %% messages vanishing as part of the queues, since the protocol
    %% offers no way of determining whether a queue contains
    %% uncommitted messages. Contrast this with the case of
    %% ordinary/committed messages, whose presence on a queue can be
    %% ascertained, by, say, invoking queue.delete with IfEmpty=true.
    Routable = not rabbit_exchange:handlers_isempty(Handlers),
    case Routable of 
        false when Mandatory ->
            %% FIXME: 312 should be replaced by the ?NO_ROUTE
            %% definition, when we move to >=0-9
            ok = basic_return(Message, WriterPid, 312, <<"unroutable">>);
        false when Immediate ->
            %% FIXME: 313 should be replaced by the ?NO_CONSUMERS
            %% definition, when we move to >=0-9
            ok = basic_return(Message, WriterPid, 313, <<"not_delivered">>);
        _Other -> ok
    end,
    Routable.

persist_delivery(_TxnKey, none, _QueueNames) ->
    ok;
persist_delivery(TxnKey, PKey, QueueNames) ->
    ok = persist_work(TxnKey, [{delivery, PKey, QueueNames}]).

publish_bottomhalf(TxnKey, Handlers, Mandatory, Immediate,
                   Message, WriterPid) ->
    QueueNames = deliver(Handlers, Mandatory, Immediate, Message, WriterPid),
    ok = persist_delivery(TxnKey, Message#basic_message.persistent_key, QueueNames).

deliver(Handlers, Mandatory, Immediate, Message, WriterPid) ->
    case rabbit_exchange:deliver(Handlers, Mandatory, Immediate, Message) of
        {ok, QueueNames}       -> QueueNames;
        {error, unroutable}    ->
            %% FIXME: 312 should be replaced by the ?NO_ROUTE
            %% definition, when we move to >=0-9
            ok = basic_return(Message, WriterPid, 312, <<"unroutable">>),
            [];
        {error, not_delivered} ->
            %% FIXME: 313 should be replaced by the ?NO_CONSUMERS
            %% definition, when we move to >=0-9
            ok = basic_return(Message, WriterPid, 313, <<"not_delivered">>),
            []
    end.

basic_return(#basic_message{exchange_name = ExchangeName,
                            routing_key   = RoutingKey,
                            content       = Content},
             WriterPid, ReplyCode, ReplyText) ->
    ok = rabbit_writer:send_command(
           WriterPid,
           #'basic.return'{reply_code  = ReplyCode,
                           reply_text  = ReplyText,
                           exchange    = ExchangeName#resource.name,
                           routing_key = RoutingKey},
           Content).

collect_acks(Q, DeliveryTag, Multiple) ->
    collect_acks(queue:new(), queue:new(), Q, DeliveryTag, Multiple).

collect_acks(ToAcc, PrefixAcc, Q, DeliveryTag, Multiple) ->
    %% FIXME: Unknown delivery tag -> channel exception
    %% FIXME: Should explicitly support "0" tag as "all messages so far"
    case queue:out(Q) of
        {{value, UnackedMsg = {CurrentDeliveryTag, _ConsumerTag, _QName, _QPid, _Message}},
         QTail} ->
            if 
                CurrentDeliveryTag == DeliveryTag ->
                    {queue:in(UnackedMsg, ToAcc), queue:join(PrefixAcc, QTail)};
                Multiple ->
                    collect_acks(queue:in(UnackedMsg, ToAcc), PrefixAcc,
                                 QTail, DeliveryTag, Multiple);
                true ->
                    collect_acks(ToAcc, queue:in(UnackedMsg, PrefixAcc),
                                 QTail, DeliveryTag, Multiple)
            end;
        {empty, _} ->
            {ToAcc, PrefixAcc}
    end.

internal_commit(State = #tstate{writer_pid = WriterPid,
                                transaction_id = TxnKey,
                                uncommitted_publish_q = UPQ}) ->
    ok = run_publish_queue(TxnKey, UPQ, WriterPid),
    ok = rabbit_persister:commit_transaction(TxnKey),
    State#tstate{transaction_id = rabbit_persister:begin_transaction(),
                 uncommitted_publish_q = queue:new(),
                 uncommitted_ack_q = queue:new()}.

run_publish_queue(TxnKey, UPQ, WriterPid) ->
    case queue:out(UPQ) of
        {{value, {Handlers, Immediate, Message}}, Tail} ->
            ok = publish_bottomhalf(TxnKey, Handlers, false, Immediate,
                                    Message, WriterPid),
            run_publish_queue(TxnKey, Tail, WriterPid);
        {empty, _} ->
            ok
    end.

internal_rollback(State = #tstate{transaction_id = TxnKey,
                                  uncommitted_ack_q = UAQ,
                                  unacked_message_q = UAMQ}) ->
    rabbit_log:debug("Internal rollback ~p~n  - ~p acks uncommitted, ~p messages unacked, ~p publishes uncommitted~n",
              [self(),
               queue:len(UAQ),
               queue:len(UAMQ),
               queue:len(State#tstate.uncommitted_publish_q)]),
    ok = rabbit_persister:rollback_transaction(TxnKey),
    ok = requeue_messages([UAMQ, UAQ]),
    State#tstate{uncommitted_publish_q = queue:new(),
                 uncommitted_ack_q = queue:new(),
                 unacked_message_q = queue:new()}.

redeliver_messages(WriterPid, MQ) ->
    case queue:out(MQ) of
        {{value, {_DeliveryTag, none, _QName, QPid, Message}}, Rest} ->
            %% Was sent as a basic.get_ok. We requeue it here since
            %% there's nothing else sensible to do. FIXME: appropriate?
            ok = internal_requeue_message_queue(QPid, queue:from_list([Message])),
            redeliver_messages(WriterPid, Rest);
        {{value, {_DeliveryTag, ConsumerTag, QName, _QPid, Message}}, Rest} ->
            %% Was sent as a proper consumer delivery. Resend it as before.
            %% FIXME: It's given a new delivery tag. Should the old one be preserved?
            %% FIXME: What should happen if the consumer's been cancelled since?
            ok = rabbit_writer:deliver(WriterPid, ConsumerTag, true,
                                       QName, none,
                                       mark_message_redelivered(Message)),
            redeliver_messages(WriterPid, Rest);
        {empty, _} ->
            ok
    end.

requeue_messages(MessageQueues) ->
    D = lists:foldl(fun (MQ, D0) ->
                            sort_by_queue(queue:to_list(MQ), D0)
                    end, dict:new(), MessageQueues),
    ok = dict:fold(fun (QPid, MessageQueue, ok) ->
                           ?LOGDEBUG("Requeue: ~p ~p~n", [QPid, MessageQueue]),
                           ok = internal_requeue_message_queue(QPid, MessageQueue)
                   end, ok, D),
    ok.

internal_requeue_message_queue(QPid, MessageQueue) ->
    ok = gen_server:cast(QPid, {requeue, MessageQueue}).

sort_by_queue([], D) ->
    D;
sort_by_queue([{_DeliveryTag, _ConsumerTag, _QName, QPid, Message0} | Rest], D) ->
    Message = mark_message_redelivered(Message0),
    D1 = dict:update(QPid, fun (Q) -> queue:in(Message, Q) end, queue:in(Message, queue:new()), D),
    sort_by_queue(Rest, D1).

mark_message_redelivered(Message = #basic_message{}) ->
    Message#basic_message{redelivered = true}.

persist_work(none, WorkList) ->
    rabbit_persister:dirty_work(WorkList);
persist_work(TxnKey, WorkList) ->
    rabbit_persister:extend_transaction(TxnKey, WorkList).

persist_publish(Message = #basic_message{content = Content}, TxnKey) ->
    Properties = Content#content.properties,
    case Properties#'P_basic'.delivery_mode of
        1 ->
            Message;
        2 ->
            Key = {msgkey, node(), erlang:now(), make_ref()},
            NewMessage = Message#basic_message{persistent_key = Key},
            Request = {publish,
                       %% Wipe the (recoverable) decoded properties before logging.
                       NewMessage#basic_message{content = Content#content{properties = none}}},
            ok = persist_work(TxnKey, [Request]),
            NewMessage;
        undefined ->
            %% We treat non-specified as non-persistent.
            Message;
        Other ->
            rabbit_log:warning("Unknown delivery mode ~p - treating as 1, non-persistent~n",
                               [Other]),
            Message
    end.

persist_acks(Acks, TxnKey) ->
    Messages = persist_acks1([], Acks),
    ok = persist_work(TxnKey, Messages).

persist_acks1(Acc, AckQ) ->
    case queue:out(AckQ) of
        {{value,
          {_DeliveryTag, _ConsumerTag, QName, _QPid, #basic_message{persistent_key = PKey}}},
         Tail} ->
            case PKey of
                none ->
                    persist_acks1(Acc, Tail);
                _ ->
                    persist_acks1([{ack, QName, PKey} | Acc], Tail)
            end;
        {empty, _} ->
            Acc
    end.
