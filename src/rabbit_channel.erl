%%   The contents of this file are subject to the Mozilla Public Licenses
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
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_channel).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([start_link/4, do/2, do/3, shutdown/1]).
-export([send_command/2, deliver/4]).

%% callbacks
-export([init/2, handle_message/2]).

-record(ch, {state, proxy_pid, reader_pid, writer_pid,
             transaction_id, tx_participants, next_tag,
             uncommitted_ack_q, unacked_message_q,
             username, virtual_host,
             most_recently_declared_queue, consumer_mapping, next_ticket}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/4 :: (pid(), pid(), username(), vhost()) -> pid()).
-spec(do/2 :: (pid(), amqp_method()) -> 'ok').
-spec(do/3 :: (pid(), amqp_method(), maybe(content())) -> 'ok').
-spec(shutdown/1 :: (pid()) -> 'ok').
-spec(send_command/2 :: (pid(), amqp_method()) -> 'ok').
-spec(deliver/4 :: (pid(), ctag(), bool(), msg()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

start_link(ReaderPid, WriterPid, Username, VHost) ->
    buffering_proxy:start_link(?MODULE, [ReaderPid, WriterPid,
                                         Username, VHost]).

do(Pid, Method) ->
    do(Pid, Method, none).

do(Pid, Method, Content) ->
    Pid ! {method, Method, Content},
    ok.

shutdown(Pid) ->
    Pid ! terminate,
    ok.

send_command(Pid, Msg) ->
    Pid ! {command, Msg},
    ok.

deliver(Pid, ConsumerTag, AckRequired, Msg) ->
    Pid ! {deliver, ConsumerTag, AckRequired, Msg},
    ok.

%%---------------------------------------------------------------------------

init(ProxyPid, [ReaderPid, WriterPid, Username, VHost]) ->
    process_flag(trap_exit, true),
    link(WriterPid),
    #ch{state                   = starting,
        proxy_pid               = ProxyPid,
        reader_pid              = ReaderPid,
        writer_pid              = WriterPid,
        transaction_id          = none,
        tx_participants         = sets:new(),
        next_tag                = 1,
        uncommitted_ack_q       = queue:new(),
        unacked_message_q       = queue:new(),
        username                = Username,
        virtual_host            = VHost,
        most_recently_declared_queue = <<>>,
        consumer_mapping        = dict:new(),
        next_ticket             = 101}.

handle_message({method, Method, Content}, State) ->
    case (catch handle_method(Method, Content, State)) of
        {reply, Reply, NewState} ->
            ok = rabbit_writer:send_command(NewState#ch.writer_pid, Reply),
            NewState;
        {noreply, NewState} ->
            NewState;
        stop ->
            exit(normal);
        {'EXIT', {amqp, Error, Explanation, none}} ->
            terminate({amqp, Error, Explanation,
                       rabbit_misc:method_record_type(Method)},
                      State);
        {'EXIT', Reason} ->
            terminate(Reason, State)
    end;

handle_message(terminate, State) ->
    terminate(normal, State);

handle_message({command, Msg}, State = #ch{writer_pid = WriterPid}) ->
    ok = rabbit_writer:send_command(WriterPid, Msg),
    State;

handle_message({deliver, ConsumerTag, AckRequired, Msg},
               State = #ch{proxy_pid = ProxyPid,
                           writer_pid = WriterPid,
                           next_tag = DeliveryTag}) ->
    State1 = lock_message(AckRequired, {DeliveryTag, ConsumerTag, Msg}, State),
    ok = internal_deliver(WriterPid, ProxyPid,
                          true, ConsumerTag, DeliveryTag, Msg),
    State1#ch{next_tag = DeliveryTag + 1};

handle_message({'EXIT', _Pid, Reason}, State) ->
    terminate(Reason, State);

handle_message(Other, State) ->
    terminate({unexpected_channel_message, Other}, State).

%%---------------------------------------------------------------------------

terminate(Reason, State = #ch{writer_pid = WriterPid}) ->
    Res = notify_queues(internal_rollback(State)),
    ok = rabbit_realm:leave_realms(self()),
    case Reason of
        normal -> ok = Res;
        _      -> ok
    end,
    rabbit_writer:shutdown(WriterPid),
    exit(Reason).

return_ok(State, true, _Msg)  -> {noreply, State};
return_ok(State, false, Msg)  -> {reply, Msg, State}.

ok_msg(true, _Msg) -> undefined;
ok_msg(false, Msg) -> Msg.

return_queue_declare_ok(State, NoWait, Q) ->
    NewState = State#ch{most_recently_declared_queue =
                        (Q#amqqueue.name)#resource.name},
    case NoWait of
        true  -> {noreply, NewState};
        false ->
            {ok, ActualName, MessageCount, ConsumerCount} =
                rabbit_misc:with_exit_handler(
                  fun () -> {ok, Q#amqqueue.name, 0, 0} end,
                  fun () -> rabbit_amqqueue:stat(Q) end),
            Reply = #'queue.declare_ok'{queue = ActualName#resource.name,
                                        message_count = MessageCount,
                                        consumer_count = ConsumerCount},
            {reply, Reply, NewState}
    end.

expand_queue_name_shortcut(<<>>, #ch{ most_recently_declared_queue = <<>> }) ->
    rabbit_misc:protocol_error(
      not_allowed, "no previously declared queue", []);
expand_queue_name_shortcut(<<>>, #ch{ virtual_host = VHostPath,
                                      most_recently_declared_queue = MRDQ }) ->
    rabbit_misc:r(VHostPath, queue, MRDQ);
expand_queue_name_shortcut(QueueNameBin, #ch{ virtual_host = VHostPath }) ->
    rabbit_misc:r(VHostPath, queue, QueueNameBin).

expand_routing_key_shortcut(<<>>, <<>>,
                            #ch{ most_recently_declared_queue = <<>> }) ->
    rabbit_misc:protocol_error(
      not_allowed, "no previously declared queue", []);
expand_routing_key_shortcut(<<>>, <<>>,
                            #ch{ most_recently_declared_queue = MRDQ }) ->
    MRDQ;
expand_routing_key_shortcut(_QueueNameBin, RoutingKey, _State) ->
    RoutingKey.

die_precondition_failed(Fmt, Params) ->
    %% FIXME: 406 should be replaced with precondition_failed when we
    %% move to AMQP spec >=8.1
    rabbit_misc:protocol_error({false, 406, <<"PRECONDITION_FAILED">>},
                               Fmt, Params).

check_ticket(TicketNumber, FieldIndex, Name, #ch{ username = Username}) ->
    rabbit_ticket:check_ticket(TicketNumber, FieldIndex, Name, Username).

lookup_ticket(TicketNumber, FieldIndex,
              #ch{ username = Username, virtual_host = VHostPath }) ->
    rabbit_ticket:lookup_ticket(TicketNumber, FieldIndex,
                                Username, VHostPath).

%% check that an exchange/queue name does not contain the reserved
%% "amq."  prefix.
%%
%% One, quite reasonable, interpretation of the spec, taken by the
%% QPid M1 Java client, is that the exclusion of "amq." prefixed names
%% only applies on actual creation, and not in the cases where the
%% entity already exists. This is how we use this function in the code
%% below. However, AMQP JIRA 123 changes that in 0-10, and possibly
%% 0-9SP1, making it illegal to attempt to declare an exchange/queue
%% with an amq.* name when passive=false. So this will need
%% revisiting.
%%
%% TODO: enforce other constraints on name. See AMQP JIRA 69.
check_name(Kind, NameBin = <<"amq.", _/binary>>) ->
    rabbit_misc:protocol_error(
      access_refused,
      "~s name '~s' contains reserved prefix 'amq.*'",[Kind, NameBin]);
check_name(_Kind, NameBin) ->
    NameBin.

handle_method(#'channel.open'{}, _, State = #ch{state = starting}) ->
    {reply, #'channel.open_ok'{}, State#ch{state = running}};

handle_method(#'channel.open'{}, _, _State) ->
    rabbit_misc:protocol_error(
      command_invalid, "second 'channel.open' seen", []);

handle_method(_Method, _, #ch{state = starting}) ->
    rabbit_misc:protocol_error(channel_error, "expected 'channel.open'", []);

handle_method(#'channel.close'{}, _, State = #ch{writer_pid = WriterPid}) ->
    ok = notify_queues(internal_rollback(State)),
    ok = rabbit_realm:leave_realms(self()),
    ok = rabbit_writer:send_command(WriterPid, #'channel.close_ok'{}),
    ok = rabbit_writer:shutdown(WriterPid),
    stop;

handle_method(#'access.request'{realm = RealmNameBin,
                                exclusive = Exclusive,
                                passive = Passive,
                                active = Active,
                                write = Write,
                                read = Read},
              _, State = #ch{username = Username,
                             virtual_host = VHostPath,
                             next_ticket = NextTicket}) ->
    RealmName = rabbit_misc:r(VHostPath, realm, RealmNameBin),
    Ticket = #ticket{realm_name = RealmName,
                     passive_flag = Passive,
                     active_flag = Active,
                     write_flag = Write,
                     read_flag = Read},
    case rabbit_realm:access_request(Username, Exclusive, Ticket) of
        ok ->
            rabbit_ticket:record_ticket(NextTicket, Ticket),
            NewState = State#ch{next_ticket = NextTicket + 1},
            {reply, #'access.request_ok'{ticket = NextTicket}, NewState};
        {error, not_found} ->
            rabbit_misc:protocol_error(
              invalid_path, "no ~s", [rabbit_misc:rs(RealmName)]);
        {error, bad_realm_path} ->
            %% FIXME: spec bug? access_refused is a soft error, spec requires it to be hard
            rabbit_misc:protocol_error(
              access_refused, "bad path for ~s", [rabbit_misc:rs(RealmName)]);
        {error, resource_locked} ->
            rabbit_misc:protocol_error(
              resource_locked, "~s is locked", [rabbit_misc:rs(RealmName)]);
        {error, access_refused} ->
            rabbit_misc:protocol_error(
              access_refused,
              "~w permissions denied for user '~s' attempting to access ~s",
              [rabbit_misc:permission_list(Ticket),
               Username, rabbit_misc:rs(RealmName)])
    end;

handle_method(#'basic.publish'{ticket = TicketNumber,
                               exchange = ExchangeNameBin,
                               routing_key = RoutingKey,
                               mandatory = Mandatory,
                               immediate = Immediate},
              Content, State = #ch{ virtual_host = VHostPath}) ->
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_ticket(TicketNumber, #ticket.write_flag, ExchangeName, State),
    Exchange = rabbit_exchange:lookup_or_die(ExchangeName),
    %% We decode the content's properties here because we're almost
    %% certain to want to look at delivery-mode and priority.
    DecodedContent = rabbit_binary_parser:ensure_content_decoded(Content),
    PersistentKey = case is_message_persistent(DecodedContent) of
                        true  -> rabbit_misc:guid();
                        false -> none
                    end,
    {noreply, publish(Mandatory, Immediate,
                      #basic_message{exchange_name  = ExchangeName,
                                     routing_key    = RoutingKey,
                                     content        = DecodedContent,
                                     persistent_key = PersistentKey},
                      rabbit_exchange:route(Exchange, RoutingKey), State)};

handle_method(#'basic.ack'{delivery_tag = DeliveryTag,
                           multiple = Multiple},
              _, State = #ch{transaction_id = TxnKey,
                             next_tag = NextDeliveryTag,
                             unacked_message_q = UAMQ}) ->
    if DeliveryTag >= NextDeliveryTag ->
            rabbit_misc:protocol_error(
              command_invalid, "unknown delivery tag ~w", [DeliveryTag]);
       true -> ok
    end,
    {Acked, Remaining} = collect_acks(UAMQ, DeliveryTag, Multiple),
    Participants = ack(State#ch.proxy_pid, TxnKey, Acked),
    {noreply, case TxnKey of
                  none -> State#ch{unacked_message_q = Remaining};
                  _    -> NewUAQ = queue:join(State#ch.uncommitted_ack_q,
                                              Acked),
                          add_tx_participants(
                            Participants,
                            State#ch{unacked_message_q = Remaining,
                                     uncommitted_ack_q = NewUAQ})
              end};

handle_method(#'basic.get'{ticket = TicketNumber,
                           queue = QueueNameBin,
                           no_ack = NoAck},
              _, State = #ch{ proxy_pid = ProxyPid, writer_pid = WriterPid,
                              next_tag = DeliveryTag }) ->
    QueueName = expand_queue_name_shortcut(QueueNameBin, State),
    check_ticket(TicketNumber, #ticket.read_flag, QueueName, State),
    case rabbit_amqqueue:with_or_die(
           QueueName,
           fun (Q) -> rabbit_amqqueue:basic_get(Q, ProxyPid, NoAck) end) of
        {ok, MessageCount,
         Msg = {_QName, _QPid, _MsgId, Redelivered,
                #basic_message{exchange_name = ExchangeName,
                               routing_key = RoutingKey,
                               content = Content}}} ->
            State1 = lock_message(not(NoAck), {DeliveryTag, none, Msg}, State),
            ok = rabbit_writer:send_command(
                   WriterPid,
                   #'basic.get_ok'{delivery_tag = DeliveryTag,
                                   redelivered = Redelivered,
                                   exchange = ExchangeName#resource.name,
                                   routing_key = RoutingKey,
                                   message_count = MessageCount},
                   Content),
            {noreply, State1#ch{next_tag = DeliveryTag + 1}};
        empty ->
            {reply, #'basic.get_empty'{cluster_id = <<>>}, State}
    end;

handle_method(#'basic.consume'{ticket = TicketNumber,
                               queue = QueueNameBin,
                               consumer_tag = ConsumerTag,
                               no_local = _, % FIXME: implement
                               no_ack = NoAck,
                               exclusive = ExclusiveConsume,
                               nowait = NoWait},
              _, State = #ch{ proxy_pid = ProxyPid,
                              reader_pid = ReaderPid,
                              consumer_mapping = ConsumerMapping }) ->
    case dict:find(ConsumerTag, ConsumerMapping) of
        error ->
            QueueName = expand_queue_name_shortcut(QueueNameBin, State),
            check_ticket(TicketNumber, #ticket.read_flag, QueueName, State),
            ActualConsumerTag =
                case ConsumerTag of
                    <<>>  -> rabbit_misc:binstring_guid("amq.ctag");
                    Other -> Other
                end,

            %% In order to ensure that the consume_ok gets sent before
            %% any messages are sent to the consumer, we get the queue
            %% process to send the consume_ok on our behalf.
            case rabbit_amqqueue:with_or_die(
                   QueueName,
                   fun (Q) ->
                           rabbit_amqqueue:basic_consume(
                             Q, NoAck, ReaderPid, ProxyPid,
                             ActualConsumerTag, ExclusiveConsume,
                             ok_msg(NoWait, #'basic.consume_ok'{
                                      consumer_tag = ActualConsumerTag}))
                   end) of
                ok ->
                    {noreply, State#ch{consumer_mapping =
                                       dict:store(ActualConsumerTag,
                                                  QueueName,
                                                  ConsumerMapping)}};
                {error, queue_owned_by_another_connection} ->
                    %% The spec is silent on which exception to use
                    %% here. This seems reasonable? 
                    %% FIXME: check this

                    rabbit_misc:protocol_error(
                      resource_locked, "~s owned by another connection",
                      [rabbit_misc:rs(QueueName)]);
                {error, exclusive_consume_unavailable} ->
                    rabbit_misc:protocol_error(
                      access_refused, "~s in exclusive use",
                      [rabbit_misc:rs(QueueName)])
            end;
        {ok, _} ->
            %% Attempted reuse of consumer tag.
            rabbit_misc:protocol_error(
              not_allowed, "attempt to reuse consumer tag '~s'", [ConsumerTag])
    end;

handle_method(#'basic.cancel'{consumer_tag = ConsumerTag,
                              nowait = NoWait},
              _, State = #ch{ proxy_pid = ProxyPid,
                              consumer_mapping = ConsumerMapping }) ->
    OkMsg = #'basic.cancel_ok'{consumer_tag = ConsumerTag},
    case dict:find(ConsumerTag, ConsumerMapping) of
        error ->
            %% Spec requires we ignore this situation.
            return_ok(State, NoWait, OkMsg);
        {ok, QueueName} ->
            NewState = State#ch{consumer_mapping =
                                dict:erase(ConsumerTag,
                                           ConsumerMapping)},
            case rabbit_amqqueue:with(
                   QueueName,
                   fun (Q) ->
                           %% In order to ensure that no more messages
                           %% are sent to the consumer after the
                           %% cancel_ok has been sent, we get the
                           %% queue process to send the cancel_ok on
                           %% our behalf. If we were sending the
                           %% cancel_ok ourselves it might overtake a
                           %% message sent previously by the queue.
                           rabbit_amqqueue:basic_cancel(
                             Q, ProxyPid, ConsumerTag,
                             ok_msg(NoWait, #'basic.cancel_ok'{
                                      consumer_tag = ConsumerTag}))
                   end) of
                ok ->
                    {noreply, NewState};
                {error, not_found} ->
                    %% Spec requires we ignore this situation.
                    return_ok(NewState, NoWait, OkMsg)
            end
    end;

handle_method(#'basic.qos'{}, _, State) ->
    %% FIXME: Need to implement QOS
    {reply, #'basic.qos_ok'{}, State};

handle_method(#'basic.recover'{requeue = true},
              _, State = #ch{ transaction_id = none,
                              proxy_pid = ProxyPid,
                              unacked_message_q = UAMQ }) ->
    ok = fold_per_queue(
           fun (QPid, MsgIds, ok) ->
                   %% The Qpid python test suite incorrectly assumes
                   %% that messages will be requeued in their original
                   %% order. To keep it happy we reverse the id list
                   %% since we are given them in reverse order.
                   rabbit_amqqueue:requeue(
                     QPid, lists:reverse(MsgIds), ProxyPid)
           end, ok, UAMQ),
    %% No answer required, apparently!
    {noreply, State#ch{unacked_message_q = queue:new()}};

handle_method(#'basic.recover'{requeue = false},
              _, State = #ch{ transaction_id = none,
                              proxy_pid = ProxyPid,
                              writer_pid = WriterPid,
                              unacked_message_q = UAMQ }) ->
    lists:foreach(
      fun ({_DeliveryTag, none, _Msg}) ->
              %% Was sent as a basic.get_ok. Don't redeliver
              %% it. FIXME: appropriate?
              ok;
          ({DeliveryTag, ConsumerTag,
            {QName, QPid, MsgId, _Redelivered, Message}}) ->
              %% Was sent as a proper consumer delivery.  Resend it as
              %% before.
              %%
              %% FIXME: What should happen if the consumer's been
              %% cancelled since?
              %%
              %% FIXME: should we allocate a fresh DeliveryTag?
              ok = internal_deliver(
                     WriterPid, ProxyPid,
                     false, ConsumerTag, DeliveryTag,
                     {QName, QPid, MsgId, true, Message})
      end, queue:to_list(UAMQ)),
    %% No answer required, apparently!
    {noreply, State};

handle_method(#'basic.recover'{}, _, _State) ->
    rabbit_misc:protocol_error(
      not_allowed, "attempt to recover a transactional channel",[]);

handle_method(#'exchange.declare'{ticket = TicketNumber,
                                  exchange = ExchangeNameBin,
                                  type = TypeNameBin,
                                  passive = false,
                                  durable = Durable,
                                  auto_delete = AutoDelete,
                                  internal = false,
                                  nowait = NoWait,
                                  arguments = Args},
              _, State = #ch{ virtual_host = VHostPath }) ->
    #ticket{realm_name = RealmName} =
        lookup_ticket(TicketNumber, #ticket.active_flag, State),
    CheckedType = rabbit_exchange:check_type(TypeNameBin),
    %% FIXME: clarify spec as per declare wrt differing realms
    X = case rabbit_exchange:lookup(
               rabbit_misc:r(VHostPath, exchange, ExchangeNameBin)) of
            {ok, FoundX} -> FoundX;
            {error, not_found} ->
                ActualNameBin = check_name('exchange', ExchangeNameBin),
                rabbit_exchange:declare(RealmName,
                                        ActualNameBin,
                                        CheckedType,
                                        Durable,
                                        AutoDelete,
                                        Args)
        end,
    ok = rabbit_exchange:assert_type(X, CheckedType),
    return_ok(State, NoWait, #'exchange.declare_ok'{});

handle_method(#'exchange.declare'{ticket = TicketNumber,
                                  exchange = ExchangeNameBin,
                                  type = TypeNameBin,
                                  passive = true,
                                  nowait = NoWait},
              _, State = #ch{ virtual_host = VHostPath }) ->
    %% FIXME: spec issue: permit active_flag here as well as passive_flag?
    #ticket{} = lookup_ticket(TicketNumber, #ticket.passive_flag, State),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    X = rabbit_exchange:lookup_or_die(ExchangeName),
    ok = rabbit_exchange:assert_type(X, rabbit_exchange:check_type(TypeNameBin)),
    return_ok(State, NoWait, #'exchange.declare_ok'{});

handle_method(#'exchange.delete'{ticket = TicketNumber,
                                 exchange = ExchangeNameBin,
                                 if_unused = IfUnused,
                                 nowait = NoWait},
              _, State = #ch { virtual_host = VHostPath }) ->
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_ticket(TicketNumber, #ticket.active_flag, ExchangeName, State),
    case rabbit_exchange:delete(ExchangeName, IfUnused) of
        {error, not_found} ->
            rabbit_misc:protocol_error(
              not_found, "no ~s", [rabbit_misc:rs(ExchangeName)]);
        {error, in_use} ->
            die_precondition_failed(
              "~s in use", [rabbit_misc:rs(ExchangeName)]);
        ok ->
            return_ok(State, NoWait,  #'exchange.delete_ok'{})
    end;

handle_method(#'queue.declare'{ticket = TicketNumber,
                               queue = QueueNameBin,
                               passive = false,
                               durable = Durable,
                               exclusive = ExclusiveDeclare,
                               auto_delete = AutoDelete,
                               nowait = NoWait,
                               arguments = Args},
              _, State = #ch { virtual_host = VHostPath,
                               reader_pid = ReaderPid }) ->
    #ticket{realm_name = RealmName} =
        lookup_ticket(TicketNumber, #ticket.active_flag, State),
    %% FIXME: atomic create&claim
    Finish =
        fun (Q) ->
                if ExclusiveDeclare ->
                        case rabbit_amqqueue:claim_queue(Q, ReaderPid) of
                            locked ->
                                %% AMQP 0-8 doesn't say which
                                %% exception to use, so we mimic QPid
                                %% here.
                                rabbit_misc:protocol_error(
                                  resource_locked,
                                  "cannot obtain exclusive access to locked ~s",
                                  [rabbit_misc:rs(Q#amqqueue.name)]);
                            ok -> ok
                        end;
                   true ->
                        ok
                end,
                Q
        end,
    %% FIXME: clarify spec as per declare wrt differing realms
    Q = case rabbit_amqqueue:with(
               rabbit_misc:r(VHostPath, queue, QueueNameBin),
               Finish) of
            {error, not_found} ->
                ActualNameBin =
                    case QueueNameBin of
                        <<>>  -> rabbit_misc:binstring_guid("amq.gen");
                        Other -> check_name('queue', Other)
                    end,
                Finish(rabbit_amqqueue:declare(RealmName,
                                               ActualNameBin,
                                               Durable,
                                               AutoDelete,
                                               Args));
            Other -> Other
        end,
    return_queue_declare_ok(State, NoWait, Q);

handle_method(#'queue.declare'{ticket = TicketNumber,
                               queue = QueueNameBin,
                               passive = true,
                               nowait = NoWait},
              _, State = #ch{ virtual_host = VHostPath }) ->
    #ticket{} = lookup_ticket(TicketNumber, #ticket.passive_flag, State),
    QueueName = rabbit_misc:r(VHostPath, queue, QueueNameBin),
    Q = rabbit_amqqueue:with_or_die(QueueName, fun (Q) -> Q end),
    return_queue_declare_ok(State, NoWait, Q);

handle_method(#'queue.delete'{ticket = TicketNumber,
                              queue = QueueNameBin,
                              if_unused = IfUnused,
                              if_empty = IfEmpty,
                              nowait = NoWait
                             },
              _, State) ->
    QueueName = expand_queue_name_shortcut(QueueNameBin, State),
    check_ticket(TicketNumber, #ticket.active_flag, QueueName, State),
    case rabbit_amqqueue:with_or_die(
           QueueName,
           fun (Q) -> rabbit_amqqueue:delete(Q, IfUnused, IfEmpty) end) of
        {error, in_use} ->
            die_precondition_failed(
              "~s in use", [rabbit_misc:rs(QueueName)]);
        {error, not_empty} ->
            die_precondition_failed(
              "~s not empty", [rabbit_misc:rs(QueueName)]);
        {ok, PurgedMessageCount} ->
            return_ok(State, NoWait,
                      #'queue.delete_ok'{
                               message_count = PurgedMessageCount})
    end;

handle_method(#'queue.bind'{ticket = TicketNumber,
                            queue = QueueNameBin,
                            exchange = ExchangeNameBin,
                            routing_key = RoutingKey,
                            nowait = NoWait,
                            arguments = Arguments},
              _, State = #ch{ virtual_host = VHostPath }) ->
    %% FIXME: connection exception (!) on failure?? (see rule named "failure" in spec-XML)
    %% FIXME: don't allow binding to internal exchanges - including the one named "" !
    QueueName = expand_queue_name_shortcut(QueueNameBin, State),
    ActualRoutingKey = expand_routing_key_shortcut(QueueNameBin, RoutingKey,
                                                   State),
    check_ticket(TicketNumber, #ticket.active_flag, QueueName, State),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    case rabbit_amqqueue:add_binding(QueueName, ExchangeName,
                                     ActualRoutingKey, Arguments) of
        {error, queue_not_found} -> 
            rabbit_misc:protocol_error(
              not_found, "no ~s", [rabbit_misc:rs(QueueName)]);
        {error, exchange_not_found} -> 
            rabbit_misc:protocol_error(
              not_found, "no ~s", [rabbit_misc:rs(ExchangeName)]);
        {error, durability_settings_incompatible} ->
            rabbit_misc:protocol_error(
              not_allowed, "durability settings of ~s incompatible with ~s",
              [rabbit_misc:rs(QueueName), rabbit_misc:rs(ExchangeName)]);
        {ok, _BindingCount} ->
            return_ok(State, NoWait, #'queue.bind_ok'{})
    end;

handle_method(#'queue.purge'{ticket = TicketNumber,
                             queue = QueueNameBin,
                             nowait = NoWait},
              _, State) ->
    QueueName = expand_queue_name_shortcut(QueueNameBin, State),
    check_ticket(TicketNumber, #ticket.read_flag, QueueName, State),
    {ok, PurgedMessageCount} = rabbit_amqqueue:with_or_die(
                                 QueueName,
                                 fun (Q) -> rabbit_amqqueue:purge(Q) end),
    return_ok(State, NoWait,
              #'queue.purge_ok'{message_count = PurgedMessageCount});

handle_method(#'tx.select'{}, _, State = #ch{transaction_id = none}) ->
    {reply, #'tx.select_ok'{}, new_tx(State)};

handle_method(#'tx.select'{}, _, State) ->
    {reply, #'tx.select_ok'{}, State};

handle_method(#'tx.commit'{}, _, #ch{transaction_id = none}) ->
    rabbit_misc:protocol_error(
      not_allowed, "channel is not transactional", []);

handle_method(#'tx.commit'{}, _, State) ->
    {reply, #'tx.commit_ok'{}, internal_commit(State)};

handle_method(#'tx.rollback'{}, _, #ch{transaction_id = none}) ->
    rabbit_misc:protocol_error(
      not_allowed, "channel is not transactional", []);

handle_method(#'tx.rollback'{}, _, State) ->
    {reply, #'tx.rollback_ok'{}, internal_rollback(State)};

handle_method(#'channel.flow'{active = _}, _, State) ->
    %% FIXME: implement
    {reply, #'channel.flow_ok'{active = true}, State};

handle_method(_MethodRecord, _Content, _State) ->
    rabbit_misc:protocol_error(
      command_invalid, "unimplemented method", []).

%%----------------------------------------------------------------------------

publish(Mandatory, Immediate, Message, QPids,
        State = #ch{transaction_id = TxnKey, writer_pid = WriterPid}) ->
    Handled = deliver(QPids, Mandatory, Immediate, TxnKey,
                      Message, WriterPid),
    case TxnKey of
        none -> State;
        _    -> add_tx_participants(Handled, State)
    end.

deliver(QPids, Mandatory, Immediate, Txn, Message, WriterPid) ->
    case rabbit_router:deliver(QPids, Mandatory, Immediate, Txn, Message) of
        {ok, DeliveredQPids}   -> DeliveredQPids;
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

collect_acks(Q, 0, true) ->
    {Q, queue:new()};
collect_acks(Q, DeliveryTag, Multiple) ->
    collect_acks(queue:new(), queue:new(), Q, DeliveryTag, Multiple).

collect_acks(ToAcc, PrefixAcc, Q, DeliveryTag, Multiple) ->
    case queue:out(Q) of
        {{value, UnackedMsg = {CurrentDeliveryTag, _ConsumerTag, _Msg}},
         QTail} ->
            if CurrentDeliveryTag == DeliveryTag ->
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

add_tx_participants(MoreP, State = #ch{tx_participants = Participants}) ->
    State#ch{tx_participants = sets:union(Participants,
                                          sets:from_list(MoreP))}.

ack(ProxyPid, TxnKey, UAQ) ->
    fold_per_queue(
      fun (QPid, MsgIds, L) ->
              ok = rabbit_amqqueue:ack(QPid, TxnKey, MsgIds, ProxyPid),
              [QPid | L]
      end, [], UAQ).

make_tx_id() -> rabbit_misc:guid().

safe_pmap_set_ok(F, S) ->
    case lists:filter(fun (R) -> R =/= ok end,
                      rabbit_misc:upmap(
                        fun (V) ->
                                try F(V)
                                catch Class:Reason -> {Class, Reason}
                                end
                        end, sets:to_list(S))) of
        []     -> ok;
        Errors -> {error, Errors}
    end.

notify_participants(F, TxnKey, Participants) ->
    safe_pmap_set_ok(fun (QPid) -> F(QPid, TxnKey) end, Participants).

new_tx(State) ->
    State#ch{transaction_id    = make_tx_id(),
             tx_participants   = sets:new(),
             uncommitted_ack_q = queue:new()}.

internal_commit(State = #ch{transaction_id = TxnKey,
                            tx_participants = Participants}) ->
    case notify_participants(fun rabbit_amqqueue:commit/2,
                             TxnKey, Participants) of
        ok              -> new_tx(State);
        {error, Errors} -> exit({commit_failed, Errors})
    end.

internal_rollback(State = #ch{transaction_id = TxnKey,
                              tx_participants = Participants,
                              uncommitted_ack_q = UAQ,
                              unacked_message_q = UAMQ}) ->
    ?LOGDEBUG("rollback ~p~n  - ~p acks uncommitted, ~p messages unacked~n",
              [self(),
               queue:len(UAQ),
               queue:len(UAMQ)]),
    case notify_participants(fun rabbit_amqqueue:rollback/2,
                             TxnKey, Participants) of
        ok              -> NewUAMQ = queue:join(UAQ, UAMQ),
                           new_tx(State#ch{unacked_message_q = NewUAMQ});
        {error, Errors} -> exit({rollback_failed, Errors})
    end.

fold_per_queue(F, Acc0, UAQ) ->
    D = lists:foldl(
          fun ({_DTag, _CTag,
                {_QName, QPid, MsgId, _Redelivered, _Message}}, D) ->
                  %% dict:append would be simpler and avoid the
                  %% lists:reverse in handle_message({recover, true},
                  %% ...). However, it is significantly slower when
                  %% going beyond a few thousand elements.
                  dict:update(QPid,
                              fun (MsgIds) -> [MsgId | MsgIds] end,
                              [MsgId],
                              D)
          end, dict:new(), queue:to_list(UAQ)),
    dict:fold(fun (QPid, MsgIds, Acc) -> F(QPid, MsgIds, Acc) end,
              Acc0, D).

notify_queues(#ch{proxy_pid = ProxyPid, consumer_mapping = Consumers}) ->
    safe_pmap_set_ok(
      fun (QueueName) ->
              case rabbit_amqqueue:with(
                     QueueName,
                     fun (Q) ->
                             rabbit_amqqueue:notify_down(Q, ProxyPid)
                     end) of
                  ok ->
                      ok;
                  {error, not_found} ->
                      %% queue has been deleted in the meantime
                      ok
              end
      end,
      dict:fold(fun (_ConsumerTag, QueueName, S) ->
                        sets:add_element(QueueName, S)
                end, sets:new(), Consumers)).

is_message_persistent(#content{properties = #'P_basic'{
                                 delivery_mode = Mode}}) ->
    case Mode of
        1         -> false;
        2         -> true;
        undefined -> false;
        Other     -> rabbit_log:warning("Unknown delivery mode ~p - treating as 1, non-persistent~n",
                                        [Other]),
                     false
    end.

lock_message(true, MsgStruct, State = #ch{unacked_message_q = UAMQ}) ->
    State#ch{unacked_message_q = queue:in(MsgStruct, UAMQ)};
lock_message(false, _MsgStruct, State) ->
    State.

internal_deliver(WriterPid, ChPid, Notify, ConsumerTag, DeliveryTag,
                 {_QName, QPid, _MsgId, Redelivered,
                  #basic_message{exchange_name = ExchangeName,
                                 routing_key = RoutingKey,
                                 content = Content}}) ->
    M = #'basic.deliver'{consumer_tag = ConsumerTag,
                         delivery_tag = DeliveryTag,
                         redelivered = Redelivered,
                         exchange = ExchangeName#resource.name,
                         routing_key = RoutingKey},
    ok = case Notify of
             true  -> rabbit_writer:send_command_and_notify(
                        WriterPid, QPid, ChPid, M, Content);
             false -> rabbit_writer:send_command(WriterPid, M, Content)
         end.
