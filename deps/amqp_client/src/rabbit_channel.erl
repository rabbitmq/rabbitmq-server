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
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_channel).
-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([safe_handle_method/3, handle_method/3]).

%---------------------------------------------------------------------------

safe_handle_method(MethodRecord, Content,
                   State = #ch{channel = Channel,
                               reader_pid = Reader,
                               writer_pid = Writer}) ->
    ?LOGMESSAGE(in, State#ch.channel, MethodRecord, Content),
    case catch handle_method(MethodRecord, Content, State) of
        {'EXIT', normal} ->
            %% the reader must be told about the channel closure
            %% *before* the close_ok is sent. Otherwise there is a
            %% race between a potential subsequent channel.open and
            %% the notification of the channel closure. This could
            %% result in the reader rejecting the channel.open,
            %% sending it into the ether, or considering the newly
            %% created channel as closed.
            Reader ! {channel_close, self()},
            receive
                terminate -> exit(normal);
                handshake -> ok
                    end,
            ok = rabbit_writer:send_command(Writer, #'channel.close_ok'{}),
            shutdown(State),
            stop;
        {'EXIT', Reason} ->
            ok = rabbit_realm:exit_realms(self()),
            Reader ! {channel_exception, Channel, Reason},
            shutdown(State),
            terminate;
        {reply, Reply, NewState} ->
            ok = rabbit_writer:send_command(Writer, Reply),
            NewState;
        {noreply, NewState} ->
            NewState
    end.

shutdown(State) ->
    rabbit_writer:shutdown(State#ch.writer_pid),
    rabbit_transaction:shutdown(State#ch.tx),
    ok.

return_ok(State, true, _Msg)  -> {noreply, State};
return_ok(State, false, Msg)  -> {reply, Msg, State}.

ok_msg(true, _Msg) -> undefined;
ok_msg(false, Msg) -> Msg.

return_queue_declare_ok(State, true, _Q) ->
    {noreply, State};
return_queue_declare_ok(State, false, Q) ->
    {ok, ActualName, MessageCount, ConsumerCount} = rabbit_amqqueue:stat(Q),
    Reply = #'queue.declare_ok'{queue = ActualName#resource.name,
                                message_count = MessageCount,
                                consumer_count = ConsumerCount},
    {reply, Reply, State}.

expand_queue_name_shortcut(<<>>, #ch{ most_recently_declared_queue = <<>> }, MethodName) ->
    rabbit_misc:die(not_allowed, MethodName);
expand_queue_name_shortcut(<<>>, #ch{ virtual_host = VHostPath,
                                      most_recently_declared_queue = MRDQ }, _MethodName) ->
    rabbit_misc:r(VHostPath, queue, MRDQ);
expand_queue_name_shortcut(QueueNameBin, #ch{ virtual_host = VHostPath }, _MethodName) ->
    rabbit_misc:r(VHostPath, queue, QueueNameBin).

die_precondition_failed(MethodName) ->
    %% FIXME: 406 should be replaced with precondition_failed when we move to AMQP spec >=8.1
    rabbit_misc:die({false, 406, <<"PRECONDITION_FAILED">>}, MethodName).

check_ticket(TicketNumber, FieldIndex, Name,
             #ch{ username = Username},
             Method) ->
    rabbit_ticket:check_ticket(TicketNumber, FieldIndex, Name,
                               Username, Method).

lookup_ticket(TicketNumber, FieldIndex,
              #ch{ username = Username, virtual_host = VHostPath },
              Method) ->
    rabbit_ticket:lookup_ticket(TicketNumber, FieldIndex,
                                Username, VHostPath, Method).

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
            rabbit_log:info("Realm access refused: no such realm; User ~p, VHost ~p, Realm ~p~n",
                            [Username, VHostPath, RealmNameBin]),
            rabbit_misc:die(invalid_path, 'access.request');
        {error, bad_realm_path} ->
            %% FIXME: spec bug? access_refused is a soft error, spec requires it to be hard
            rabbit_log:info("Realm access refused: bad path; User ~p, VHost ~p, Realm ~p~n",
                            [Username, VHostPath, RealmNameBin]),
            rabbit_misc:die(access_refused, 'access.request');
        {error, resource_locked} ->
            rabbit_log:info("Realm access refused: locked; User ~p, VHost ~p, Realm ~p~n",
                            [Username, VHostPath, RealmNameBin]),
            rabbit_misc:die(resource_locked, 'access.request');
        {error, access_refused} ->
            rabbit_log:info("Realm access refused: permission denied; User ~p, VHost ~p, Realm ~p~n",
                            [Username, VHostPath, RealmNameBin]),
            rabbit_misc:die(access_refused, 'access.request')
    end;

handle_method(#'basic.publish'{ticket = TicketNumber,
                               exchange = ExchangeNameBin,
                               routing_key = RoutingKey,
                               mandatory = Mandatory,
                               immediate = Immediate},
              Content, State = #ch{ virtual_host = VHostPath, tx = Tx }) ->
    %% We decode the content's properties here because we're almost
    %% certain to want to look at delivery-mode (ie persistence) and
    %% priority.
    DefinitelyDecodedContent = rabbit_binary_parser:ensure_content_decoded(Content),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_ticket(TicketNumber, #ticket.write_flag, ExchangeName,
                 State, 'basic.publish'),
    ok = rabbit_transaction:publish(Tx, Mandatory, Immediate,
                                    #basic_message{exchange_name = ExchangeName,
                                                   routing_key = RoutingKey,
                                                   content = DefinitelyDecodedContent,
                                                   redelivered = false,
                                                   persistent_key = none}),
    {noreply, State};

handle_method(#'basic.ack'{delivery_tag = DeliveryTag,
                           multiple = Multiple},
              _, State = #ch{ tx = Tx }) ->
    ok = rabbit_transaction:release_message(Tx, DeliveryTag, Multiple),
    {noreply, State};

handle_method(#'basic.get'{ticket = TicketNumber,
                           queue = QueueNameBin,
                           no_ack = NoAck},
              _, State = #ch{ writer_pid = WriterPid }) ->
    QueueName = expand_queue_name_shortcut(QueueNameBin, State, 'basic.get'),
    check_ticket(TicketNumber, #ticket.read_flag, QueueName,
                 State, 'basic.get'),
    Q = #amqqueue{pid = QPid} =
        rabbit_amqqueue:lookup_or_die(QueueName, 'basic.get'),
    case rabbit_amqqueue:basic_get(Q) of
        {ok, Message, MessageCount} ->
            rabbit_writer:get_ok(WriterPid, MessageCount, not(NoAck),
                                 QueueName, QPid, Message),
            {noreply, State};
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
              _, State = #ch{ reader_pid = ReaderPid,
                              writer_pid = WriterPid,
                              consumer_mapping = ConsumerMapping }) ->
    case dict:find(ConsumerTag, ConsumerMapping) of
        error ->
            ActualQueueName = expand_queue_name_shortcut(QueueNameBin, State, 'basic.consume'),
            check_ticket(TicketNumber, #ticket.read_flag, ActualQueueName,
                         State, 'basic.consume'),
            Q = rabbit_amqqueue:lookup_or_die(ActualQueueName, 'basic.consume'),
            ActualConsumerTag =
                case ConsumerTag of
                    <<>>  -> list_to_binary(rabbit_gensym:gensym("amq.ctag"));
                    Other -> Other
                end,

            %% In order to ensure that the consume_ok gets sent before
            %% any messages are sent to the consumer, we get the queue
            %% process to send the consume_ok on our behalf.
            case rabbit_amqqueue:basic_consume(
                   Q, NoAck, ReaderPid, WriterPid,
                   ActualConsumerTag, ExclusiveConsume,
                   ok_msg(NoWait, #'basic.consume_ok'{
                            consumer_tag = ActualConsumerTag})) of
                ok ->
                    {noreply, State#ch{consumer_mapping =
                                       dict:store(ActualConsumerTag,
                                                  ActualQueueName,
                                                  ConsumerMapping)}};
                {error, queue_owned_by_another_connection} ->
                    %% The spec is silent on which exception to use
                    %% here. This seems reasonable?
                    %% FIXME: check this
                    rabbit_misc:die(resource_locked, 'basic.consume');
                {error, exclusive_consume_unavailable} ->
                    rabbit_misc:die(access_refused, 'basic.consume')
            end;
        {ok, _} ->
            %% Attempted reuse of consumer tag.
            rabbit_log:error("Detected attempted reuse of consumer tag ~p~n", [ConsumerTag]),
            rabbit_misc:die(not_allowed)
    end;

handle_method(#'basic.cancel'{consumer_tag = ConsumerTag,
                              nowait = NoWait},
              _, State = #ch{ writer_pid = WriterPid,
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
            case rabbit_amqqueue:lookup(QueueName) of
                {error, not_found} ->
                    %% Spec requires we ignore this situation.
                    return_ok(NewState, NoWait, OkMsg);
                {ok, Q} ->
                    %% In order to ensure that no more messages are
                    %% sent to the consumer after the cancel_ok has
                    %% been sent, we get the queue process to send the
                    %% cancel_ok on our behalf. If we were sending the
                    %% cancel_ok ourselves it might overtake a message
                    %% sent previously by the queue.
                    rabbit_amqqueue:basic_cancel(
                      Q, WriterPid, ConsumerTag,
                      ok_msg(NoWait, #'basic.cancel_ok'{
                               consumer_tag = ConsumerTag})),
                    {noreply, NewState}
            end
    end;

handle_method(#'basic.qos'{}, _, State) ->
    % FIXME: Need to implement QOS
    {reply, #'basic.qos_ok'{}, State};

handle_method(#'basic.recover'{requeue = ShouldRequeue},
              _, State = #ch{ tx = Tx }) ->
    ok = rabbit_transaction:recover(Tx, ShouldRequeue),
    %% No answer required, apparently!
    {noreply, State};

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
    {ok, #ticket{realm_name = RealmName}} =
        lookup_ticket(TicketNumber, #ticket.active_flag,
                      State, 'exchange.declare'),
    % FIXME: atomic creation with no race
    % FIXME: how to deal with an exchange that exists in a different realm??

    % Workaround: QPid interprets the spec's meaning of "creation" of
    % an exchange, regarding the reserved "amq." prefix on exchange
    % names, to mean attempting to create an exchange that doesn't
    % already exist. That is, QPid seems to permit creation attempts
    % for amq.direct etc, since they already exist so aren't actually
    % created...
    ActualNameBin = rabbit_exchange:maybe_gensym_name(ExchangeNameBin),
    CheckedType = rabbit_exchange:check_type(TypeNameBin),
    X = case rabbit_exchange:lookup(rabbit_misc:r(VHostPath, exchange, ActualNameBin)) of
            {ok, FoundX} -> FoundX;
            {error, not_found} ->
                ok = rabbit_exchange:check_name(ExchangeNameBin),
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
    {ok, #ticket{}} =
        lookup_ticket(TicketNumber, #ticket.passive_flag,
                      State, 'exchange.declare'),
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    X = rabbit_exchange:lookup_or_die(ExchangeName, 'exchange.declare'),
    ok = rabbit_exchange:assert_type(X, rabbit_exchange:check_type(TypeNameBin)),
    return_ok(State, NoWait, #'exchange.declare_ok'{});

handle_method(#'exchange.delete'{ticket = TicketNumber,
                                 exchange = ExchangeNameBin,
                                 if_unused = IfUnused,
                                 nowait = NoWait},
              _, State = #ch { virtual_host = VHostPath }) ->
    ExchangeName = rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
    check_ticket(TicketNumber, #ticket.active_flag, ExchangeName,
                 State, 'exchange.delete'),
    case rabbit_exchange:delete(ExchangeName, IfUnused) of
        {error, not_found} -> rabbit_misc:die(not_found, 'exchange.delete');
        {error, in_use} -> die_precondition_failed('exchange.delete');
        ok -> return_ok(State, NoWait,  #'exchange.delete_ok'{})
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
    % FIXME: clarify spec as per exchange.declare wrt differing realms
    {ok, #ticket{realm_name = RealmName}} =
        lookup_ticket(TicketNumber, #ticket.active_flag,
                      State, 'queue.declare'),
    % FIXME: atomic creation with no race
    QueueName = rabbit_misc:r(VHostPath, queue, QueueNameBin),
    Q = case rabbit_amqqueue:lookup(QueueName) of
            {ok, FoundQ} -> FoundQ;
            {error, not_found} -> rabbit_amqqueue:declare(RealmName,
                                                          QueueNameBin,
                                                          Durable,
                                                          AutoDelete,
                                                          Args)
        end,
    ok = if
             ExclusiveDeclare ->
                 case rabbit_amqqueue:claim_queue(Q, ReaderPid) of
                     locked ->
                         %% AMQP 0-8 doesn't say which exception to use, so we mimic QPid here.
                         rabbit_misc:die(resource_locked, 'queue.declare');
                     ok -> ok
                 end;
             true ->
                 ok
         end,
    NewState = State#ch{most_recently_declared_queue =
                        (Q#amqqueue.name)#resource.name},
    return_queue_declare_ok(NewState, NoWait, Q);
    
handle_method(#'queue.declare'{ticket = TicketNumber,
                               queue = QueueNameBin,
                               passive = true,
                               nowait = NoWait},
              _, State = #ch{ virtual_host = VHostPath }) ->
    {ok, #ticket{}} =
        lookup_ticket(TicketNumber, #ticket.passive_flag,
                      State, 'queue.declare'),
    QueueName = rabbit_misc:r(VHostPath, queue, QueueNameBin),
    Q = rabbit_amqqueue:lookup_or_die(QueueName, 'queue.declare'),
    NewState = State#ch{most_recently_declared_queue =
                        (Q#amqqueue.name)#resource.name},
    return_queue_declare_ok(NewState, NoWait, Q);

handle_method(#'queue.delete'{ticket = TicketNumber,
                              queue = QueueNameBin,
                              if_unused = IfUnused,
                              if_empty = IfEmpty,
                              nowait = NoWait
                             },
              _, State) ->
    QueueName = expand_queue_name_shortcut(QueueNameBin, State, 'queue.delete'),
    check_ticket(TicketNumber, #ticket.active_flag, QueueName,
                 State, 'queue.delete'),
    Q = rabbit_amqqueue:lookup_or_die(QueueName, 'queue.delete'),
    case rabbit_amqqueue:delete(Q, IfUnused, IfEmpty) of
        {error, not_found} -> rabbit_misc:die(not_found, 'queue.delete');
        {error, in_use} -> die_precondition_failed('queue.delete');
        {error, not_empty} -> die_precondition_failed('queue.delete');
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
    % FIXME: connection exception (!) on failure?? (see rule named "failure" in spec-XML)
    % FIXME: don't allow binding to internal exchanges - including the one named "" !
    ActualQueueName = expand_queue_name_shortcut(QueueNameBin, State, 'queue.bind'),
    check_ticket(TicketNumber, #ticket.active_flag, ActualQueueName,
                 State, 'queue.bind'),
    case rabbit_amqqueue:add_binding(ActualQueueName,
                                     rabbit_misc:r(VHostPath, exchange, ExchangeNameBin),
                                     RoutingKey,
                                     Arguments) of
        {error, not_found} -> rabbit_misc:die(not_found, 'queue.bind');
        {error, durability_settings_incompatible} -> rabbit_misc:die(not_allowed, 'queue.bind');
        ok ->
            return_ok(State, NoWait, #'queue.bind_ok'{})
    end;

handle_method(#'queue.purge'{ticket = TicketNumber,
                             queue = QueueNameBin,
                             nowait = NoWait},
              _, State) ->
    ActualQueueName = expand_queue_name_shortcut(QueueNameBin, State, 'queue.purge'),
    check_ticket(TicketNumber, #ticket.read_flag, ActualQueueName,
                 State, 'queue.purge'),
    Q = rabbit_amqqueue:lookup_or_die(ActualQueueName, 'queue.purge'),
    {ok, PurgedMessageCount} = rabbit_amqqueue:purge(Q),
    return_ok(State, NoWait,
              #'queue.purge_ok'{message_count = PurgedMessageCount});

handle_method(#'tx.select'{}, _, State = #ch{ tx = Tx0 }) ->
    Tx1 = rabbit_transaction:select(Tx0),
    {reply, #'tx.select_ok'{}, State#ch{ tx = Tx1 }};

handle_method(#'tx.commit'{}, _, State = #ch{ tx = Tx }) ->
    ok = rabbit_transaction:commit(Tx),
    {reply, #'tx.commit_ok'{}, State};

handle_method(#'tx.rollback'{}, _, State = #ch{ tx = Tx }) ->
    ok = rabbit_transaction:rollback(Tx),
    {reply, #'tx.rollback_ok'{}, State};

handle_method(#'channel.open'{}, _, State) ->
    rabbit_log:error("Second channel.open seen on channel ~p~n",
                     [State#ch.channel]),
    rabbit_misc:die(command_invalid, 'channel.open');

handle_method(#'channel.close'{}, _, _State) ->
    exit(normal);

handle_method(#'channel.flow'{active = _}, _, State) ->
    %% FIXME: implement
    {reply, #'channel.flow_ok'{active = true}, State};

handle_method(MethodRecord, Content, State) ->
    rabbit_log:error("Unexpected ch~p method:~n-method ~p~n-content ~p~n",
              [State#ch.channel, MethodRecord, Content]),
    rabbit_misc:die(command_invalid, rabbit_misc:method_record_type(MethodRecord)).
