-module(rabbit_amqp1_0_session).

-behaviour(gen_server2).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-export([start_link/7, process_frame/2]).

-ifdef(debug).
-export([parse_destination/1]).
-endif.

-record(session, {channel_num, backing_connection, backing_channel,
                  declaring_channel,
                  reader_pid, writer_pid, transfer_number = 0,
                  outgoing_lwm = 0, outgoing_session_credit,
                  xfer_num_to_tag }).
-record(outgoing_link, {credit,
                        transfer_count = 0,
                        transfer_unit = 0,
                        no_ack}).

-record(incoming_link, {name, exchange, routing_key}).

-define(SEND_ROLE, false).
-define(RECV_ROLE, true).

-define(DEFAULT_OUTCOME, #'v1_0.released'{}).

-define(OUTCOMES, [?V_1_0_SYMBOL_ACCEPTED,
                   ?V_1_0_SYMBOL_REJECTED,
                   ?V_1_0_SYMBOL_RELEASED]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

%% We have to keep track of a few things for sessions,
%% across outgoing links:
%%  - transfer number
%%  - unsettled_lwm
%% across incoming links:
%%  - unsettled_lwm
%%  - session credit
%% and for each outgoing link,
%%  - credit we've been issued
%%  - unsettled messages
%% and for each incoming link,
%%  - how much credit we've issued
%%
%% TODO figure out how much of this actually needs to be serialised.
%% TODO links can be migrated between sessions -- seriously.

%% TODO account for all these things
start_link(Channel, ReaderPid, WriterPid, _Username, _VHost,
           _Collector, _StartLimiterFun) ->
    gen_server2:start_link(
      ?MODULE, [Channel, ReaderPid, WriterPid], []).

process_frame(Pid, Frame) ->
    gen_server2:cast(Pid, {frame, Frame}).

%% ---------

init([Channel, ReaderPid, WriterPid]) ->
    process_flag(trap_exit, true),
    %% TODO pass through authentication information
    {ok, Conn} = amqp_connection:start(direct),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    {ok, #session{ channel_num        = Channel,
                   backing_connection = Conn,
                   backing_channel    = Ch,
                   reader_pid         = ReaderPid,
                   writer_pid         = WriterPid,
                   xfer_num_to_tag    = dict:new()}}.

terminate(_Reason, #session{ backing_connection = Conn,
                             backing_channel    = Ch}) ->
    amqp_channel:close(Ch),
    amqp_connection:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(Msg, _From, State) ->
    {reply, {error, not_understood, Msg}, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    %% Handled above
    {noreply, State};

handle_info({#'basic.deliver'{consumer_tag = ConsumerTag,
                              delivery_tag = DeliveryTag}, Msg},
            State = #session{ writer_pid = WriterPid,
                              transfer_number = TransferNum }) ->
    %% FIXME, don't ignore ack required, keep track of credit, um .. etc.
    Handle = ctag_to_handle(ConsumerTag),
    case get({out, Handle}) of
        Link = #outgoing_link{} ->
            {NewLink, NewState} =
                transfer(WriterPid, Handle, Link, State, Msg, DeliveryTag),
            put({out, Handle}, NewLink),
            {noreply, NewState#session{
                        transfer_number = next_transfer_number(TransferNum)}};
        undefined ->
            %% FIXME handle missing link -- why does the queue think it's there?
            io:format("Delivery to non-existent consumer ~p", [ConsumerTag]),
            {noreply, State}
    end;

%% TODO these pretty much copied wholesale from rabbit_channel
handle_info({'EXIT', WriterPid, Reason = {writer, send_failed, _Error}},
            State = #session{writer_pid = WriterPid}) ->
    State#session.reader_pid ! {channel_exit, State#session.channel_num, Reason},
    {stop, normal, State};
handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};
handle_info({'DOWN', _MRef, process, _QPid, _Reason}, State) ->
    %% TODO do we care any more since we're using direct client?
    {noreply, State}. % FIXME rabbit_channel uses queue_blocked?

handle_cast({frame, Frame},
            State = #session{ writer_pid = Sock }) ->
    try handle_control(Frame, State) of
        {reply, Reply, NewState} ->
            ok = rabbit_amqp1_0_writer:send_command(Sock, Reply),
            noreply(NewState);
        {noreply, NewState} ->
            noreply(NewState);
        stop ->
            {stop, normal, State}
    catch exit:Reason = #'v1_0.error'{} ->
            %% TODO shut down nicely like rabbit_channel
            Close = #'v1_0.end'{ error = Reason },
            ok = rabbit_amqp1_0_writer:send_command(Sock, Close),
            {stop, normal, State};
          exit:normal ->
            {stop, normal, State};
          _:Reason ->
            {stop, {Reason, erlang:get_stacktrace()}, State}
    end.

%% TODO rabbit_channel returns {noreply, State, hibernate}, but that
%% appears to break things here (it stops the session responding to
%% frames).
noreply(State) ->
    {noreply, State}.

%% ------

handle_control(#'v1_0.begin'{}, State = #session{ channel_num = Channel }) ->
    {reply, #'v1_0.begin'{
       remote_channel = {ushort, Channel}}, State};

handle_control(#'v1_0.attach'{name = Name,
                              handle = Handle,
                              local = ClientLinkage,
                              transfer_unit = Unit,
                              role = ?SEND_ROLE}, %% client is sender
               State = #session{ outgoing_lwm = LWM }) ->
    %% TODO associate link name with target
    #'v1_0.linkage'{ source = Source, target = Target } = ClientLinkage,
    case ensure_target(Target, #incoming_link{ name = Name }, State) of
        {ok, ServerTarget, IncomingLink, State1} ->
            put({incoming, Handle}, IncomingLink),
            {reply,
             #'v1_0.attach'{
               name = Name,
               handle = Handle,
               remote = ClientLinkage,
               local = #'v1_0.linkage'{
                 source = Source,
                 target = ServerTarget },
               flow_state = #'v1_0.flow_state'{
                 link_credit = {uint, 50},
                 unsettled_lwm = {uint, LWM},
                 transfer_count = {uint, 0}},
               transfer_unit = Unit,
               role = ?RECV_ROLE}, %% server is receiver
             State1};
        {error, Reason, State1} ->
            rabbit_log:warning("AMQP 1.0 attach rejected ~p~n", [Reason]),
            {reply,
             #'v1_0.attach'{
               name = Name,
               handle = Handle,
               remote = ClientLinkage,
               local = undefined}, State1},
            protocol_error(?V_1_0_INVALID_FIELD,
                               "Attach rejected: ~p", [Reason])
    end;

%% TODO we don't really implement link-based flow control. Reject connections
%% that try to use it - except ATM the Python test case asks to use it
handle_control(#'v1_0.attach'{local = Linkage,
                              role = ?RECV_ROLE} = Attach, %% client is receiver
               State) ->
    %% TODO ensure_destination
    #'v1_0.linkage'{ source  = #'v1_0.source' {
                       default_outcome = DO,
                       outcomes = Os
                      }
                   } = Linkage,
    DefaultOutcome = case DO of
                         undefined -> ?DEFAULT_OUTCOME;
                         _         -> DO
                     end,
    Outcomes = case Os of
                   undefined -> ?OUTCOMES;
                   _         -> Os
               end,
    case lists:filter(fun(O) -> not lists:member(O, ?OUTCOMES) end, Outcomes) of
        []   -> attach_outgoing(DefaultOutcome, Outcomes, Attach, State);
        Bad  -> protocol_error(?V_1_0_NOT_IMPLEMENTED,
                               "Outcomes not supported: ~p", [Bad])
    end;

handle_control(#'v1_0.transfer'{handle = Handle,
                                fragments = Fragments},
                          State = #session{backing_channel = Ch}) ->
    case get({incoming, Handle}) of
        #incoming_link{ exchange = X, routing_key = RK } ->
            Msg = rabbit_amqp1_0_message:assemble(Fragments),
            amqp_channel:call(Ch, #'basic.publish' { exchange    = X,
                                                     routing_key = RK }, Msg);
        undefined ->
            %% FIXME What am I supposed to do here
            no_such_handle
    end,
    {noreply, State};

handle_control(#'v1_0.disposition'{ batchable = Batchable,
                                    extents = Extents,
                                    role = ?RECV_ROLE} = Disp, %% Client is receiver
               State) ->
    {SettledExtents, NewState} =
        lists:foldl(fun(Extent, {SettledExtents1, State1}) ->
                            {Settled, State2} = settle(Extent, Batchable, State1),
                            {[Settled | SettledExtents1], State2}
                    end, {[], State}, Extents),
    case lists:filter(fun (none) -> false;
                          (_Ext)  -> true
                      end, SettledExtents) of
        []   -> {noreply, NewState}; %% everything in its place
        Exts -> {reply,
                 Disp#'v1_0.disposition'{ extents = Exts,
                                          role = ?SEND_ROLE }, %% server is sender
                 NewState}
    end;

handle_control(#'v1_0.detach'{ handle = Handle }, State) ->
    erase({incoming, Handle}),
    {reply, #'v1_0.detach'{ handle = Handle }, State};

handle_control(#'v1_0.end'{}, #session{ writer_pid = Sock }) ->
    ok = rabbit_amqp1_0_writer:send_command(Sock, #'v1_0.end'{}),
    stop;

handle_control(Frame, State) ->
    io:format("Ignoring frame: ~p~n", [Frame]),
    {noreply, State}.

%% ------

protocol_error(Condition, Msg, Args) ->
    exit(#'v1_0.error'{
        condition = Condition,
        description = {utf8, list_to_binary(
                               lists:flatten(io_lib:format(Msg, Args)))}
       }).

attach_outgoing(DefaultOutcome, Outcomes,
                #'v1_0.attach'{name = Name,
                               handle = Handle,
                               local = Linkage,
                               flow_state = Flow = #'v1_0.flow_state'{
                                              session_credit = {uint, ClientSC},
                                              link_credit = {uint, Credit}
                                             },
                               transfer_unit = Unit},
               State = #session{backing_channel = Ch,
                                outgoing_session_credit = ServerSC}) ->
    NoAck = DefaultOutcome == #'v1_0.accepted'{} andalso
        Outcomes == [?V_1_0_SYMBOL_ACCEPTED],
    SessionCredit =
        case ServerSC of
            undefined -> #'basic.qos_ok'{} =
                             amqp_channel:call(Ch, #'basic.qos'{
                                                 prefetch_count = ClientSC}),
                         ClientSC;
            _         -> ServerSC
        end,
    #'v1_0.linkage'{ source = #'v1_0.source' {address = {utf8, Q}} } = Linkage,
    #'queue.declare_ok'{message_count = Available} =
        amqp_channel:call(Ch, #'queue.declare'{queue = Q, passive = true}),
    case amqp_channel:subscribe(
           Ch, #'basic.consume' { queue = Q,
                                  consumer_tag = handle_to_ctag(Handle),
                                  no_ack = NoAck,
                                  %% TODO exclusive?
                                  exclusive = false}, self()) of
        #'basic.consume_ok'{} ->
            %% FIXME we should avoid the race by getting the queue to send
            %% attach back, but a.t.m. it would use the wrong codec.
            put({out, Handle},
                #outgoing_link{
                  credit = Credit,
                  transfer_unit = Unit,
                  no_ack = NoAck
                 }),
            {reply, #'v1_0.attach'{
               name = Name,
               handle = Handle,
               remote = Linkage,
               local = #'v1_0.linkage'{
                 source = #'v1_0.source'{
                   address = {utf8, Q},
                   default_outcome = DefaultOutcome
                   %% TODO this breaks the Python client, when it
                   %% tries to send us back a matching detach message
                   %% it gets confused between described(true, [...])
                   %% and [...]. We think we're correct here
                   %% outcomes = Outcomes
                  }
                },
               flow_state = Flow#'v1_0.flow_state'{
                              available = {uint, Available}
                             },
               role = ?SEND_ROLE % server is sender
              }, State#session{outgoing_session_credit = SessionCredit}};
        Fail ->
            protocol_error(?V_1_0_INTERNAL_ERROR, "Consume failed: ~p", Fail)
    end.

transfer(WriterPid, LinkHandle,
         Link = #outgoing_link{ credit = Credit,
                                transfer_unit = Unit,
                                transfer_count = Count,
                                no_ack = NoAck},
         Session = #session{ transfer_number = TransferNumber,
                             outgoing_session_credit = SessionCredit,
                             xfer_num_to_tag = Dict },
         Msg = #amqp_msg{payload = Content},
         DeliveryTag) ->
    TransferSize = transfer_size(Content, Unit),
    NewLink = Link#outgoing_link{
                credit = Credit - TransferSize,
                transfer_count = Count + TransferSize
               },
    NewSession = Session#session {outgoing_session_credit = SessionCredit - 1},
    T = #'v1_0.transfer'{handle = LinkHandle,
                         flow_state = flow_state(NewLink, NewSession),
                         delivery_tag = {binary, <<DeliveryTag/integer>>},
                         transfer_id = {uint, TransferNumber},
                         settled = NoAck,
                         state = #'v1_0.transfer_state'{
                           %% TODO DUBIOUS this replicates information we
                           %% and the client already have
                           bytes_transferred = {ulong, 0}
                          },
                         resume = false,
                         more = false,
                         aborted = false,
                         batchable = false,
                         fragments =
                             rabbit_amqp1_0_message:fragments(Msg)},
    rabbit_amqp1_0_writer:send_command(WriterPid, T),
    {NewLink, NewSession#session { xfer_num_to_tag = dict:store(TransferNumber,
                                                                DeliveryTag,
                                                                Dict) }}.

settle(#'v1_0.extent'{
          first = {uint, First},
          last = {uint, Last}, %% TODO handle this
          handle = _Handle, %% TODO DUBIOUS what on earth is this for?
          settled = Settled,
          state = #'v1_0.transfer_state'{ outcome = Outcome }
         } = Extent,
       _Batchable, %% TODO is this documented anywhere? Handle it.
       State = #session{backing_channel = Ch,
                        outgoing_session_credit = SessionCredit,
                        xfer_num_to_tag = Dict}) ->
    {Dict1, SessionCredit1} =
        lists:foldl(
          fun (Transfer, {TransferMap, SC}) ->
                  DeliveryTag = dict:fetch(Transfer, TransferMap),
                  Ack =
                      case Outcome of
                          #'v1_0.accepted'{} ->
                              #'basic.ack' {delivery_tag = DeliveryTag,
                                            multiple     = false };
                          #'v1_0.rejected'{} ->
                              #'basic.reject' {delivery_tag = DeliveryTag,
                                               requeue      = false };
                          #'v1_0.released'{} ->
                              #'basic.reject' {delivery_tag = DeliveryTag,
                                               requeue      = true }
                      end,
                  ok = amqp_channel:call(Ch, Ack),
                  {dict:erase(Transfer, TransferMap), SC + 1}
          end,
          {Dict, SessionCredit}, lists:seq(First, Last)),
    {case Settled of
         true  -> none;
         false -> Extent#'v1_0.extent'{ settled = true }
     end,
     State#session{outgoing_session_credit = SessionCredit1,
                   xfer_num_to_tag = Dict1}}.

flow_state(#outgoing_link{credit = Credit,
                          transfer_count = Count},
           #session{outgoing_lwm = LWM,
                    outgoing_session_credit = SessionCredit}) ->
    #'v1_0.flow_state'{
            unsettled_lwm = {uint, LWM},
            session_credit = {uint, SessionCredit},
            transfer_count = {uint, Count},
            link_credit = {uint, Credit}
           }.

ensure_declaring_channel(State = #session{
                           backing_connection = Conn,
                           declaring_channel = undefined}) ->
    {ok, Ch} = amqp_connection:open_channel(Conn),
    State#session{declaring_channel = Ch};
ensure_declaring_channel(State) ->
    State.

%% There are a few things that influence what source and target
%% definitions mean for our purposes.
%%
%% Addresses: we artificially segregate exchanges and queues, since
%% they have different namespaces. However, we allow both incoming and
%% outgoing links to exchanges: outgoing links from an exchange
%% involve an anonymous queue.
%%
%% For targets, addresses are
%% Address = "/exchange/" Name
%%         | "/queue"
%%         | "/queue/" Name
%%
%% For sources, addresses are
%% Address = "/exchange/" Name "/" RoutingKey
%%         | "/queue/" Name
%%
%% We use the message property "Subject" as the equivalent of the
%% routing key.  In AMQP 0-9-1 terms, a target of /queue is equivalent
%% to the default exchange; that is, the message is routed to the
%% queue named by the subject.  A target of "/queue/Name" ignores the
%% subject.  The reason for both varieties is that a
%% dynamically-created queue must be fully addressable as a target,
%% while a service may wish to use /queue and route each message to
%% its reply-to queue name (as it is done in 0-9-1).
%%
%% A dynamic source or target only ever creates a queue, and the
%% address is returned in full; e.g., "/queue/amq.gen.123456".
%% However, that cannot be used as a reply-to, since a 0-9-1 client
%% will use it unaltered as the routing key naming the queue.
%% Therefore, we rewrite reply-to from 1.0 clients to be just the
%% queue name, and expect replying clients to use /queue and the
%% subject field.
%%
%% For a source queue, the distribution-mode is always move.  For a
%% source exchange, it is always copy. Anything else should be
%% refused.
%%
%% TODO default-outcome and outcomes, dynamic lifetimes

ensure_target(Target = #'v1_0.target'{address=Address,
                                      dynamic=Dynamic},
              #incoming_link{},
              State) ->
    case Dynamic of
        undefined ->
            case Address of
                {utf8, Destination} ->
                    case parse_destination(Destination) of
                        ["queue", Name] ->
                            case check_queue(Name, State) of
                                {ok, QueueName, State1} ->
                                    {ok, Target,
                                     #incoming_link{exchange = <<"">>,
                                                    routing_key = QueueName},
                                     State1};
                                {error, Reason, State1} ->
                                    {error, Reason, State1}
                            end;
                        ["queue"] ->
                            %% Rely on the Subject being set
                            {ok, Target, #incoming_link{exchange = <<"">>}, State};
                        ["exchange", Name] ->
                            case check_exchange(Name, State) of
                                {ok, ExchangeName, State1} ->
                                    {ok, Target,
                                     #incoming_link{exchange = ExchangeName},
                                     State1};
                                {error, Reason, State2} ->
                                    {error, Reason, State2}
                            end;
                        {error, Reason} ->
                            {error, Reason, State}
                    end;
                _Else ->
                    {error, {unknown_address, Address}, State}
            end;
        {symbol, Lifetime} ->
            case Address of
                undefined ->
                    {ok, QueueName, State1} = create_queue(Lifetime, State),
                    {ok,
                     Target#'v1_0.target'{address = {utf8, queue_address(QueueName)}},
                     #incoming_link{exchange = <<"">>, routing_key = QueueName},
                     State1};
                _Else ->
                    {error, both_dynamic_and_address_supplied, State}
            end
    end.

parse_destination(Destination) when is_binary(Destination) ->
    parse_destination(binary_to_list(Destination));
parse_destination(Destination) when is_list(Destination) ->
    case regexp:split(Destination, "/") of
        {ok, ["", Type | Tail]} when
              Type =:= "queue" orelse Type =:= "exchange" ->
            [Type | Tail];
        _Else ->
            {error, {malformed_address, Destination}}
    end.

%% Check that a queue exists
check_queue(QueueName, State) when is_list(QueueName) ->
    check_queue(list_to_binary(QueueName), State);
check_queue(QueueName, State) ->
    QDecl = #'queue.declare'{queue = QueueName, passive = true},
    State1 = #session{
      declaring_channel = Channel} = ensure_declaring_channel(State),
    case catch amqp_channel:call(Channel, QDecl) of
        {'EXIT', _Reason} ->
            {error, not_found, State1#session{ declaring_channel = undefined }};
        #'queue.declare_ok'{} ->
            {ok, QueueName, State1}
    end.

check_exchange(ExchangeName, State) when is_list(ExchangeName) ->
    check_exchange(list_to_binary(ExchangeName), State);
check_exchange(ExchangeName, State) when is_binary(ExchangeName) ->
    XDecl = #'exchange.declare'{ exchange = ExchangeName, passive = true },
    State1 = #session{
      declaring_channel = Channel } = ensure_declaring_channel(State),
    case catch amqp_channel:call(Channel, XDecl) of
        {'EXIT', _Reason} ->
            {error, not_found, State1#session{declaring_channel = undefined}};
        #'exchange.declare_ok'{} ->
            {ok, ExchangeName, State1}
    end.

%% TODO Lifetimes: we approximate these with exclusive + auto_delete
%% for the minute.
create_queue(_Lifetime, State) ->
    State1 = #session{ declaring_channel = Ch } = ensure_declaring_channel(State),
    #'queue.declare_ok'{queue = QueueName} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true, auto_delete = true}),
    {ok, QueueName, State1}.

queue_address(QueueName) when is_binary(QueueName) ->
    <<"/queue/", QueueName/binary>>.

next_transfer_number(TransferNumber) ->
    %% TODO this should be a serial number
    TransferNumber + 1.

%% FIXME
transfer_size(_Content, _Unit) ->
    1.

handle_to_ctag({uint, H}) ->
    <<H:32/integer>>.

ctag_to_handle(<<H:32/integer>>) ->
    {uint, H}.
