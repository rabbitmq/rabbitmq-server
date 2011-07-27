-module(rabbit_amqp1_0_session).

-behaviour(gen_server2).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-export([start_link/7, process_frame/2]).

-ifdef(debug).
-export([parse_destination/1]).
-endif.

-record(session, {channel_num, %% we just use the incoming (AMQP 1.0) channel number
                  backing_connection, backing_channel,
                  declaring_channel, %% a sacrificial client channel for declaring things
                  reader_pid, writer_pid,
                  next_transfer_number = 0, % next outgoing id
                  max_outgoing_id, % based on the remote incoming window size
                  next_incoming_id, % just to keep a check
                  next_publish_id, %% the 0-9-1-side counter for confirms
                  %% we make incoming and outgoing session buffers the
                  %% same size
                  window_size,
                  ack_counter = 0,
                  incoming_unsettled_map,
                  outgoing_unsettled_map }).
-record(outgoing_link, {queue,
                        delivery_count = 0,
                        no_ack,
                        default_outcome}).

%% Just make these constant for the time being.
-define(INCOMING_CREDIT, 65536).
-define(INIT_TXFR_COUNT, 0).

-record(incoming_link, {name, exchange, routing_key,
                        delivery_count = 0,
                        credit_used = ?INCOMING_CREDIT div 2
                       }).

-record(outgoing_transfer, {delivery_tag, expected_outcome}).

-define(SEND_ROLE, false).
-define(RECV_ROLE, true).

-define(EXCHANGE_SUB_LIFETIME, "delete-on-close").

-define(DEFAULT_OUTCOME, #'v1_0.released'{}).

-define(OUTCOMES, [?V_1_0_SYMBOL_ACCEPTED,
                   ?V_1_0_SYMBOL_REJECTED,
                   ?V_1_0_SYMBOL_RELEASED]).

%% TODO test where the sweetspot for gb_trees is
-define(MAX_SESSION_BUFFER_SIZE, 4096).
-define(DEFAULT_MAX_HANDLE, 16#ffffffff).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_amqp1_0.hrl").

%% TODO account for all these things
start_link(Channel, ReaderPid, WriterPid, _Username, _VHost,
           _Collector, _StartLimiterFun) ->
    gen_server2:start_link(
      ?MODULE, [Channel, ReaderPid, WriterPid], []).

process_frame(Pid, Frame) ->
    gen_server2:cast(Pid, {frame, Frame}).

%% ---------

init([Channel, ReaderPid, WriterPid]) ->
    %% TODO pass through authentication information
    {ok, Conn} = amqp_connection:start(#amqp_params_direct{}),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    {ok, #session{ channel_num            = Channel,
                   backing_connection     = Conn,
                   backing_channel        = Ch,
                   reader_pid             = ReaderPid,
                   writer_pid             = WriterPid,
                   next_publish_id        = 0,
                   ack_counter            = 0,
                   incoming_unsettled_map = gb_trees:empty(),
                   outgoing_unsettled_map = gb_trees:empty()}}.

terminate(_Reason, _State = #session{ backing_connection = Conn,
                                     declaring_channel  = DeclCh,
                                     backing_channel    = Ch}) ->
    ?DEBUG("Shutting down session ~p", [_State]),
    case DeclCh of
        undefined -> ok;
        Channel   -> amqp_channel:close(Channel)
    end,
    amqp_channel:close(Ch),
    %% TODO: closing the connection here leads to errors in the logs
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
                              next_transfer_number = TransferNum }) ->
    %% FIXME, don't ignore ack required, keep track of credit, um .. etc.
    Handle = ctag_to_handle(ConsumerTag),
    case get({out, Handle}) of
        Link = #outgoing_link{} ->
            {NewLink, NewState} =
                transfer(WriterPid, Handle, Link, State, Msg, DeliveryTag),
            put({out, Handle}, NewLink),
            {noreply, NewState#session{
                        next_transfer_number = next_transfer_number(TransferNum)}};
        undefined ->
            %% FIXME handle missing link -- why does the queue think it's there?
            rabbit_log:warning("Delivery to non-existent consumer ~p", [ConsumerTag]),
            {noreply, State}
    end;

%% A message from the queue saying that the credit is either exhausted
%% or there are no more messages
handle_info(#'basic.credit_state'{consumer_tag = CTag,
                                  credit       = LinkCredit,
                                  count        = Count,
                                  available    = Available0,
                                  drain        = Drain},
            State = #session{writer_pid = WriterPid}) ->
    Available = case Available0 of
                    -1  -> undefined;
                    Num -> {uint, Num}
                end,
    Handle = ctag_to_handle(CTag),
    
    %% The transfer count that is given by the queue should be at
    %% least that we have locally, since we will either have received
    %% all the deliveries and transfered them, or the queue will have
    %% advanced it due to drain. So we adopt the queue's idea of the
    %% count.

    %% FIXME account for it not being there any more
    Out = get({out, Handle}),
    
    F = #'v1_0.flow'{ handle      = Handle,
                      delivery_count = {uint, Count},
                      link_credit = {uint, LinkCredit},
                      available   = Available,
                      drain       = Drain },
    put({out, Handle}, Out#outgoing_link{ delivery_count = Count }),
    rabbit_amqp1_0_writer:send_command(WriterPid, F),
    {noreply, State};

%% An acknowledgement from the queue.  To keep the incoming window
%% moving, we make sure to update them with the session counters every
%% once in a while.  Assuming that the buffer is an appropriate size,
%% about once every window_size / 2 is a good heuristic.
handle_info(#'basic.ack'{delivery_tag = DTag, multiple = Multiple},
            State = #session{incoming_unsettled_map = Unsettled,
                             window_size = Window,
                             ack_counter = AckCounter,
                             writer_pid = WriterPid}) ->
    {TransferIds, Unsettled1} =
        case Multiple of
            true  -> acknowledgement_range(DTag, Unsettled);
            false -> case gb_trees:lookup(DTag, Unsettled) of
                         {value, Id} ->
                             {[Id], gb_trees:delete(DTag, Unsettled)};
                         none ->
                             {[], Unsettled}
                     end
        end,
    case TransferIds of
        [] -> ok;
        _  -> D = acknowledgement(TransferIds,
                                  #'v1_0.disposition'{role = ?RECV_ROLE}),
              rabbit_amqp1_0_writer:send_command(WriterPid, D)
    end,
    HalfWindow = Window div 2,
    AckCounter1 = case (AckCounter + length(TransferIds)) of
                      Over when Over >= HalfWindow ->
                          F = flow_session_fields(State),
                          rabbit_amqp1_0_writer:send_command(WriterPid, F),
                          Over - HalfWindow;
                      Counter ->
                          Counter
                  end,
    {noreply, State#session{
                ack_counter = AckCounter1,
                incoming_unsettled_map = Unsettled1 }};

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
        {reply, Replies, NewState} when is_list(Replies) ->
            lists:foreach(fun (Reply) ->
                                  rabbit_amqp1_0_writer:send_command(Sock, Reply)
                          end, Replies),
            noreply(NewState);
        {reply, Reply, NewState} ->
            rabbit_amqp1_0_writer:send_command(Sock, Reply),
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

%% Session window:
%%
%% Each session has two buffers, one to record the unsettled state of
%% incoming messages, one to record the unsettled state of outgoing
%% messages.  In general we want to bound these buffers; but if we
%% bound them, and don't tell the other side, we may end up
%% deadlocking the other party.
%%
%% Hence the flow frame contains a session window, expressed as the
%% next-id and the window size for each of the buffers. The frame
%% refers to the buffers of the sender of the frame, of course.
%%
%% The numbers work this way: for the outgoing buffer, the next-id is
%% the next transfer id the session will send, and it will stop
%% sending at next-id + window.  For the incoming buffer, the next-id
%% is the next transfer id expected, and it will not accept messages
%% beyond next-id + window (in fact it will probably close the
%% session, since sending outside the window is a transgression of the
%% protocol).
%%
%% Usually we will want to base our incoming window size on the other
%% party's outgoing window size (given in begin{}), since we will
%% never need more state than they are keeping (they'll stop sending
%% before that happens), subject to a maximum.  Similarly the outgoing
%% window, on the basis that the other party is likely to make its
%% buffers the same size (or that's our best guess).
%%
%% Note that we will occasionally overestimate these buffers, because
%% the far side may be using a circular buffer, in which case they
%% care about the distance from the low water mark (i.e., the least
%% transfer for which they have unsettled state) rather than the
%% number of entries.
%%
%% We use ordered sets for our buffers, which means we care about the
%% total number of entries, rather than the smallest entry. Thus, our
%% window will always be, by definition, BOUND - TOTAL.

handle_control(#'v1_0.begin'{next_outgoing_id = {uint, RemoteNextIn},
                             incoming_window = RemoteInWindow,
                             outgoing_window = RemoteOutWindow,
                             handle_max = HandleMax0},
               State = #session{
                 next_transfer_number = LocalNextOut,
                 backing_channel = AmqpChannel,
                 channel_num = Channel }) ->
    Window =
        case RemoteInWindow of
            {uint, Size} -> Size;
            undefined    -> ?MAX_SESSION_BUFFER_SIZE
        end,
    HandleMax = case HandleMax0 of
                    {uint, Max} -> Max;
                    _ -> ?DEFAULT_MAX_HANDLE
                end,
    %% TODO does it make sense to have two different sizes
    SessionBufferSize = erlang:min(Window, ?MAX_SESSION_BUFFER_SIZE),
    %% Attempt to limit the number of "at risk" messages we can have.
    amqp_channel:cast(AmqpChannel,
                      #'basic.qos'{prefetch_count = SessionBufferSize}),
    {reply, #'v1_0.begin'{
       remote_channel = {ushort, Channel},
       handle_max = {uint, HandleMax},
       next_outgoing_id = {uint, LocalNextOut},
       incoming_window = {uint, SessionBufferSize},
       outgoing_window = {uint, SessionBufferSize}},
     State#session{
       next_incoming_id = RemoteNextIn,
       max_outgoing_id = rabbit_misc:serial_add(RemoteNextIn, Window),
       window_size = SessionBufferSize}};

handle_control(#'v1_0.attach'{name = Name,
                              handle = Handle,
                              source = Source,
                              target = Target,
                              initial_delivery_count = {uint, InitTransfer},
                              role = ?SEND_ROLE}, %% client is sender
               State = #session{ backing_channel = Ch,
                                 next_publish_id = NextPublishId }) ->
    %% TODO associate link name with target
    case ensure_target(Target, #incoming_link{ name = Name }, State) of
        {ok, ServerTarget,
         IncomingLink = #incoming_link{ delivery_count = InitTransfer },
         State1} ->
            {_, Outcomes} = outcomes(Source),
            State2 =
                case Outcomes of
                    [?V_1_0_SYMBOL_ACCEPTED] ->
                        State1;
                    _ ->
                        amqp_channel:register_confirm_handler(Ch, self()),
                        amqp_channel:call(Ch, #'confirm.select'{}),
                        State1#session{ next_publish_id =
                                            erlang:max(1, NextPublishId) }
            end,
            put({in, Handle}, IncomingLink),
            Flow = flow_session_fields(State2),
            Flow1 = Flow#'v1_0.flow'{ handle = Handle,
                                      link_credit = {uint, ?INCOMING_CREDIT},
                                      drain = false,
                                      echo = false },
            Attach = #'v1_0.attach'{
              name = Name,
              handle = Handle,
              source = Source,
              target = ServerTarget,
              initial_delivery_count = undefined, % must be, I am the recvr
              role = ?RECV_ROLE}, %% server is receiver
            {reply, [Attach, Flow1], State2};
        {error, Reason, State1} ->
            rabbit_log:warning("AMQP 1.0 attach rejected ~p~n", [Reason]),
            %% TODO proper link estalishment protocol here?
            protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                               "Attach rejected: ~p", [Reason]),
            {noreply, State1}
    end;

handle_control(#'v1_0.attach'{source = Source,
                              initial_delivery_count = undefined,
                              role = ?RECV_ROLE} = Attach, %% client is receiver
               State) ->
    %% TODO ensure_destination
    {DefaultOutcome, Outcomes} = outcomes(Source),
    attach_outgoing(DefaultOutcome, Outcomes, Attach, State);

handle_control([Txfr = #'v1_0.transfer'{handle = Handle,
                                        settled = Settled,
                                        delivery_id = {uint, TxfrId}} |
                AnnotatedMessage],
               State = #session{backing_channel = Ch,
                                next_publish_id = NextPublishId,
                                incoming_unsettled_map = Unsettled}) ->
    case get({in, Handle}) of
        #incoming_link{ exchange = X, routing_key = RK,
                        delivery_count = Count,
                        credit_used = CreditUsed } = Link ->
            NewCount = rabbit_misc:serial_add(Count, 1),
            Msg = rabbit_amqp1_0_message:assemble(AnnotatedMessage),
            NextPublishId1 = case NextPublishId of
                                 0 -> 0;
                                 _ -> NextPublishId + 1 % serial?
                             end,
            amqp_channel:call(Ch, #'basic.publish' { exchange    = X,
                                                     routing_key = RK }, Msg),
            {SendFlow, CreditUsed1} = case CreditUsed - 1 of
                                          C when C =< 0 ->
                                              {true,  ?INCOMING_CREDIT div 2};
                                          D ->
                                              {false, D}
                                      end,
            NewLink = Link#incoming_link{ delivery_count = NewCount,
                                          credit_used = CreditUsed1 },
            put({in, Handle}, NewLink),
            State1 = State#session{
                       next_publish_id = NextPublishId1,
                       next_incoming_id = next_transfer_number(TxfrId) },
            State2 = case Settled of
                         true  -> State1;
                         %% Be lenient -- this is a boolean and really ought
                         %% to have a value, but the spec doesn't currently
                         %% require it.
                         Symbol when
                         Symbol =:= false orelse
                         Symbol =:= undefined ->
                             Unsettled1 = gb_trees:insert(
                                            NextPublishId,
                                            TxfrId,
                                            Unsettled),
                             State1#session{
                               incoming_unsettled_map = Unsettled1}
                     end,
            case SendFlow of
                true ->
                    ?DEBUG("sending flow for incoming ~p", [NewLink]),
                    incoming_flow(NewLink, Handle, State2);
                false ->
                    {noreply, State2}
            end;
        undefined ->
            protocol_error(?V_1_0_AMQP_ERROR_ILLEGAL_STATE,
                           "Unknown link handle ~p", [Handle])
    end;

%% Disposition: a single extent is settled at a time.  This may
%% involve more than one message. TODO: should we send a flow after
%% this, to indicate the state of the session window?
handle_control(#'v1_0.disposition'{ role = ?RECV_ROLE } = Disp, State) ->
    case settle(Disp, State) of
        {none, NewState} ->
            {noreply, NewState};
        {ReplyDisp, NewState} ->
            {reply, ReplyDisp, NewState}
    end;

handle_control(#'v1_0.detach'{ handle = Handle }, State) ->
    %% TODO keep the state around depending on the lifetime
    erase({in, Handle}),
    {reply, #'v1_0.detach'{ handle = Handle }, State};

handle_control(#'v1_0.end'{}, _State = #session{ writer_pid = Sock }) ->
    ok = rabbit_amqp1_0_writer:send_command(Sock, #'v1_0.end'{}),
    stop;

%% Flow control.  These frames come with two pieces of information:
%% the session window, and optionally, credit for a particular link.
%% We'll deal with each of them separately.
%%
%% See above regarding the session window. We should already know the
%% next outgoing transfer sequence number, because it's one more than
%% the last transfer we saw; and, we don't need to know the next
%% incoming transfer sequence number (although we might use it to
%% detect congestion -- e.g., if it's lagging far behind our outgoing
%% sequence number). We probably care about the outgoing window, since
%% we want to keep it open by sending back settlements, but there's
%% not much we can do to hurry things along.
%%
%% We do care about the incoming window, because we must not send
%% beyond it. This may cause us problems, even in normal operation,
%% since we want our unsettled transfers to be exactly those that are
%% held as unacked by the backing channel; however, the far side may
%% close the window while we still have messages pending
%% transfer. Note that this isn't a race so far as AMQP 1.0 is
%% concerned; it's only because AMQP 0-9-1 defines QoS in terms of the
%% total number of unacked messages, whereas 1.0 has an explicit window.
handle_control(Flow = #'v1_0.flow'{},
               State = #session{ next_incoming_id = LocalNextIn,
                                 max_outgoing_id = _LocalMaxOut,
                                 next_transfer_number = LocalNextOut }) ->
    #'v1_0.flow'{ next_incoming_id = RemoteNextIn0,
                  incoming_window = {uint, RemoteWindowIn},
                  next_outgoing_id = {uint, RemoteNextOut},
                  outgoing_window = {uint, RemoteWindowOut}} = Flow,
    %% Check the things that we know for sure
    %% TODO sequence number comparisons
    ?DEBUG("~p == ~p~n", [RemoteNextOut, LocalNextIn]),
    %% TODO the Python client sets next_outgoing_id=2 on begin, then sends a
    %% flow with next_outgoing_id=1. Not sure what that's meant to mean.
    %% RemoteNextOut = LocalNextIn,
    %% The far side may not have our begin{} with our next-transfer-id
    RemoteNextIn = case RemoteNextIn0 of
                       {uint, Id} -> Id;
                       undefined  -> LocalNextOut
                   end,
    ?DEBUG("~p =< ~p~n", [RemoteNextIn, LocalNextOut]),
    true = (RemoteNextIn =< LocalNextOut),
    %% Adjust our window
    RemoteMaxOut = RemoteNextIn + RemoteWindowIn,
    State1 = State#session{ max_outgoing_id = RemoteMaxOut },
    case Flow#'v1_0.flow'.handle of
        undefined ->
            {noreply, State1};
        Handle ->
            case get({in, Handle}) of
                undefined ->
                    case get({out, Handle}) of
                        undefined ->
                            rabbit_log:warning("Flow for unknown link handle ~p", [Flow]),
                            protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                                           "Unattached handle: ~p", [Handle]);
                        Out = #outgoing_link{} ->
                            outgoing_flow(Out, Flow, State1)
                    end;
                _In = #incoming_link{} ->
                    %% We're being told about available messages at
                    %% the sender.  Yawn.
                    %% TODO at least check transfer-count?
                    {noreply, State1}
            end
    end;

handle_control(Frame, State) ->
    %% FIXME should this bork?
    io:format("Ignoring frame: ~p~n", [Frame]),
    {noreply, State}.

%% ------

protocol_error(Condition, Msg, Args) ->
    exit(#'v1_0.error'{
        condition = Condition,
        description = {utf8, list_to_binary(
                               lists:flatten(io_lib:format(Msg, Args)))}
       }).


outcomes(Source) ->
    {DefaultOutcome, Outcomes} =
        case Source of
            #'v1_0.source' {
                      default_outcome = DO,
                      outcomes = Os
                     } ->
                DO1 = case DO of
                          undefined -> ?DEFAULT_OUTCOME;
                          _         -> DO
                      end,
                Os1 = case Os of
                          undefined -> ?OUTCOMES;
                          _         -> Os
                      end,
                {DO1, Os1};
            _ ->
                {?DEFAULT_OUTCOME, ?OUTCOMES}
        end,
    case [O || O <- Outcomes, not lists:member(O, ?OUTCOMES)] of
        []   -> {DefaultOutcome, Outcomes};
        Bad  -> protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                               "Outcomes not supported: ~p", [Bad])
    end.

attach_outgoing(DefaultOutcome, Outcomes,
                #'v1_0.attach'{name = Name,
                               handle = Handle,
                               source = Source},
               State = #session{backing_channel = Ch}) ->
    NoAck = DefaultOutcome == #'v1_0.accepted'{} andalso
        Outcomes == [?V_1_0_SYMBOL_ACCEPTED],
    DOSym = rabbit_amqp1_0_framing:symbol_for(DefaultOutcome),
    case ensure_source(Source,
                       #outgoing_link{ delivery_count = ?INIT_TXFR_COUNT,
                                       no_ack = NoAck,
                                       default_outcome = DOSym}, State) of
        {ok, Source1,
         OutgoingLink = #outgoing_link{ queue = QueueName,
                                        delivery_count = Count }, State1} ->
            CTag = handle_to_ctag(Handle),
            %% Zero the credit before we start consuming, so that we only
            %% use explicitly given credit.
            amqp_channel:cast(Ch, #'basic.credit'{consumer_tag = CTag,
                                                  credit       = 0,
                                                  count        = Count,
                                                  drain        = false}),
            case amqp_channel:subscribe(
                   Ch, #'basic.consume' { queue = QueueName,
                                          consumer_tag = CTag,
                                          no_ack = NoAck,
                                          %% TODO exclusive?
                                          exclusive = false}, self()) of
                #'basic.consume_ok'{} ->
                    %% FIXME we should avoid the race by getting the queue to send
                    %% attach back, but a.t.m. it would use the wrong codec.
                    put({out, Handle}, OutgoingLink),
                    {reply, #'v1_0.attach'{
                       name = Name,
                       handle = Handle,
                       initial_delivery_count = {uint, ?INIT_TXFR_COUNT},
                       source = Source1#'v1_0.source'{
                                  default_outcome = DefaultOutcome
                                  %% TODO this breaks the Python client, when it
                                  %% tries to send us back a matching detach message
                                  %% it gets confused between described(true, [...])
                                  %% and [...]. We think we're correct here
                                  %% outcomes = Outcomes
                                 },
                       role = ?SEND_ROLE}, State1};
                Fail ->
                    protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR, "Consume failed: ~p", Fail)
            end;
        {error, _Reason, State1} ->
            {reply, #'v1_0.attach'{source = undefined}, State1}
    end.

flow_session_fields(#session{ next_transfer_number = NextOut,
                              next_incoming_id = NextIn,
                              window_size = Window,
                              outgoing_unsettled_map = UnsettledOut,
                              incoming_unsettled_map = UnsettledIn }) ->
    #'v1_0.flow'{ next_outgoing_id = {uint, NextOut},
                  outgoing_window = {uint, Window - gb_trees:size(UnsettledOut)},
                  next_incoming_id = {uint, NextIn},
                  incoming_window = {uint, Window - gb_trees:size(UnsettledIn)}}.

outgoing_flow(#outgoing_link{ delivery_count = LocalCount },
              #'v1_0.flow'{
                handle = Handle,
                delivery_count = Count0,
                link_credit = {uint, RemoteCredit},
                drain = Drain},
              State = #session{backing_channel = Ch}) ->
    RemoteCount = case Count0 of
                      undefined -> LocalCount;
                      {uint, Count} -> Count
                  end,
    %% Rebase to our transfer-count
    Credit = RemoteCount + RemoteCredit - LocalCount,
    CTag = handle_to_ctag(Handle),
    #'basic.credit_ok'{available = Available} =
        %% FIXME calculate the credit based on the transfer count
        amqp_channel:call(Ch,
                          #'basic.credit'{consumer_tag = CTag,
                                          credit       = Credit,
                                          count        = LocalCount,
                                          drain        = Drain}),
    case Available of
        -1 ->
            {noreply, State};
        %% We don't know - probably because this flow relates
        %% to a handle that does not yet exist
        %% TODO is this an error?
        _  ->
            Flow1 = flow_session_fields(State),
            {reply, Flow1#'v1_0.flow'{
                      handle = Handle,
                      delivery_count = {uint, LocalCount},
                      link_credit = {uint, Credit},
                      available = {uint, Available},
                      drain = Drain}, State}
    end.

incoming_flow(#incoming_link{ delivery_count = Count }, Handle, State) ->
    Flow = flow_session_fields(State),
    {reply, Flow#'v1_0.flow'{
              handle = Handle,
              delivery_count = {uint, Count},
              link_credit = {uint, ?INCOMING_CREDIT}},
     State}.

transfer(WriterPid, LinkHandle,
         Link = #outgoing_link{ delivery_count = Count,
                                no_ack = NoAck,
                                default_outcome = DefaultOutcome },
         Session = #session{ next_transfer_number = TransferNumber,
                             max_outgoing_id = LocalMaxOut,
                             window_size = WindowSize,
                             backing_channel = AmqpChannel,
                             outgoing_unsettled_map = Unsettled },
         Msg = #amqp_msg{payload = Content},
         DeliveryTag) ->
    %% FIXME
    %% If either the outgoing session window, or the remote incoming
    %% session window, is closed, we can't send this. This probably
    %% happened because the far side is basing its window on the low
    %% water mark, whereas we can only tell the queue to have at most
    %% "prefetch_count" messages in flight (i.e., a total). For the
    %% minute we will have to just break things.
    NumUnsettled = gb_trees:size(Unsettled),
    if (LocalMaxOut > TransferNumber) andalso
       (WindowSize >= NumUnsettled) ->
            NewLink = Link#outgoing_link{
                        delivery_count = Count + 1
                       },
            T = #'v1_0.transfer'{handle = LinkHandle,
                                 delivery_tag = {binary, <<DeliveryTag:64>>},
                                 delivery_id = {uint, TransferNumber},
                                 %% The only one in AMQP 1-0
                                 message_format = {uint, 0},
                                 settled = NoAck,
                                 state = ?DEFAULT_OUTCOME,
                                 resume = false,
                                 more = false,
                                 aborted = false,
                                 %% TODO: actually batchable would be
                                 %% fine, but in any case it's only a
                                 %% hint
                                 batchable = false},
            Unsettled1 = case NoAck of
                             true -> Unsettled;
                             false -> gb_trees:insert(TransferNumber,
                                                      #outgoing_transfer{
                                                        delivery_tag = DeliveryTag,
                                                        expected_outcome = DefaultOutcome },
                                                      Unsettled)
                         end,
            rabbit_amqp1_0_writer:send_command(
              WriterPid,
              [T | rabbit_amqp1_0_message:annotated_message(Msg)]),
            {NewLink, Session#session { outgoing_unsettled_map = Unsettled1 }};
       %% FIXME We can't knowingly exceed our credit.  On the other
       %% hand, we've been handed a message to deliver. This has
       %% probably happened because the receiver has suddenly reduced
       %% the credit or session window.
       NoAck ->
            {Link, Session};
       true ->
            amqp_channel:call(AmqpChannel, #'basic.reject'{requeue = true,
                                                           delivery_tag = DeliveryTag}),
            {Link, Session}
    end.

%% We've been told that the fate of a transfer has been determined.
%% Generally if the other side has not settled it, we will do so.  If
%% the other side /has/ settled it, we don't need to reply -- it's
%% already forgotten its state for the transfer anyway.
settle(Disp = #'v1_0.disposition'{ first = First0,
                                   last = Last0,
                                   settled = Settled,
                                   state = Outcome },
       State = #session{backing_channel = Ch,
                        outgoing_unsettled_map = Unsettled}) ->
    {uint, First} = First0,
    %% Last may be omitted, in which case it's the same as first
    Last = case Last0 of
               {uint, L} -> L;
               undefined -> First
           end,

    %% The other party may be talking about something we've already
    %% forgotten; this isn't a crime, we can just ignore it.

    case gb_trees:is_empty(Unsettled) of
        true ->
            {none, State};
        false ->
            {LWM, _} = gb_trees:smallest(Unsettled),
            {HWM, _} = gb_trees:largest(Unsettled),
            if Last < LWM ->
                    {none, State};
               First > HWM ->
                    State; %% FIXME this should probably be an error, rather than ignored.
               true ->
                    Unsettled1 =
                        lists:foldl(
                          fun (Transfer, Map) ->
                                  case gb_trees:lookup(Transfer, Map) of
                                      none ->
                                          Map;
                                      {value, Entry} ->
                                          ?DEBUG("Settling ~p with ~p~n", [Transfer, Outcome]),
                                          #outgoing_transfer{ delivery_tag = DeliveryTag } = Entry,
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
                                          gb_trees:delete(Transfer, Map)
                                  end
                          end,
                          Unsettled, lists:seq(erlang:max(LWM, First),
                                               erlang:min(HWM, Last))),
                    {case Settled of
                         true  -> none;
                         false -> Disp#'v1_0.disposition'{ settled = true,
                                                           role = ?SEND_ROLE }
                     end,
                     State#session{outgoing_unsettled_map = Unsettled1}}
            end
    end.

acknowledgement_range(DTag, Unsettled) ->
    acknowledgement_range(DTag, Unsettled, []).

acknowledgement_range(DTag, Unsettled, Acc) ->
    case gb_trees:is_empty(Unsettled) of
        true ->
            {lists:reverse(Acc), Unsettled};
        false ->
            {DTag1, TransferId} = gb_trees:smallest(Unsettled),
            case DTag1 =< DTag of
                true ->
                    {_K, _V, Unsettled1} = gb_trees:take_smallest(Unsettled),
                    acknowledgement_range(DTag, Unsettled1,
                                          [TransferId|Acc]);
                false ->
                    {lists:reverse(Acc), Unsettled}
            end
    end.

acknowledgement(TransferIds, Disposition) ->
    Disposition#'v1_0.disposition'{ first = {uint, hd(TransferIds)},
                                    last = {uint, lists:last(TransferIds)},
                                    settled = true,
                                    state = #'v1_0.accepted'{} }.

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
              Link = #incoming_link{},
              State) ->
    case Dynamic of
        undefined ->
            case Address of
                {Enc, Destination}
                when Enc =:= utf8 orelse Enc =:= utf16 ->
                    case parse_destination(Destination, Enc) of
                        ["queue", Name] ->
                            case check_queue(Name, State) of
                                {ok, QueueName, State1} ->
                                    {ok, Target,
                                     Link#incoming_link{exchange = <<"">>,
                                                        routing_key = QueueName},
                                     State1};
                                {error, Reason, State1} ->
                                    {error, Reason, State1}
                            end;
                        ["queue"] ->
                            %% Rely on the Subject being set
                            {ok, Target, Link#incoming_link{exchange = <<"">>}, State};
                        ["exchange", Name] ->
                            case check_exchange(Name, State) of
                                {ok, ExchangeName, State1} ->
                                    {ok, Target,
                                     Link#incoming_link{exchange = ExchangeName},
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
                     Link#incoming_link{exchange = <<"">>,
                                        routing_key = QueueName},
                     State1};
                _Else ->
                    {error, {both_dynamic_and_address_supplied,
                             Dynamic, Address},
                     State}
            end
    end.

ensure_source(Source = #'v1_0.source'{ address = Address,
                                       dynamic = Dynamic },
              Link = #outgoing_link{}, State) ->
    case Dynamic of
        undefined ->
            %% TODO ugh. This will go away after the planned codec rewrite.
            Destination = case Address of
                              {_Enc, D} -> binary_to_list(D);
                              D         -> D
                          end,
            case parse_destination(Destination) of
                ["queue", Name] ->
                    case check_queue(Name, State) of
                        {ok, QueueName, State1} ->
                            {ok, Source,
                             Link#outgoing_link{queue = QueueName}, State1};
                        {error, Reason, State1} ->
                            {error, Reason, State1}
                    end;
                ["exchange", Name, RK] ->
                    case check_exchange(Name, State) of
                        {ok, ExchangeName, State1} ->
                            RoutingKey = list_to_binary(RK),
                            {ok, QueueName, State2} =
                                create_bound_queue(ExchangeName, RoutingKey,
                                                   State1),
                            {ok, Source, Link#outgoing_link{queue = QueueName},
                             State2};
                        {error, Reason, State1} ->
                            {error, Reason, State1}
                    end;
                _Otherwise ->
                    {error, {unknown_address, Destination}, State}
            end;
        {symbol, Lifetime} ->
            case Address of
                undefined ->
                    {ok, QueueName, State1} = create_queue(Lifetime, State),
                    {ok,
                     Source#'v1_0.source'{address = {utf8, queue_address(QueueName)}},
                     #outgoing_link{queue = QueueName},
                     State1};
                _Else ->
                    {error, {both_dynamic_and_address_supplied,
                             Dynamic, Address},
                     State}
            end
    end.

parse_destination(Destination, Enc) when is_binary(Destination) ->
    parse_destination(unicode:characters_to_list(Destination, Enc)).

parse_destination(Destination) when is_list(Destination) ->
    case re:split(Destination, "/", [{return, list}]) of
        ["", Type | Tail] when
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

%% TODO Lifetimes: we approximate these with auto_delete, but not
%% exclusive, since exclusive queues and the direct client are broken
%% at the minute.
create_queue(_Lifetime, State) ->
    State1 = #session{ declaring_channel = Ch } = ensure_declaring_channel(State),
    #'queue.declare_ok'{queue = QueueName} =
        amqp_channel:call(Ch, #'queue.declare'{auto_delete = true}),
    {ok, QueueName, State1}.

create_bound_queue(ExchangeName, RoutingKey, State) ->
    {ok, QueueName, State1 = #session{ declaring_channel = Ch}} =
        create_queue(?EXCHANGE_SUB_LIFETIME, State),
    %% Don't both ensuring the channel, the previous should have done it
    #'queue.bind_ok'{} =
        amqp_channel:call(Ch, #'queue.bind'{ exchange = ExchangeName,
                                             queue = QueueName,
                                             routing_key = RoutingKey }),
    {ok, QueueName, State1}.

queue_address(QueueName) when is_binary(QueueName) ->
    <<"/queue/", QueueName/binary>>.

next_transfer_number(TransferNumber) ->
    %% TODO this should be a serial number
    rabbit_misc:serial_add(TransferNumber, 1).

handle_to_ctag({uint, H}) ->
    <<"ctag-", H:32/integer>>.

ctag_to_handle(<<"ctag-", H:32/integer>>) ->
    {uint, H}.
