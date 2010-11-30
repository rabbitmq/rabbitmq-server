-module(rabbit_amqp1_0_session).

-behaviour(gen_server2).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-export([start_link/7, process_frame/2]).

-record(session, {channel_num, backing_connection, backing_channel,
                  reader_pid, writer_pid, transfer_number = 0,
                  outgoing_lwm = 0, outgoing_session_credit = 10,
                  xfer_num_to_tag }).
-record(outgoing_link, {credit,
                        transfer_count = 0,
                        transfer_unit = 0,
                        no_ack}).

-record(incoming_link, {name, exchange, routing_key}).

-define(ACCEPTED, #'v1_0.accepted'{}).
-define(REJECTED, #'v1_0.rejected'{}).
-define(RELEASED, #'v1_0.released'{}).

-define(DEFAULT_OUTCOME, ?RELEASED).
%%-define(DEFAULT_OUTCOME, ?ACCEPTED).
-define(OUTCOMES, [?ACCEPTED, ?REJECTED, ?RELEASED]).

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
start_link(Channel, ReaderPid, WriterPid, Username, VHost,
           Collector, StartLimiterFun) ->
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

terminate(Reason, State = #session{ backing_connection = Conn,
                                    backing_channel    = Ch}) ->
    amqp_channel:close(Ch),
    amqp_connection:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(Msg, From, State) ->
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
handle_info({'DOWN', _MRef, process, QPid, _Reason}, State) ->
    {noreply, State}. % FIXME rabbit_channel uses queue_blocked?

handle_cast({frame, Frame},
            State = #session{ writer_pid = Sock,
                              channel_num = Channel}) ->
    case handle_control(Frame, State) of
        {reply, Reply, NewState} ->
            ok = rabbit_amqp1_0_writer:send_command(Sock, Reply),
            noreply(NewState);
        {noreply, NewState} ->
            noreply(NewState);
        stop ->
            {stop, normal, State}
    %% TODO rabbit_channel has some extra error handling here
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
                              local = Linkage,
                              flow_state = Flow,
                              role = false}, %% client is sender
               State = #session{ outgoing_lwm = LWM }) ->
    %% TODO associate link name with target
    #'v1_0.linkage'{ source = Source, target = Target } = Linkage,
    #'v1_0.flow_state'{ transfer_count = TransferCount } = Flow,
    {utf8, Destination} = Target#'v1_0.target'.address,
    case ensure_destination(Destination, #incoming_link{ name = Name }, State) of
        {ok, IncomingLink, State1} ->
            put({incoming, Handle}, IncomingLink),
            {reply,
             #'v1_0.attach'{
               name = Name,
               handle = Handle,
               remote = Linkage,
               local = #'v1_0.linkage'{
                 %% TODO include whatever the source was
                 source = Source,
                 target = #'v1_0.target'{ address = {utf8, Destination} }},
               flow_state = Flow#'v1_0.flow_state'{
                              link_credit = {uint, 50},
                              unsettled_lwm = {uint, LWM}},
               role = true}, State};
        {error, State1} ->
            {reply,
             #'v1_0.attach'{
               name = Name,
               handle = Handle,
               remote = Linkage,
               local = undefined}, State1}
    end;

%% TODO we don't really implement flow control. Reject connections
%% that try to use it - except ATM the Python test case asks to use it
handle_control(#'v1_0.attach'{name = Name,
                              handle = Handle,
                              local = Linkage,
                              flow_state = Flow = #'v1_0.flow_state'{
                                             link_credit = {uint, Credit},
                                             drain = Drain
                                            },
                              transfer_unit = Unit,
                              role = true} = Attach, %% client is receiver
               State = #session{backing_channel = Ch}) ->
    %% TODO ensure_destination
    #'v1_0.linkage'{ source = Source = #'v1_0.source' {
                                address = {utf8, Q},
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
    case lists:any(fun(O) -> not lists:member(O, ?OUTCOMES) end, Outcomes) of
        true  -> close_error();
        false -> attach_outgoing(DefaultOutcome, Outcomes, Attach, State)
    end;

handle_control(#'v1_0.transfer'{handle = Handle,
                                delivery_tag = Tag,
                                transfer_id = TransferId,
                                fragments = Fragments},
                          State = #session{backing_channel = Ch}) ->
    case get({incoming, Handle}) of
        #incoming_link{ exchange = X, routing_key = RK } ->
            %% TODO what's the equivalent of the routing key?
            Msg = rabbit_amqp1_0_fragmentation:assemble(Fragments),
            amqp_channel:call(Ch, #'basic.publish' { exchange    = X,
                                                     routing_key = RK }, Msg);
        undefined ->
            %% FIXME What am I supposed to do here
            no_such_handle
    end,
    {noreply, State};

handle_control(#'v1_0.disposition'{ batchable = Batchable,
                                    extents = Extents,
                                    role = true} = Disp, %% Client is receiver
               State) ->
    {SettledExtents, NewState} =
        lists:foldl(fun(Extent, {SettledExtents1, State1}) ->
                            {SettledExtent, State2} =
                                settle(Extent, Batchable, State1),
                            {[SettledExtent | SettledExtents1], State2}
                    end, {[], State}, Extents),
    {reply, Disp#'v1_0.disposition'{ extents = SettledExtents }, NewState};

handle_control(#'v1_0.detach'{ handle = Handle },
               State = #session{ writer_pid = Sock,
                                 channel_num = Channel }) ->
    erase({incoming, Handle}),
    {reply, #'v1_0.detach'{ handle = Handle }, State};

handle_control(#'v1_0.end'{}, #session{ writer_pid = Sock }) ->
    ok = rabbit_amqp1_0_writer:send_command(Sock, #'v1_0.end'{}),
    stop;

handle_control(Frame, State) ->
    io:format("Ignoring frame: ~p~n", [Frame]),
    {noreply, State}.

%% ------

close_error() ->
    %% TODO handle errors
    stop.

attach_outgoing(DefaultOutcome, Outcomes,
                #'v1_0.attach'{name = Name,
                               handle = Handle,
                               local = Linkage,
                               flow_state = Flow = #'v1_0.flow_state'{
                                              link_credit = {uint, Credit}
                                             },
                               transfer_unit = Unit},
               State = #session{backing_channel = Ch}) ->
    NoAck = DefaultOutcome == ?ACCEPTED andalso Outcomes == [?ACCEPTED],
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
                 source = #'v1_0.source'{address = {utf8, Q},
                                         default_outcome = DefaultOutcome,
                                         outcomes = Outcomes
                                        }
                },
               flow_state = Flow#'v1_0.flow_state'{
                              available = {uint, Available}
                             },
               role = false
              }, State};
        _ ->
            close_error()
    end.

transfer(WriterPid, LinkHandle,
         Link = #outgoing_link{ credit = Credit,
                                transfer_unit = Unit,
                                transfer_count = Count,
                                no_ack = NoAck},
         Session = #session{ transfer_number = TransferNumber,
                             xfer_num_to_tag = Dict },
         Msg = #amqp_msg{payload = Content},
         DeliveryTag) ->
    TransferSize = transfer_size(Content, Unit),
    NewLink = Link#outgoing_link{
                credit = Credit - TransferSize,
                transfer_count = Count + TransferSize },
    T = #'v1_0.transfer'{handle = LinkHandle,
                         flow_state = flow_state(Link, Session),
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
                             rabbit_amqp1_0_fragmentation:fragments(Msg)},
    rabbit_amqp1_0_writer:send_command(WriterPid, T),
    {NewLink, Session#session { xfer_num_to_tag = dict:store(TransferNumber,
                                                             DeliveryTag,
                                                             Dict) }}.

settle(#'v1_0.extent'{
          first = {uint, First},
          last = {uint, Last}, %% TODO handle this
          handle = Handle, %% TODO DUBIOUS what on earth is this for?
          settled = Settled,
          state = #'v1_0.transfer_state'{ outcome = Outcome }
         } = Extent,
       Batchable, %% TODO is this documented anywhere? Handle it.
       State = #session{backing_channel = Ch,
                        xfer_num_to_tag = Dict}) ->
    case Settled of
        true ->
            %% Do nothing, the receiver is settled.
            ok;

        false ->
            DeliveryTag = dict:fetch(First, Dict),
            case Outcome of
                ?ACCEPTED ->
                    amqp_channel:call(Ch, #'basic.ack' {
                                        delivery_tag = DeliveryTag,
                                        multiple     = false });
                ?REJECTED ->
                    amqp_channel:call(Ch, #'basic.reject' {
                                        delivery_tag = DeliveryTag,
                                        requeue      = true });
                ?RELEASED ->
                    amqp_channel:call(Ch, #'basic.reject' {
                                        delivery_tag = DeliveryTag,
                                        requeue      = false })
            end
    end,
    {Extent#'v1_0.extent'{ settled = true },
     State#session{xfer_num_to_tag = dict:erase(First, Dict)}}.

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

ensure_destination(Destination, Link = #incoming_link{}, State) ->
    %% TODO Break the destination down into elements,
    %% check that exchanges exist,
    %% possibly create a subscription queue, etc.
    {ok,
     Link#incoming_link{exchange = Destination, routing_key = <<"">>},
     State}.

next_transfer_number(TransferNumber) ->
    %% TODO this should be a serial number
    TransferNumber + 1.

%% FIXME
transfer_size(Content, Unit) ->
    1.

handle_to_ctag({uint, H}) ->
    <<H:32/integer>>.

ctag_to_handle(<<H:32/integer>>) ->
    {uint, H}.
