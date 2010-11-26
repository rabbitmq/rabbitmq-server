-module(rabbit_amqp1_0_session).

-behaviour(gen_server2).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-export([start_link/7, process_frame/2]).

-record(session, {channel_num, backing_connection, backing_channel,
                  reader_pid, writer_pid, transfer_number = 0,
                  outgoing_lwm = 0, outgoing_session_credit = 10 }).
-record(outgoing_link, {credit = 0,
                        transfer_count = 0,
                        transfer_unit = 0}).

-record(incoming_link, {name, exchange, routing_key}).

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
                   writer_pid         = WriterPid }}.

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

handle_info({#'basic.deliver'{consumer_tag = ConsumerTag}, Msg},
            State = #session{ writer_pid = WriterPid,
                              transfer_number = TransferNum }) ->
    %% FIXME, don't ignore ack required, keep track of credit, um .. etc.
    Handle = ctag_to_handle(ConsumerTag),
    case get({out, Handle}) of
        Link = #outgoing_link{} ->
            NewLink = transfer(WriterPid, Handle, Link, State, Msg),
            put({out, Handle}, NewLink),
            {noreply, State#session{ transfer_number = next_transfer_number(TransferNum)}};
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
               local = null}, State1}
    end;

handle_control(#'v1_0.attach'{name = Name,
                              handle = Handle,
                              local = Linkage,
                              flow_state = Flow,
                              role = true}, %% client is receiver
               State = #session{backing_channel = Ch}) ->
    #'v1_0.linkage'{ source = Source } = Linkage,
    {utf8, Q} = Source#'v1_0.source'.address, %% TODO ensure_destination
    case amqp_channel:subscribe(
           Ch, #'basic.consume' { queue = Q,
                                  consumer_tag = handle_to_ctag(Handle),
                                  %% TODO noack
                                  no_ack = true,
                                  %% TODO exclusive?
                                  exclusive = false}, self()) of
        #'basic.consume_ok'{} ->
            %% FIXME we should avoid the race by getting the queue to send
            %% attach back, but a.t.m. it would use the wrong codec.
            put({out, Handle}, #outgoing_link{}),
            {reply, #'v1_0.attach'{
               name = Name,
               handle = Handle,
               remote = Linkage,
               local = #'v1_0.linkage'{
                 source = #'v1_0.source'{address = {utf8, Q}}},
               flow_state = Flow, %% TODO
               role = false
              }, State};
        _ ->
            {reply, #'v1_0.attach'{
               name = Name,
               local = null,
               remote = null},
             State}
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

handle_control(#'v1_0.detach'{ handle = Handle },
               State = #session{ writer_pid = Sock,
                                 channel_num = Channel }) ->
    erase({incoming, Handle}),
    {reply, #'v1_0.detach'{ handle = Handle }, State};

handle_control(#'v1_0.end'{}, #session{ writer_pid = Sock }) ->
    ok = rabbit_amqp1_0_writer:send_command(Sock, #'v1_0.end'{}),
    stop;

handle_control(Frame, State) ->
    {noreply, State}.

%% ------

transfer(WriterPid, LinkHandle,
         Link = #outgoing_link{ credit = Credit,
                                transfer_unit = Unit,
                                transfer_count = Count },
         Session = #session{ transfer_number = TransferNumber },
         Msg = #amqp_msg{payload = Content}) ->
    TransferSize = transfer_size(Content, Unit),
    NewLink = Link#outgoing_link{ credit = Credit - TransferSize,
                                  transfer_count = Count + TransferSize },
    T = #'v1_0.transfer'{handle = LinkHandle,
                         flow_state = flow_state(Link, Session),
                         delivery_tag = {binary,
                                         <<TransferNumber/integer>>},
                         transfer_id = {uint, TransferNumber},
                         settled = true,
                         state = #'v1_0.transfer_state'{
                           %% TODO body? message? frame? what?
                           bytes_transferred = {ulong, 0}
                          },
                         resume = false,
                         more = false,
                         aborted = false,
                         batchable = false,
                         fragments =
                             rabbit_amqp1_0_fragmentation:fragments(Msg)},
    rabbit_amqp1_0_writer:send_command(WriterPid, T),
    NewLink.

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
