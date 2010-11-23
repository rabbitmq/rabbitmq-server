-module(rabbit_amqp1_0_session).

-behaviour(gen_server2).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-export([start_link/3, process_frame/2]).

-record(session, {channel, reader_pid, writer_pid, transfer_number = 0,
                  outgoing_lwm = 0, outgoing_session_credit = 10 }).
-record(outgoing_link, {credit = 0,
                        transfer_count = 0,
                        transfer_unit = 0}).

-record(incoming_link, {name, target}).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
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

start_link(IncomingChannel, ReaderPid, WriterPid) ->
    {ok, Pid} = gen_server2:start_link(
                  ?MODULE, [IncomingChannel, ReaderPid, WriterPid], []),
    Pid.

process_frame(Pid, Frame) ->
    gen_server2:cast(Pid, {frame, Frame}).

%% ---------

init([Channel, ReaderPid, WriterPid]) ->
    process_flag(trap_exit, true),
    link(WriterPid),
    {ok, #session{ channel = Channel,
                   reader_pid = ReaderPid,
                   writer_pid = WriterPid }}.

terminate(Reason, State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(Msg, From, State) ->
    {reply, {error, not_understood, Msg}, State}.

%% TODO these pretty much copied wholesale from rabbit_channel
handle_info({'EXIT', WriterPid, Reason = {writer, send_failed, _Error}},
            State = #session{writer_pid = WriterPid}) ->
    State#session.reader_pid ! {channel_exit, State#session.channel, Reason},
    {stop, normal, State};
handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};
handle_info({'DOWN', _MRef, process, QPid, _Reason}, State) ->
    {noreply, State}. % FIXME rabbit_channel uses queue_blocked?

handle_cast({deliver, ConsumerTag, AckRequired, MsgStruct},
            State = #session{ writer_pid = WriterPid,
                              transfer_number = TransferNum }) ->
    %% FIXME, don't ignore ack required, keep track of credit, um .. etc.
    %% Consumer tag is the link handle
    case get({out, ConsumerTag}) of
        Link = #outgoing_link{} ->
            NewLink = transfer(WriterPid, ConsumerTag, Link, State, MsgStruct),
            put({out, ConsumerTag}, NewLink),
            {noreply, State#session{ transfer_number = next_transfer_number(TransferNum)}};
        _ ->
            %% FIXME handle missing link -- why does the queue think it's there?
            io:format("Delivery to non-existent consumer ~p", [ConsumerTag]),
            {noreply, State}
    end;

handle_cast({frame, Frame},
            State = #session{ writer_pid = Sock,
                              channel = Channel}) ->
    case handle_control(Frame, State) of
        {reply, Reply, NewState} ->
            ok = rabbit_writer:send_control_v1_0(Sock, Reply),
            noreply(NewState);
        {noreply, NewState} ->
            noreply(NewState);
        stop ->
            {stop, normal, State}
    %% TODO rabbit_channel has some extra error handling here
    end.

noreply(State) ->
    {noreply, State, hibernate}.

%% ------

handle_control(#'v1_0.begin'{}, State = #session{ channel = Channel }) ->
    {reply, #'v1_0.begin'{
       remote_channel = {ushort, Channel}}, State};

handle_control(#'v1_0.attach'{name = Name,
                              handle = Handle,
                              local = Linkage,
                              flow_state = Flow,
                              role = false}, %% client is sender
               State = #session{ outgoing_lwm = LWM }) ->
    %% TODO associate link name with target
    #'v1_0.linkage'{ target = Exchange } = Linkage,
    #'v1_0.flow_state'{ transfer_count = TransferCount } = Flow,
    %% FIXME check for the exchange ..
    Link = #incoming_link{ name = Name, target = Exchange },
    put({incoming, Handle}, Link),
    {reply, 
     #'v1_0.attach'{
       name = Name,
       handle = Handle,
       remote = Linkage,
       local = #'v1_0.linkage'{
         target = Exchange 
        }, %% TODO include whatever the source was
       flow_state = Flow#'v1_0.flow_state'{
                      link_credit = {uint, 50},
                      unsettled_lwm = {uint, LWM}
                     },
       role = true %% reciever
      }, State};

handle_control(#'v1_0.attach'{name = Name,
                              handle = Handle,
                              local = Linkage,
                              flow_state = Flow,
                              role = true}, %% client is receiver
               State) ->
    #'v1_0.linkage'{ source = {utf8, Q} } = Linkage,
    case rabbit_amqqueue:with(
           rabbit_misc:r(<<"/">>, queue, Q),
           fun (Queue) ->
                   rabbit_amqqueue:basic_consume(Queue,
                                                 true, %% FIXME noack
                                                 self(),
                                                 undefined, %% FIXME limiter
                                                 Handle,
                                                 false, %% exclusive
                                                 undefined),
                   %% FIXME we should avoid the race by getting the queue to send
                   %% attach back, but a.t.m. it would use the wrong codec.
                   put({out, Handle}, #outgoing_link{}),
                   ok
           end) of
        ok ->
            {reply, #'v1_0.attach'{
               name = Name,
               handle = Handle,
               remote = Linkage,
               local = #'v1_0.linkage'{ source = {utf8, Q} },
               flow_state = Flow, %% TODO
               role = false
              }, State};
        {error, _} ->
            {reply, #'v1_0.attach'{
               name = Name,
               local = null,
               remote = null},
             State}
    end;

handle_control(#'v1_0.transfer'{handle = Handle,
                                delivery_tag = Tag,
                                transfer_id = TransferId,
                                fragments = {list, Fragments}
                               },
                          State) ->
    case get({incoming, Handle}) of
        #incoming_link{ target = {utf8, Target} } ->
            %% Send to the exchange!
            ExchangeName = rabbit_misc:r(<<"/">>, exchange, Target),
            %% Check permitted
            Exchange = rabbit_exchange:lookup_or_die(ExchangeName),
            %% Scangiest way to the content
            Msg = assemble_v1_0_message(ExchangeName, Fragments),
            {RoutingRes, DeliveredQPids} =
                rabbit_exchange:publish(
                  Exchange,
                  rabbit_basic:delivery(false, false, none, Msg));
        undefined ->
            %% FIXME What am I supposed to do here
            no_such_handle
    end,
    {noreply, State};

handle_control(#'v1_0.detach'{ handle = Handle },
               State = #session{ writer_pid = Sock,
                                 channel = Channel }) ->
    erase({incoming, Handle}),
    {reply, #'v1_0.detach'{ handle = Handle }, State};

handle_control(#'v1_0.end'{}, #session{ writer_pid = Sock }) ->
    ok = rabbit_writer:send_control_v1_0(Sock, #'v1_0.end'{}),
    stop;

handle_control(Frame, State) ->
    io:format("Session frame: ~p~n", [Frame]),
    {noreply, State}.

%% ------

%% Kludged because so is the python client
assemble_v1_0_message(ExchangeName, Fragments) ->
    %% get the class_id we need
    {ClassId, _MethodId} = rabbit_framing:method_id('basic.publish'),
    [Fragment | _] = Fragments,
    {described, {symbol, "amqp:fragment:list"},
     {list, [_, _, _, _, {binary, Payload}]}} = Fragment,
    Content = #content{class_id = ClassId,
                       payload_fragments_rev = [Payload],
                       properties = #'P_basic'{},
                       properties_bin = none},
    #basic_message{exchange_name = ExchangeName,
                   routing_key = <<"">>,
                   content = Content,
                   guid = rabbit_guid:guid(),
                   is_persistent = false}.

transfer(WriterPid, LinkHandle,
         Link = #outgoing_link{ credit = Credit,
                                transfer_unit = Unit,
                                transfer_count = Count },
         Session = #session{ transfer_number = TransferNumber },
         {_QName, QPid, _MsgId, Redelivered,
          #basic_message{content = Content}}) ->
    TransferSize = transfer_size(Content, Unit),
    NewLink = Link#outgoing_link{ credit = Credit - TransferSize,
                                  transfer_count = Count + TransferSize },
    T = #'v1_0.transfer'{handle = LinkHandle,
                         flow_state = flow_state(Link, Session),
                         delivery_tag = {binary,
                                         <<TransferNumber/integer>>},
                         transfer_id = {uint, TransferNumber},
                         settled = true,
                         state = {symbol, "ACCEPTED"}, % FIXME
                         resume = false,
                         more = false,
                         aborted = false,
                         batchable = false,
                         fragments = fragments(Content)},
    rabbit_writer:send_control_and_notify_v1_0(
      WriterPid, QPid, self(), T),
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

next_transfer_number(TransferNumber) ->
    %% TODO this should be a serial number
    TransferNumber + 1.

%% FIXME
fragments(Content) ->
    {list, []}.

%% FIXME
transfer_size(Content, Unit) ->
    1.

send_on_channel_v1_0(Sock, Channel, Control) ->
    ok = rabbit_writer:internal_send_control_v1_0(Sock, Channel, Control).
