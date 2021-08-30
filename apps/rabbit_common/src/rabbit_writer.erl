%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_writer).

%% This module backs writer processes ("writers"). The responsibility of
%% a writer is to serialise protocol methods and write them to the socket.
%% Every writer is associated with a channel and normally it's the channel
%% that delegates method delivery to it. However, rabbit_reader
%% (connection process) can use this module's functions to send data
%% on channel 0, which is only used for connection negotiation and
%% other "special" purposes.
%%
%% This module provides multiple functions that send protocol commands,
%% including some that are credit flow-aware.
%%
%% Writers perform internal buffering. When the amount of data
%% buffered exceeds a threshold, a socket flush is performed.
%% See FLUSH_THRESHOLD for details.
%%
%% When a socket write fails, writer will exit.

-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([start/6, start_link/6, start/7, start_link/7, start/8, start_link/8]).

-export([system_continue/3, system_terminate/4, system_code_change/4]).

-export([send_command/2, send_command/3,
         send_command_sync/2, send_command_sync/3,
         send_command_and_notify/4, send_command_and_notify/5,
         send_command_flow/2, send_command_flow/3,
         flush/1]).
-export([internal_send_command/4, internal_send_command/6]).
-export([msg_size/1, maybe_gc_large_msg/1, maybe_gc_large_msg/2]).

%% internal
-export([enter_mainloop/2, mainloop/2, mainloop1/2]).

-record(wstate, {
    %% socket (port)
    sock,
    %% channel number
    channel,
    %% connection-negotiated frame_max setting
    frame_max,
    %% see #connection.protocol in rabbit_reader
    protocol,
    %% connection (rabbit_reader) process
    reader,
    %% statistics emission timer
    stats_timer,
    %% data pending delivery (between socket
    %% flushes)
    pending,
    %% defines how ofter gc will be executed
    writer_gc_threshold
}).

-define(HIBERNATE_AFTER, 5000).
%% 1GB
-define(DEFAULT_GC_THRESHOLD, 1000000000).

%%---------------------------------------------------------------------------

-spec start
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name()) ->
            rabbit_types:ok(pid()).
-spec start_link
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name()) ->
            rabbit_types:ok(pid()).
-spec start
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name(), boolean()) ->
            rabbit_types:ok(pid()).
-spec start_link
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name(), boolean()) ->
            rabbit_types:ok(pid()).
-spec start
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name(), boolean(), undefined|non_neg_integer()) ->
            rabbit_types:ok(pid()).
-spec start_link
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name(), boolean(), undefined|non_neg_integer()) ->
            rabbit_types:ok(pid()).

-spec system_code_change(_,_,_,_) -> {'ok',_}.
-spec system_continue(_,_,#wstate{}) -> any().
-spec system_terminate(_,_,_,_) -> no_return().

-spec send_command(pid(), rabbit_framing:amqp_method_record()) -> 'ok'.
-spec send_command
        (pid(), rabbit_framing:amqp_method_record(), rabbit_types:content()) ->
            'ok'.
-spec send_command_sync(pid(), rabbit_framing:amqp_method_record()) -> 'ok'.
-spec send_command_sync
        (pid(), rabbit_framing:amqp_method_record(), rabbit_types:content()) ->
            'ok'.
-spec send_command_and_notify
        (pid(), pid(), pid(), rabbit_framing:amqp_method_record()) -> 'ok'.
-spec send_command_and_notify
        (pid(), pid(), pid(), rabbit_framing:amqp_method_record(),
         rabbit_types:content()) ->
            'ok'.
-spec send_command_flow(pid(), rabbit_framing:amqp_method_record()) -> 'ok'.
-spec send_command_flow
        (pid(), rabbit_framing:amqp_method_record(), rabbit_types:content()) ->
            'ok'.
-spec flush(pid()) -> 'ok'.
-spec internal_send_command
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         rabbit_framing:amqp_method_record(), rabbit_types:protocol()) ->
            'ok'.
-spec internal_send_command
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         rabbit_framing:amqp_method_record(), rabbit_types:content(),
         non_neg_integer(), rabbit_types:protocol()) ->
            'ok'.

-spec msg_size
        (rabbit_types:content() | rabbit_types:message()) -> non_neg_integer().

-spec maybe_gc_large_msg
        (rabbit_types:content() | rabbit_types:message()) -> non_neg_integer().
-spec maybe_gc_large_msg
        (rabbit_types:content() | rabbit_types:message(),
         undefined | non_neg_integer()) -> undefined | non_neg_integer().

%%---------------------------------------------------------------------------

start(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity) ->
    start(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity, false).

start_link(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity) ->
    start_link(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity, false).

start(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity,
      ReaderWantsStats) ->
    start(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity,
          ReaderWantsStats, ?DEFAULT_GC_THRESHOLD).

start_link(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity,
           ReaderWantsStats) ->
    start_link(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity,
               ReaderWantsStats, ?DEFAULT_GC_THRESHOLD).

start(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity,
      ReaderWantsStats, GCThreshold) ->
    State = initial_state(Sock, Channel, FrameMax, Protocol, ReaderPid,
                          ReaderWantsStats, GCThreshold),
    {ok, proc_lib:spawn(?MODULE, enter_mainloop, [Identity, State])}.

start_link(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity,
           ReaderWantsStats, GCThreshold) ->
    State = initial_state(Sock, Channel, FrameMax, Protocol, ReaderPid,
                          ReaderWantsStats, GCThreshold),
    {ok, proc_lib:spawn_link(?MODULE, enter_mainloop, [Identity, State])}.

initial_state(Sock, Channel, FrameMax, Protocol, ReaderPid, ReaderWantsStats, GCThreshold) ->
    (case ReaderWantsStats of
         true  -> fun rabbit_event:init_stats_timer/2;
         false -> fun rabbit_event:init_disabled_stats_timer/2
     end)(#wstate{sock         = Sock,
                  channel      = Channel,
                  frame_max    = FrameMax,
                  protocol     = Protocol,
                  reader       = ReaderPid,
                  pending      = [],
                  writer_gc_threshold = GCThreshold},
          #wstate.stats_timer).

system_continue(Parent, Deb, State) ->
    mainloop(Deb, State#wstate{reader = Parent}).

system_terminate(Reason, _Parent, _Deb, _State) ->
    exit(Reason).

system_code_change(Misc, _Module, _OldVsn, _Extra) ->
    {ok, Misc}.

enter_mainloop(Identity, State) ->
    ?LG_PROCESS_TYPE(writer),
    Deb = sys:debug_options([]),
    ?store_proc_name(Identity),
    mainloop(Deb, State).

mainloop(Deb, State) ->
    try
        mainloop1(Deb, State)
    catch
        exit:Error -> #wstate{reader = ReaderPid, channel = Channel} = State,
                      ReaderPid ! {channel_exit, Channel, Error}
    end,
    done.

mainloop1(Deb, State = #wstate{pending = []}) ->
    receive
        Message -> {Deb1, State1} = handle_message(Deb, Message, State),
                   ?MODULE:mainloop1(Deb1, State1)
    after ?HIBERNATE_AFTER ->
            erlang:hibernate(?MODULE, mainloop, [Deb, State])
    end;
mainloop1(Deb, State) ->
    receive
        Message -> {Deb1, State1} = handle_message(Deb, Message, State),
                   ?MODULE:mainloop1(Deb1, State1)
    after 0 ->
            ?MODULE:mainloop1(Deb, internal_flush(State))
    end.

handle_message(Deb, {system, From, Req}, State = #wstate{reader = Parent}) ->
    sys:handle_system_msg(Req, From, Parent, ?MODULE, Deb, State);
handle_message(Deb, Message, State) ->
    {Deb, handle_message(Message, State)}.

handle_message({send_command, MethodRecord}, State) ->
    internal_send_command_async(MethodRecord, State);
handle_message({send_command, MethodRecord, Content}, State) ->
    internal_send_command_async(MethodRecord, Content, State);
handle_message({send_command_flow, MethodRecord, Sender}, State) ->
    credit_flow:ack(Sender),
    internal_send_command_async(MethodRecord, State);
handle_message({send_command_flow, MethodRecord, Content, Sender}, State) ->
    credit_flow:ack(Sender),
    internal_send_command_async(MethodRecord, Content, State);
handle_message({'$gen_call', From, {send_command_sync, MethodRecord}}, State) ->
    State1 = internal_flush(
               internal_send_command_async(MethodRecord, State)),
    gen_server:reply(From, ok),
    State1;
handle_message({'$gen_call', From, {send_command_sync, MethodRecord, Content}},
               State) ->
    State1 = internal_flush(
               internal_send_command_async(MethodRecord, Content, State)),
    gen_server:reply(From, ok),
    State1;
handle_message({'$gen_call', From, flush}, State) ->
    State1 = internal_flush(State),
    gen_server:reply(From, ok),
    State1;
handle_message({send_command_and_notify, QPid, ChPid, MethodRecord}, State) ->
    State1 = internal_send_command_async(MethodRecord, State),
    rabbit_amqqueue_common:notify_sent(QPid, ChPid),
    State1;
handle_message({send_command_and_notify, QPid, ChPid, MethodRecord, Content},
               State) ->
    State1 = internal_send_command_async(MethodRecord, Content, State),
    rabbit_amqqueue_common:notify_sent(QPid, ChPid),
    State1;
handle_message({'DOWN', _MRef, process, QPid, _Reason}, State) ->
    rabbit_amqqueue_common:notify_sent_queue_down(QPid),
    State;
handle_message({inet_reply, _, ok}, State) ->
    rabbit_event:ensure_stats_timer(State, #wstate.stats_timer, emit_stats);
handle_message({inet_reply, _, Status}, _State) ->
    exit({writer, send_failed, Status});
handle_message(emit_stats, State = #wstate{reader = ReaderPid}) ->
    ReaderPid ! ensure_stats,
    rabbit_event:reset_stats_timer(State, #wstate.stats_timer);
handle_message(Message, _State) ->
    exit({writer, message_not_understood, Message}).

%%---------------------------------------------------------------------------

send_command(W, MethodRecord) ->
    W ! {send_command, MethodRecord},
    ok.

send_command(W, MethodRecord, Content) ->
    W ! {send_command, MethodRecord, Content},
    ok.

send_command_flow(W, MethodRecord) ->
    credit_flow:send(W),
    W ! {send_command_flow, MethodRecord, self()},
    ok.

send_command_flow(W, MethodRecord, Content) ->
    credit_flow:send(W),
    W ! {send_command_flow, MethodRecord, Content, self()},
    ok.

send_command_sync(W, MethodRecord) ->
    call(W, {send_command_sync, MethodRecord}).

send_command_sync(W, MethodRecord, Content) ->
    call(W, {send_command_sync, MethodRecord, Content}).

send_command_and_notify(W, Q, ChPid, MethodRecord) ->
    W ! {send_command_and_notify, Q, ChPid, MethodRecord},
    ok.

send_command_and_notify(W, Q, ChPid, MethodRecord, Content) ->
    W ! {send_command_and_notify, Q, ChPid, MethodRecord, Content},
    ok.

flush(W) -> call(W, flush).

%%---------------------------------------------------------------------------

call(Pid, Msg) ->
    {ok, Res} = gen:call(Pid, '$gen_call', Msg, infinity),
    Res.

%%---------------------------------------------------------------------------

assemble_frame(Channel, MethodRecord, Protocol) ->
    rabbit_binary_generator:build_simple_method_frame(
      Channel, MethodRecord, Protocol).

assemble_frames(Channel, MethodRecord, Content, FrameMax, Protocol) ->
    MethodName = rabbit_misc:method_record_type(MethodRecord),
    true = Protocol:method_has_content(MethodName), % assertion
    MethodFrame = rabbit_binary_generator:build_simple_method_frame(
                    Channel, MethodRecord, Protocol),
    ContentFrames = rabbit_binary_generator:build_simple_content_frames(
                      Channel, Content, FrameMax, Protocol),
    [MethodFrame | ContentFrames].

tcp_send(Sock, Data) ->
    rabbit_misc:throw_on_error(inet_error,
                               fun () -> rabbit_net:send(Sock, Data) end).

internal_send_command(Sock, Channel, MethodRecord, Protocol) ->
    ok = tcp_send(Sock, assemble_frame(Channel, MethodRecord, Protocol)).

internal_send_command(Sock, Channel, MethodRecord, Content, FrameMax,
                      Protocol) ->
    ok = lists:foldl(fun (Frame,     ok) -> tcp_send(Sock, Frame);
                         (_Frame, Other) -> Other
                     end, ok, assemble_frames(Channel, MethodRecord,
                                              Content, FrameMax, Protocol)).

internal_send_command_async(MethodRecord,
                            State = #wstate{channel   = Channel,
                                            protocol  = Protocol,
                                            pending   = Pending}) ->
    Frame = assemble_frame(Channel, MethodRecord, Protocol),
    maybe_flush(State#wstate{pending = [Frame | Pending]}).

internal_send_command_async(MethodRecord, Content,
                            State = #wstate{channel      = Channel,
                                            frame_max    = FrameMax,
                                            protocol     = Protocol,
                                            pending      = Pending,
                                            writer_gc_threshold = GCThreshold}) ->
    Frames = assemble_frames(Channel, MethodRecord, Content, FrameMax,
                             Protocol),
    maybe_gc_large_msg(Content, GCThreshold),
    maybe_flush(State#wstate{pending = [Frames | Pending]}).

%% When the amount of protocol method data buffered exceeds
%% this threshold, a socket flush is performed.
%%
%% This magic number is the tcp-over-ethernet MSS (1460) minus the
%% minimum size of a AMQP 0-9-1 basic.deliver method frame (24) plus basic
%% content header (22). The idea is that we want to flush just before
%% exceeding the MSS.
-define(FLUSH_THRESHOLD, 1414).

maybe_flush(State = #wstate{pending = Pending}) ->
    case iolist_size(Pending) >= ?FLUSH_THRESHOLD of
        true  -> internal_flush(State);
        false -> State
    end.

internal_flush(State = #wstate{pending = []}) ->
    State;
internal_flush(State = #wstate{sock = Sock, pending = Pending}) ->
    ok = port_cmd(Sock, lists:reverse(Pending)),
    State#wstate{pending = []}.

%% gen_tcp:send/2 does a selective receive of {inet_reply, Sock,
%% Status} to obtain the result. That is bad when it is called from
%% the writer since it requires scanning of the writers possibly quite
%% large message queue.
%%
%% So instead we lift the code from prim_inet:send/2, which is what
%% gen_tcp:send/2 calls, do the first half here and then just process
%% the result code in handle_message/2 as and when it arrives.
%%
%% This means we may end up happily sending data down a closed/broken
%% socket, but that's ok since a) data in the buffers will be lost in
%% any case (so qualitatively we are no worse off than if we used
%% gen_tcp:send/2), and b) we do detect the changed socket status
%% eventually, i.e. when we get round to handling the result code.
%%
%% Also note that the port has bounded buffers and port_command blocks
%% when these are full. So the fact that we process the result
%% asynchronously does not impact flow control.
port_cmd(Sock, Data) ->
    true = try rabbit_net:port_command(Sock, Data)
           catch error:Error -> exit({writer, send_failed, Error})
           end,
    ok.

%% Some processes (channel, writer) can get huge amounts of binary
%% garbage when processing huge messages at high speed (since we only
%% do enough reductions to GC every few hundred messages, and if each
%% message is 1MB then that's ugly). So count how many bytes of
%% message we have processed, and force a GC every so often.
maybe_gc_large_msg(Content) ->
    maybe_gc_large_msg(Content, ?DEFAULT_GC_THRESHOLD).

maybe_gc_large_msg(_Content, undefined) ->
    undefined;
maybe_gc_large_msg(Content, GCThreshold) ->
    Size = msg_size(Content),
    Current = case get(msg_size_for_gc) of
                  undefined -> 0;
                  C         -> C
              end,
    New = Current + Size,
    put(msg_size_for_gc, case New > GCThreshold of
                             true  -> erlang:garbage_collect(),
                                      0;
                             false -> New
                         end),
    Size.

msg_size(#content{payload_fragments_rev = PFR}) -> iolist_size(PFR);
msg_size(#basic_message{content = Content})     -> msg_size(Content).
