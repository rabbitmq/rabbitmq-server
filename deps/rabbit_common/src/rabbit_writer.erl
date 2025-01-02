%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_writer).

-behavior(gen_server).

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
-export([start/6, start_link/6, start/7, start_link/7, start/8, start_link/8]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([send_command/2, send_command/3,
         send_command_sync/2, send_command_sync/3,
         send_command_and_notify/4, send_command_and_notify/5,
         send_command_flow/2, send_command_flow/3,
         flush/1]).
-export([internal_send_command/4, internal_send_command/6]).
-export([msg_size/1, maybe_gc_large_msg/1, maybe_gc_large_msg/2]).

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
    %% defines how often gc will be executed
    writer_gc_threshold
}).

-define(HIBERNATE_AFTER, 6_000).
%% 1GB
-define(DEFAULT_GC_THRESHOLD, 1_000_000_000).

%%---------------------------------------------------------------------------

-spec start
        (rabbit_net:socket(), rabbit_types:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name()) ->
            rabbit_types:ok(pid()).
-spec start_link
        (rabbit_net:socket(), rabbit_types:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name()) ->
            rabbit_types:ok(pid()).
-spec start
        (rabbit_net:socket(), rabbit_types:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name(), boolean()) ->
            rabbit_types:ok(pid()).
-spec start_link
        (rabbit_net:socket(), rabbit_types:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name(), boolean()) ->
            rabbit_types:ok(pid()).
-spec start
        (rabbit_net:socket(), rabbit_types:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name(), boolean(), undefined|non_neg_integer()) ->
            rabbit_types:ok(pid()).
-spec start_link
        (rabbit_net:socket(), rabbit_types:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(),
         rabbit_types:proc_name(), boolean(), undefined|non_neg_integer()) ->
            rabbit_types:ok(pid()).

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
        (rabbit_net:socket(), rabbit_types:channel_number(),
         rabbit_framing:amqp_method_record(), rabbit_types:protocol()) ->
            'ok'.
-spec internal_send_command
        (rabbit_net:socket(), rabbit_types:channel_number(),
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
    Options = [{hibernate_after, ?HIBERNATE_AFTER}],
    gen_server:start(?MODULE, [Identity, State], Options).

start_link(Sock, Channel, FrameMax, Protocol, ReaderPid, Identity,
           ReaderWantsStats, GCThreshold) ->
    State = initial_state(Sock, Channel, FrameMax, Protocol, ReaderPid,
                          ReaderWantsStats, GCThreshold),
    Options = [{hibernate_after, ?HIBERNATE_AFTER}],
    gen_server:start_link(?MODULE, [Identity, State], Options).

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

init([Identity, State]) ->
    ?LG_PROCESS_TYPE(writer),
    ?store_proc_name(Identity),
    {ok, State}.

handle_call({send_command_sync, MethodRecord}, _From, State) ->
    try
        State1 = internal_flush(
                   internal_send_command_async(MethodRecord, State)),
        {reply, ok, State1, 0}
    catch
        _Class:Reason ->
            {stop, {shutdown, Reason}, State}
    end;
handle_call({send_command_sync, MethodRecord, Content}, _From, State) ->
    try
        State1 = internal_flush(
                   internal_send_command_async(MethodRecord, Content, State)),
        {reply, ok, State1, 0}
    catch
        _Class:Reason ->
            {stop, {shutdown, Reason}, State}
    end;
handle_call(flush, _From, State) ->
    try
        State1 = internal_flush(State),
        {reply, ok, State1, 0}
    catch
        _Class:Reason ->
            {stop, {shutdown, Reason}, State}
    end.

handle_cast(_Message, State) ->
    {noreply, State, 0}.

handle_info(timeout, State) ->
    try
        State1 = internal_flush(State),
        {noreply, State1}
    catch
        _Class:Reason ->
            {stop, {shutdown, Reason}, State}
    end;
handle_info(Message, State) ->
    try
        State1 = handle_message(Message, State),
        {noreply, State1, 0}
    catch
        _Class:Reason ->
            {stop, {shutdown, Reason}, State}
    end.

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
handle_message(emit_stats, State = #wstate{reader = ReaderPid}) ->
    ReaderPid ! ensure_stats,
    rabbit_event:reset_stats_timer(State, #wstate.stats_timer);
handle_message(ok, State) ->
    State;
handle_message({_Ref, ok} = Msg, State) ->
    rabbit_log:warning("AMQP 0-9-1 channel writer has received a message it does not support: ~p", [Msg]),
    State;
handle_message({ok, _Ref} = Msg, State) ->
    rabbit_log:warning("AMQP 0-9-1 channel writer has received a message it does not support: ~p", [Msg]),
    State;
handle_message(Message, _State) ->
    exit({writer, message_not_understood, Message}).

terminate(Reason, State) ->
    #wstate{reader = ReaderPid, channel = Channel} = State,
    ReaderPid ! {channel_exit, Channel, Reason},
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

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
    gen_server:call(Pid, Msg, infinity).

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
    _ = maybe_gc_large_msg(Content, GCThreshold),
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
internal_flush(State0 = #wstate{sock = Sock, pending = Pending}) ->
    case rabbit_net:send(Sock, lists:reverse(Pending)) of
        ok ->
            ok;
        {error, Reason} ->
            exit({writer, send_failed, Reason})
    end,
    State = State0#wstate{pending = []},
    rabbit_event:ensure_stats_timer(State, #wstate.stats_timer, emit_stats).

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
