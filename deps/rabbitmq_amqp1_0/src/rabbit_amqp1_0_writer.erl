%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp1_0_writer).
-behaviour(gen_server).

-include("rabbit_amqp1_0.hrl").

%% client API
-export([start_link/3,
         send_command/3,
         send_command/4,
         send_command_sync/3,
         send_command_and_notify/6,
         internal_send_command/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         format_status/1]).

-record(state, {
          sock :: rabbit_net:socket(),
          max_frame_size :: unlimited | pos_integer(),
          reader :: pid(),
          pending :: iolist(),
          %% This field is just an optimisation to minimize the cost of erlang:iolist_size/1
          pending_size :: non_neg_integer()
         }).

-define(HIBERNATE_AFTER, 6_000).
-define(CALL_TIMEOUT, 300_000).
-define(AMQP_SASL_FRAME_TYPE, 1).

%%%%%%%%%%%%%%%%%%
%%% client API %%%
%%%%%%%%%%%%%%%%%%

-spec start_link (rabbit_net:socket(), non_neg_integer(), pid()) ->
    rabbit_types:ok(pid()).
start_link(Sock, MaxFrame, ReaderPid) ->
    Args = {Sock, MaxFrame, ReaderPid},
    Opts = [{hibernate_after, ?HIBERNATE_AFTER}],
    gen_server:start_link(?MODULE, Args, Opts).

-spec send_command(pid(),
                   rabbit_types:channel_number(),
                   rabbit_framing:amqp_method_record()) -> ok.
send_command(Writer, ChannelNum, MethodRecord) ->
    Request = {send_command, ChannelNum, MethodRecord},
    gen_server:cast(Writer, Request).

-spec send_command(pid(),
                   rabbit_types:channel_number(),
                   rabbit_framing:amqp_method_record(),
                   rabbit_types:content()) -> ok.
send_command(Writer, ChannelNum, MethodRecord, Content) ->
    Request = {send_command, ChannelNum, MethodRecord, Content},
    gen_server:cast(Writer, Request).

-spec send_command_sync(pid(),
                        rabbit_types:channel_number(),
                        rabbit_framing:amqp_method_record()) -> ok.
send_command_sync(Writer, ChannelNum, MethodRecord) ->
    Request = {send_command, ChannelNum, MethodRecord},
    gen_server:call(Writer, Request, ?CALL_TIMEOUT).

-spec send_command_and_notify(pid(),
                              rabbit_types:channel_number(),
                              pid(),
                              pid(),
                              rabbit_framing:amqp_method_record(),
                              rabbit_types:content()) -> ok.
send_command_and_notify(Writer, ChannelNum, QueuePid, SessionPid, MethodRecord, Content) ->
    Request = {send_command_and_notify, ChannelNum, QueuePid, SessionPid, MethodRecord, Content},
    gen_server:cast(Writer, Request).

-spec internal_send_command(rabbit_net:socket(),
                            rabbit_framing:amqp_method_record(),
                            amqp10_framing | rabbit_amqp1_0_sasl) -> ok.
internal_send_command(Sock, MethodRecord, Protocol) ->
    Data = assemble_frame(0, MethodRecord, Protocol),
    ok = tcp_send(Sock, Data).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server callbacks %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init({Sock, MaxFrame, ReaderPid}) ->
    State = #state{sock = Sock,
                   max_frame_size = MaxFrame,
                   reader = ReaderPid,
                   pending = [],
                   pending_size = 0},
    {ok, State}.

handle_cast({send_command, ChannelNum, MethodRecord}, State0) ->
    State = internal_send_command_async(ChannelNum, MethodRecord, State0),
    no_reply(State);
handle_cast({send_command, ChannelNum, MethodRecord, Content}, State0) ->
    State = internal_send_command_async(ChannelNum, MethodRecord, Content, State0),
    no_reply(State);
handle_cast({send_command_and_notify, ChannelNum, QueuePid, SessionPid, MethodRecord, Content}, State0) ->
    State = internal_send_command_async(ChannelNum, MethodRecord, Content, State0),
    rabbit_amqqueue:notify_sent(QueuePid, SessionPid),
    no_reply(State).

handle_call({send_command, ChannelNum, MethodRecord}, _From, State0) ->
    State1 = internal_send_command_async(ChannelNum, MethodRecord, State0),
    State = flush(State1),
    {reply, ok, State}.

handle_info(timeout, State0) ->
    State = flush(State0),
    {noreply, State};
handle_info({'DOWN', _MRef, process, QueuePid, _Reason}, State) ->
    rabbit_amqqueue:notify_sent_queue_down(QueuePid),
    no_reply(State).

format_status(Status) ->
    maps:update_with(
      state,
      fun(#state{sock = Sock,
                 max_frame_size = MaxFrame,
                 reader = Reader,
                 pending = Pending,
                 pending_size = PendingSize}) ->
              #{socket => Sock,
                max_frame_size => MaxFrame,
                reader => Reader,
                %% Below 2 fields should always have the same value.
                pending => iolist_size(Pending),
                pending_size => PendingSize}
      end,
      Status).

%%%%%%%%%%%%%%%
%%% Helpers %%%
%%%%%%%%%%%%%%%

no_reply(State) ->
    {noreply, State, 0}.

internal_send_command_async(Channel, MethodRecord,
                            State = #state{pending = Pending,
                                           pending_size = PendingSize}) ->
    Frame = assemble_frame(Channel, MethodRecord),
    maybe_flush(State#state{pending = [Frame | Pending],
                            pending_size = PendingSize + iolist_size(Frame)}).

internal_send_command_async(Channel, MethodRecord, Content,
                            State = #state{max_frame_size = MaxFrame,
                                           pending = Pending,
                                           pending_size = PendingSize}) ->
    Frames = assemble_frames(Channel, MethodRecord, Content, MaxFrame),
    maybe_flush(State#state{pending = [Frames | Pending],
                            pending_size = PendingSize + iolist_size(Frames)}).

%% Note: a transfer record can be followed by a number of other
%% records to make a complete frame but unlike 0-9-1 we may have many
%% content records. However, that's already been handled for us, we're
%% just sending a chunk, so from this perspective it's just a binary.

%%TODO respect MaxFrame
assemble_frames(Channel, Performative, Content, _MaxFrame) ->
    ?DEBUG("Channel ~tp <-~n~tp~n followed by ~tp bytes of content~n",
           [Channel, amqp10_framing:pprint(Performative),
            iolist_size(Content)]),
    PerfBin = amqp10_framing:encode_bin(Performative),
    amqp10_binary_generator:build_frame(Channel, [PerfBin, Content]).

assemble_frame(Channel, Performative) ->
    assemble_frame(Channel, Performative, amqp10_framing).

assemble_frame(Channel, Performative, amqp10_framing) ->
    ?DEBUG("Channel ~tp <-~n~tp~n",
           [Channel, amqp10_framing:pprint(Performative)]),
    PerfBin = amqp10_framing:encode_bin(Performative),
    amqp10_binary_generator:build_frame(Channel, PerfBin);
assemble_frame(Channel, Performative, rabbit_amqp1_0_sasl) ->
    ?DEBUG("Channel ~tp <-~n~tp~n",
           [Channel, amqp10_framing:pprint(Performative)]),
    PerfBin = amqp10_framing:encode_bin(Performative),
    amqp10_binary_generator:build_frame(Channel, ?AMQP_SASL_FRAME_TYPE, PerfBin).

tcp_send(Sock, Data) ->
    rabbit_misc:throw_on_error(
      inet_error,
      fun() -> rabbit_net:send(Sock, Data) end).

%% This magic number is the tcp-over-ethernet MSS (1460) minus the
%% minimum size of a AMQP basic.deliver method frame (24) plus basic
%% content header (22). The idea is that we want to flush just before
%% exceeding the MSS.
%% TODO doesn't make sense for AMQP 1.0
-define(FLUSH_THRESHOLD, 1414).

maybe_flush(State = #state{pending_size = PendingSize}) ->
    case PendingSize >= ?FLUSH_THRESHOLD of
        true  -> flush(State);
        false -> State
    end.

flush(State = #state{pending = []}) ->
    State;
flush(State = #state{sock = Sock,
                     pending = Pending}) ->
    case rabbit_net:send(Sock, lists:reverse(Pending)) of
        ok ->
            State#state{pending = [],
                        pending_size = 0};
        {error, Reason} ->
            exit({writer, send_failed, Reason})
    end.
