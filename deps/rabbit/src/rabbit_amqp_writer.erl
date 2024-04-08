%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp_writer).
-behaviour(gen_server).

-include("rabbit_amqp.hrl").

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
          reader :: rabbit_types:connection(),
          pending :: iolist(),
          %% This field is just an optimisation to minimize the cost of erlang:iolist_size/1
          pending_size :: non_neg_integer()
         }).

-define(HIBERNATE_AFTER, 6_000).
-define(CALL_TIMEOUT, 300_000).
-define(AMQP_SASL_FRAME_TYPE, 1).

-type performative() :: tuple().
-type payload() :: iodata().

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
                   performative()) -> ok.
send_command(Writer, ChannelNum, Performative) ->
    Request = {send_command, ChannelNum, Performative},
    gen_server:cast(Writer, Request).

-spec send_command(pid(),
                   rabbit_types:channel_number(),
                   performative(),
                   payload()) -> ok.
send_command(Writer, ChannelNum, Performative, Payload) ->
    Request = {send_command, ChannelNum, Performative, Payload},
    gen_server:cast(Writer, Request).

-spec send_command_sync(pid(),
                        rabbit_types:channel_number(),
                        performative()) -> ok.
send_command_sync(Writer, ChannelNum, Performative) ->
    Request = {send_command, ChannelNum, Performative},
    gen_server:call(Writer, Request, ?CALL_TIMEOUT).

%% Delete this function when feature flag credit_api_v2 becomes required.
-spec send_command_and_notify(pid(),
                              rabbit_types:channel_number(),
                              pid(),
                              pid(),
                              performative(),
                              payload()) -> ok.
send_command_and_notify(Writer, ChannelNum, QueuePid, SessionPid, Performative, Payload) ->
    Request = {send_command_and_notify, ChannelNum, QueuePid, SessionPid, Performative, Payload},
    gen_server:cast(Writer, Request).

-spec internal_send_command(rabbit_net:socket(),
                            performative(),
                            amqp10_framing | rabbit_amqp_sasl) -> ok.
internal_send_command(Sock, Performative, Protocol) ->
    Data = assemble_frame(0, Performative, Protocol),
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
    process_flag(message_queue_data, off_heap),
    {ok, State}.

handle_cast({send_command, ChannelNum, Performative}, State0) ->
    State = internal_send_command_async(ChannelNum, Performative, State0),
    no_reply(State);
handle_cast({send_command, ChannelNum, Performative, Payload}, State0) ->
    State = internal_send_command_async(ChannelNum, Performative, Payload, State0),
    no_reply(State);
%% Delete below function clause when feature flag credit_api_v2 becomes required.
handle_cast({send_command_and_notify, ChannelNum, QueuePid, SessionPid, Performative, Payload}, State0) ->
    State = internal_send_command_async(ChannelNum, Performative, Payload, State0),
    rabbit_amqqueue:notify_sent(QueuePid, SessionPid),
    no_reply(State).

handle_call({send_command, ChannelNum, Performative}, _From, State0) ->
    State1 = internal_send_command_async(ChannelNum, Performative, State0),
    State = flush(State1),
    {reply, ok, State}.

handle_info(timeout, State0) ->
    State = flush(State0),
    {noreply, State};
%% Delete below function clause when feature flag credit_api_v2 becomes required.
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

internal_send_command_async(Channel, Performative,
                            State = #state{pending = Pending,
                                           pending_size = PendingSize}) ->
    Frame = assemble_frame(Channel, Performative),
    maybe_flush(State#state{pending = [Frame | Pending],
                            pending_size = PendingSize + iolist_size(Frame)}).

internal_send_command_async(Channel, Performative, Payload,
                            State = #state{max_frame_size = MaxFrame,
                                           pending = Pending,
                                           pending_size = PendingSize}) ->
    Frames = assemble_frame(Channel, Performative, Payload, MaxFrame),
    maybe_flush(State#state{pending = [Frames | Pending],
                            pending_size = PendingSize + iolist_size(Frames)}).

assemble_frame(Channel, Performative) ->
    assemble_frame(Channel, Performative, amqp10_framing).

assemble_frame(Channel, Performative, amqp10_framing) ->
    ?DEBUG("~s Channel ~tp <-~n~tp~n",
           [?MODULE, Channel, amqp10_framing:pprint(Performative)]),
    PerfBin = amqp10_framing:encode_bin(Performative),
    amqp10_binary_generator:build_frame(Channel, PerfBin);
assemble_frame(Channel, Performative, rabbit_amqp_sasl) ->
    ?DEBUG("~s Channel ~tp <-~n~tp~n",
           [?MODULE, Channel, amqp10_framing:pprint(Performative)]),
    PerfBin = amqp10_framing:encode_bin(Performative),
    amqp10_binary_generator:build_frame(Channel, ?AMQP_SASL_FRAME_TYPE, PerfBin).

%%TODO respect MaxFrame
assemble_frame(Channel, Performative, Payload, _MaxFrame) ->
    ?DEBUG("~s Channel ~tp <-~n~tp~n followed by ~tp bytes of payload~n",
           [?MODULE, Channel, amqp10_framing:pprint(Performative),
            iolist_size(Payload)]),
    PerfIoData = amqp10_framing:encode_bin(Performative),
    amqp10_binary_generator:build_frame(Channel, [PerfIoData, Payload]).

tcp_send(Sock, Data) ->
    rabbit_misc:throw_on_error(
      inet_error,
      fun() -> rabbit_net:send(Sock, Data) end).

%% Flush when more than 2.5 * 1460 bytes (TCP over Ethernet MSS) = 3650 bytes of data
%% has accumulated. The idea is to get the TCP data sections full (i.e. fill 1460 bytes)
%% as often as possible to reduce the overhead of TCP/IP headers.
-define(FLUSH_THRESHOLD, 3650).

maybe_flush(State = #state{pending_size = PendingSize}) ->
    case PendingSize > ?FLUSH_THRESHOLD of
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
